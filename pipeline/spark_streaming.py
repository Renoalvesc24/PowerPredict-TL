"""
spark_streaming.py - speed layer:  Kafka -> Spark -> anomaly detection -> MySQL + data lake

Every trigger (default 3 s) Spark reads the new Kafka offsets of every partition,
then for the micro-batch:
  1. parse JSON with an explicit schema
  2. validate (missing / impossible values, unknown municipality) + de-duplicate
  3. stream-static join with the meter master (broadcast) + feature engineering
     (same Spark code as the batch ETL -> the model sees identical features)
  4. 1-minute tumbling windows per municipality, with a 2-minute WATERMARK:
     readings older than (latest event time - 2 min) are still scored but are not
     added to the window counts (late data), like Structured Streaming does
  5. Random Forest scores every reading (anomaly type + anomaly score)
  6. MySQL: tbl_readings_live, tbl_stream_agg (upsert), tbl_anomaly_events
     (cool-down 5 min per meter + type), tbl_stream_batches; Parquet stream zone
Offsets are committed to MySQL only after a batch is stored (at-least-once, no loss).

Engines
  spark       (default) Spark micro-batches over explicit Kafka offset ranges
              (works on Windows without Hadoop's winutils)
  structured  Spark Structured Streaming readStream + withWatermark + foreachBatch
              (needs HADOOP_HOME/winutils on Windows; works out of the box on Linux)
  python      kafka-python consumer + pandas features (fallback, no Spark)

    python -m pipeline.spark_streaming
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import delete, insert, select, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from ml.predict import load_active, predict_frame  # noqa: E402

RUNNING = True
WATERMARK = timedelta(minutes=2)
MESSAGE_FIELDS = ["reading_id", "meter_id", "timestamp", "municipality", "customer_type", "contracted_kva", "phase",
                  "voltage_v", "current_a", "power_kw", "energy_kwh", "power_factor", "frequency_hz",
                  "temperature_c", "true_type", "producer_ts"]
LIVE_COLS = ["reading_id", "meter_id", "ts", "municipality", "customer_type", "contracted_kva", "voltage_v",
             "current_a", "power_kw", "energy_kwh", "power_factor", "frequency_hz", "temperature_c",
             "voltage_dev_pct", "freq_dev_hz", "load_ratio", "baseline_ratio", "consistency_ratio",
             "predicted_type", "anomaly_score", "confidence", "is_anomaly", "true_type", "latency_ms", "batch_id"]


def _stop(*_):
    global RUNNING
    RUNNING = False


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_meter_master() -> pd.DataFrame:
    with db.begin() as conn:
        return pd.DataFrame(db.rows(conn.execute(select(db.meters.c.meter_id, db.meters.c.peak_util,
                                                        db.meters.c.pf_base))))


def validate_pandas(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Same rules as the batch cleaning step (python engine)."""
    n = len(df)
    need = ["meter_id", "timestamp", "municipality", "customer_type", "contracted_kva", "phase", "voltage_v",
            "current_a", "power_kw", "power_factor", "frequency_hz", "temperature_c"]
    ok = df[need].notna().all(axis=1)
    for col, (lo, hi) in D.VALID_RANGES.items():
        if col in df:
            ok &= df[col].between(lo, hi)
    ok &= df.municipality.isin(D.MUNICIPALITY_NAMES) & df.customer_type.isin(D.CUSTOMER_TYPES)
    good = df[ok].drop_duplicates("reading_id")
    return good, n - len(good)


# ---------------------------------------------------------------------------
class Sink:
    """Scores a validated, feature-engineered micro-batch and stores every result."""

    def __init__(self, engine: str):
        self.engine = engine
        self.batch_id = self._last_batch_id()
        self.cooldown: dict[tuple[str, str], datetime] = {}
        self.lake_buffer: list[pd.DataFrame] = []
        self.last_flush = time.time()
        self.last_cleanup = 0.0
        self.max_event_ts: pd.Timestamp | None = None
        self.is_mysql = config.DB_URL.startswith("mysql")

    def _last_batch_id(self) -> int:
        with db.begin() as conn:
            v = conn.execute(select(db.stream_batches.c.batch_id).order_by(db.stream_batches.c.id.desc()).limit(1)).scalar()
        return int(v or 0)

    def handle(self, valid: pd.DataFrame, n_messages: int, n_invalid: int, started: float,
               offsets: dict | None = None) -> None:
        self.batch_id += 1
        latency, n_anom, n_late = [], 0, 0
        if len(valid):
            scored = predict_frame(valid)
            scored["ts"] = pd.to_datetime(scored["timestamp"])
            scored["latency_ms"] = (time.time() * 1000 - scored["producer_ts"].astype(float)).round(1)
            scored["batch_id"] = self.batch_id
            latency = scored["latency_ms"].tolist()
            n_anom = int(scored["is_anomaly"].sum())
            # watermark: late readings are scored and stored, but not counted in the windows
            batch_max = scored["ts"].max()
            self.max_event_ts = batch_max if self.max_event_ts is None else max(self.max_event_ts, batch_max)
            on_time = scored[scored["ts"] >= self.max_event_ts - WATERMARK]
            n_late = len(scored) - len(on_time)
            self._write_live(scored)
            if len(on_time):
                self._write_agg(on_time)
            self._write_events(scored)
            self.lake_buffer.append(scored)
        self._flush_lake()
        proc_ms = (time.time() - started) * 1000
        with db.begin() as conn:
            conn.execute(insert(db.stream_batches).values(
                batch_id=self.batch_id, engine=self.engine, started_at=datetime.fromtimestamp(started),
                n_messages=n_messages, n_valid=len(valid), n_invalid=n_invalid, n_anomalies=n_anom,
                processing_ms=round(proc_ms, 1),
                avg_latency_ms=float(np.mean(latency)) if latency else None,
                max_latency_ms=float(np.max(latency)) if latency else None,
                throughput_mps=round(n_messages / max(proc_ms / 1000, 1e-3), 1),
                offsets=json.dumps(offsets) if offsets else None))
        if n_messages:
            log(f"batch {self.batch_id}: {n_messages} msgs, {len(valid)} valid, {n_invalid} invalid, "
                f"{n_anom} anomalies, {n_late} late, {proc_ms:.0f} ms, latency {np.mean(latency) if latency else 0:.0f} ms")
        if time.time() - self.last_cleanup > 300:
            self._cleanup()

    # -- writers ----------------------------------------------------------------
    def _write_live(self, s: pd.DataFrame) -> None:
        recs = s[LIVE_COLS].astype(object).where(s[LIVE_COLS].notna(), None).to_dict("records")
        for r in recs:
            r["ts"] = r["ts"].to_pydatetime()
            r["is_anomaly"] = bool(r["is_anomaly"])
        db.bulk_insert(db.readings_live, recs)

    def _write_agg(self, s: pd.DataFrame) -> None:
        s = s.assign(window_start=s["ts"].dt.floor("min"))
        g = s.groupby(["window_start", "municipality"])
        agg = g.agg(n_readings=("reading_id", "count"), n_anomalies=("is_anomaly", "sum"),
                    total_kwh=("energy_kwh", "sum"), avg_voltage=("voltage_v", "mean"),
                    min_voltage=("voltage_v", "min"), avg_frequency=("frequency_hz", "mean"),
                    avg_score=("anomaly_score", "mean")).reset_index()
        types = (g["predicted_type"].value_counts().unstack(fill_value=0)
                 .reindex(columns=D.ANOMALIES, fill_value=0).rename(columns=db.AGG_TYPE_COLS).reset_index())
        agg = agg.merge(types, on=["window_start", "municipality"], how="left").fillna(0)
        agg["window_end"] = agg["window_start"] + pd.Timedelta(minutes=1)
        cnt = ["n_readings", "n_anomalies"] + list(db.AGG_TYPE_COLS.values())
        avg = ["avg_voltage", "avg_frequency", "avg_score"]
        cols = ["window_start", "window_end", "municipality", "total_kwh", "min_voltage"] + cnt + avg
        sql = text(
            f"INSERT INTO tbl_stream_agg ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)}) "
            "ON DUPLICATE KEY UPDATE "
            + ", ".join(f"{c} = ({c} * n_readings + VALUES({c}) * VALUES(n_readings)) / (n_readings + VALUES(n_readings))"
                        for c in avg)
            + ", min_voltage = LEAST(min_voltage, VALUES(min_voltage)), total_kwh = total_kwh + VALUES(total_kwh), "
            + ", ".join(f"{c} = {c} + VALUES({c})" for c in cnt if c != "n_readings")
            + ", n_readings = n_readings + VALUES(n_readings)")
        recs = []
        for r in agg.to_dict("records"):
            r["window_start"] = pd.Timestamp(r["window_start"]).to_pydatetime()
            r["window_end"] = pd.Timestamp(r["window_end"]).to_pydatetime()
            recs.append({k: (int(r[k]) if k in cnt else float(r[k]) if k in avg + ["total_kwh", "min_voltage"] else r[k])
                         for k in cols})
        with db.begin() as conn:
            if self.is_mysql:
                conn.execute(sql, recs)
            else:
                for r in recs:
                    conn.execute(delete(db.stream_agg).where(db.stream_agg.c.window_start == r["window_start"],
                                                             db.stream_agg.c.municipality == r["municipality"]))
                    conn.execute(insert(db.stream_agg).values(**r))

    def _write_events(self, s: pd.DataFrame) -> None:
        min_score = float(db.get_setting("alert_min_score", "0.6") or 0.6)
        hits = s[s.is_anomaly & (s.anomaly_score >= min_score)]
        now = datetime.now()
        recs = []
        for r in hits.itertuples():
            key = (r.meter_id, r.predicted_type)
            last = self.cooldown.get(key)
            if last and now - last < timedelta(minutes=5):
                continue
            self.cooldown[key] = now
            recs.append({"ts": r.ts.to_pydatetime(), "meter_id": r.meter_id, "municipality": r.municipality,
                         "customer_type": r.customer_type, "anomaly_type": r.predicted_type,
                         "severity": D.ANOMALY_SEVERITY.get(r.predicted_type, "Medium"),
                         "anomaly_score": float(r.anomaly_score), "confidence": float(r.confidence),
                         "true_type": r.true_type if isinstance(r.true_type, str) else None,
                         "reason": "; ".join(D.explain_anomaly(r)) or "Random Forest pattern match",
                         **{k: float(getattr(r, k)) for k in ("voltage_v", "current_a", "power_kw", "power_factor",
                                                              "frequency_hz", "load_ratio", "baseline_ratio",
                                                              "consistency_ratio")}})
        if recs:
            db.bulk_insert(db.anomaly_events, recs)

    def _flush_lake(self, force: bool = False) -> None:
        if not self.lake_buffer or (not force and time.time() - self.last_flush < 60):
            return
        df = pd.concat(self.lake_buffer, ignore_index=True)
        self.lake_buffer, self.last_flush = [], time.time()
        cols = MESSAGE_FIELDS + ["load_ratio", "baseline_ratio", "consistency_ratio", "predicted_type",
                                 "anomaly_score", "latency_ms", "batch_id"]
        df = df[[c for c in cols if c in df.columns]]
        for day, part in df.groupby(df["timestamp"].astype(str).str[:10]):
            folder = config.STREAM_LAKE_DIR / f"date={day}"
            folder.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pandas(part, preserve_index=False),
                           folder / f"batch-{self.batch_id:07d}-{int(time.time())}.parquet", compression="snappy")

    def _cleanup(self) -> None:
        cutoff = datetime.now() - timedelta(hours=config.LIVE_RETENTION_HOURS)
        with db.begin() as conn:
            conn.execute(delete(db.readings_live).where(db.readings_live.c.ts < cutoff))
            conn.execute(delete(db.stream_agg).where(db.stream_agg.c.window_start < datetime.now() - timedelta(days=7)))
            conn.execute(delete(db.stream_batches).where(db.stream_batches.c.started_at < datetime.now() - timedelta(days=2)))
        self.last_cleanup = time.time()

    def close(self):
        self._flush_lake(force=True)


# ---------------------------------------------------------------------------
# Offsets (stored in MySQL) + one long-lived Kafka client
# ---------------------------------------------------------------------------
def load_offsets(topic: str) -> dict[int, int]:
    with db.begin() as conn:
        rs = conn.execute(select(db.stream_offsets).where(db.stream_offsets.c.topic == topic))
        return {int(r.partition_no): int(r.next_offset) for r in rs}


def save_offsets(topic: str, offsets: dict[int, int]) -> None:
    with db.begin() as conn:
        for p, off in offsets.items():
            n = conn.execute(db.stream_offsets.update().where(db.stream_offsets.c.topic == topic,
                                                              db.stream_offsets.c.partition_no == p)
                             .values(next_offset=off, updated_at=datetime.now())).rowcount
            if not n:
                conn.execute(insert(db.stream_offsets).values(topic=topic, partition_no=p, next_offset=off))


class OffsetReader:
    """Re-using one Kafka client avoids the Windows socket leak (WinError 10038) that
    creating a client every trigger causes; it is only re-created after an error."""

    def __init__(self, topic: str):
        self.topic = topic
        self.client = None

    def end_offsets(self, retries: int = 5) -> dict[int, int]:
        from kafka import KafkaConsumer, TopicPartition
        last = None
        for attempt in range(1, retries + 1):
            try:
                if self.client is None:
                    self.client = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP, enable_auto_commit=False)
                parts = self.client.partitions_for_topic(self.topic) or set()
                tps = [TopicPartition(self.topic, p) for p in parts]
                return {tp.partition: off for tp, off in self.client.end_offsets(tps).items()}
            except Exception as exc:
                last = exc
                log(f"reading Kafka offsets failed ({type(exc).__name__}: {exc}); reconnecting ({attempt}/{retries})")
                self.close()
                time.sleep(min(2 * attempt, 10))
        raise RuntimeError(f"Kafka not reachable: {last}")

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:  # noqa: BLE001
                pass
            self.client = None


# ---------------------------------------------------------------------------
# Spark processing (shared by the micro-batch and Structured Streaming engines)
# ---------------------------------------------------------------------------
def spark_message_schema():
    from pyspark.sql import types as T
    s, d = T.StringType(), T.DoubleType()
    return T.StructType([T.StructField(n, t) for n, t in [
        ("reading_id", s), ("meter_id", s), ("timestamp", s), ("municipality", s), ("customer_type", s),
        ("contracted_kva", d), ("phase", s), ("voltage_v", d), ("current_a", d), ("power_kw", d),
        ("energy_kwh", d), ("power_factor", d), ("frequency_hz", d), ("temperature_c", d),
        ("true_type", s), ("producer_ts", T.LongType())]])


class MeterCache:
    """Meter master as a broadcastable Spark DataFrame, refreshed every 10 minutes."""

    def __init__(self, spark):
        self.spark, self.df, self.loaded = spark, None, 0.0

    def get(self):
        if self.df is None or time.time() - self.loaded > 600:
            pdf = load_meter_master()
            self.df = self.spark.createDataFrame(pdf).cache()
            self.loaded = time.time()
        return self.df


def spark_process(kafka_df, sink: Sink, meters, shapes, started: float, offsets_info: dict | None = None):
    from pyspark.sql import functions as F

    from common.spark import add_features_spark

    parsed = (kafka_df.select(F.from_json(F.col("value").cast("string"), spark_message_schema()).alias("m"))
              .select("m.*").cache())
    n_messages = parsed.count()
    if n_messages == 0:
        parsed.unpersist()
        sink.handle(pd.DataFrame(), 0, 0, started, offsets_info)
        return
    cond = F.lit(True)
    for c in ["meter_id", "timestamp", "municipality", "customer_type", "contracted_kva", "phase", "voltage_v",
              "current_a", "power_kw", "power_factor", "frequency_hz", "temperature_c"]:
        cond = cond & F.col(c).isNotNull()
    for col, (lo, hi) in D.VALID_RANGES.items():
        cond = cond & F.col(col).between(lo, hi)
    cond = cond & F.col("municipality").isin(D.MUNICIPALITY_NAMES) & F.col("customer_type").isin(D.CUSTOMER_TYPES)
    valid = (parsed.filter(cond).dropDuplicates(["reading_id"])
             .withColumn("ts", F.to_timestamp("timestamp")))
    featured = add_features_spark(valid, meters, shapes).drop("ts")
    vpd = featured.toPandas()
    parsed.unpersist()
    sink.handle(vpd, n_messages, n_messages - len(vpd), started, offsets_info)


def run_spark_microbatch(args) -> None:
    from common.spark import get_spark, shape_table
    spark = get_spark("PowerPredict-Streaming", kafka=True, memory="4g", cores="local[8]")
    log(f"Spark {spark.version} micro-batch engine, trigger {args.trigger}s, UI {spark.sparkContext.uiWebUrl}")
    sink, meters, shapes = Sink("spark-microbatch"), MeterCache(spark), shape_table(spark).cache()
    topic = config.KAFKA_TOPIC
    reader = OffsetReader(topic)
    offsets = load_offsets(topic)
    end = reader.end_offsets()
    if not offsets or args.from_latest:
        offsets = {p: (0 if args.from_beginning else o) for p, o in end.items()}
        save_offsets(topic, offsets)
    failures = 0
    while RUNNING:
        t0 = time.time()
        try:
            end = reader.end_offsets()
            for p in end:
                offsets.setdefault(p, 0)
            target = {p: min(end[p], offsets[p] + args.max_per_partition) for p in end}   # back-pressure
            if any(target[p] > offsets[p] for p in target):
                df = (spark.read.format("kafka")
                      .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
                      .option("subscribe", topic)
                      .option("startingOffsets", json.dumps({topic: {str(p): o for p, o in offsets.items()}}))
                      .option("endingOffsets", json.dumps({topic: {str(p): o for p, o in target.items()}}))
                      .load())
                spark_process(df, sink, meters.get(), shapes, t0, {"start": offsets, "end": target})
                offsets = dict(target)                 # commit only after the batch is stored
                save_offsets(topic, offsets)
            db.heartbeat("stream_processor", {"engine": "spark-microbatch", "batch": sink.batch_id,
                                              "offsets": offsets, "lag": sum(end[p] - offsets[p] for p in end),
                                              "watermark_s": WATERMARK.total_seconds()})
            failures = 0
        except Exception as exc:  # keep the stream alive; the same offsets are retried
            failures += 1
            log(f"batch failed ({type(exc).__name__}: {str(exc)[:300]}); retry {failures}")
            if failures >= 20:
                raise
            time.sleep(min(3 * failures, 30))
            continue
        time.sleep(max(0.0, args.trigger - (time.time() - t0)))
    reader.close()
    sink.close()
    spark.stop()


def run_spark_structured(args) -> None:
    from pyspark.sql import functions as F

    from common.spark import get_spark, native_hadoop_available, shape_table
    if not native_hadoop_available():
        raise SystemExit("Structured Streaming needs HADOOP_HOME with winutils.exe on Windows. "
                         "Use --engine spark (micro-batch) instead.")
    spark = get_spark("PowerPredict-StructuredStreaming", kafka=True, memory="4g", cores="local[8]")
    sink, meters, shapes = Sink("spark-structured"), MeterCache(spark), shape_table(spark).cache()
    stream = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
              .option("subscribe", config.KAFKA_TOPIC).option("startingOffsets", "latest").load()
              .withColumn("event_ts", F.to_timestamp(F.get_json_object(F.col("value").cast("string"), "$.timestamp")))
              .withWatermark("event_ts", "2 minutes"))

    def foreach_batch(df, epoch_id):
        spark_process(df, sink, meters.get(), shapes, time.time())
        db.heartbeat("stream_processor", {"engine": "spark-structured", "epoch": int(epoch_id)})

    q = (stream.writeStream.foreachBatch(foreach_batch)
         .option("checkpointLocation", str(config.DATA_DIR / "checkpoints" / "stream"))
         .trigger(processingTime=f"{int(args.trigger)} seconds").start())
    while RUNNING and q.isActive:
        time.sleep(1)
    q.stop()
    sink.close()


def run_python(args) -> None:
    from kafka import KafkaConsumer
    sink = Sink("python-consumer")
    meters = load_meter_master()
    consumer = KafkaConsumer(config.KAFKA_TOPIC, bootstrap_servers=config.KAFKA_BOOTSTRAP,
                             group_id="powerpredict-python", auto_offset_reset="latest", enable_auto_commit=False,
                             value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    log("kafka-python consumer engine")
    while RUNNING:
        t0 = time.time()
        polled = consumer.poll(timeout_ms=int(args.trigger * 1000), max_records=20000)
        msgs = [m.value for batch in polled.values() for m in batch]
        if msgs:
            df = pd.DataFrame(msgs).reindex(columns=MESSAGE_FIELDS)
            valid, n_bad = validate_pandas(df)
            sink.handle(D.add_features_pandas(valid, meters) if len(valid) else valid, len(df), n_bad, t0)
            consumer.commit()
        db.heartbeat("stream_processor", {"engine": "python-consumer", "batch": sink.batch_id})
    sink.close()
    consumer.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", choices=["spark", "structured", "python"], default="spark")
    ap.add_argument("--trigger", type=float, default=config.STREAM_TRIGGER_SECONDS)
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--from-latest", action="store_true", help="skip any backlog on start")
    ap.add_argument("--max-per-partition", type=int, default=20000, help="back-pressure per micro-batch")
    args = ap.parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    row, _ = load_active("supervised", force=True)
    if row is None:
        raise SystemExit("No active Random Forest - run `python -m ml.train` first.")
    log(f"active model #{row['id']} {row['algorithm']} (anomaly F1 {row['anomaly_f1']:.4f})")
    {"spark": run_spark_microbatch, "structured": run_spark_structured, "python": run_python}[args.engine](args)


if __name__ == "__main__":
    main()
