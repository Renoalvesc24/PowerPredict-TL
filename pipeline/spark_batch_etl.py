"""
Spark batch ETL - batch layer (Lambda architecture) for ANOMALY DETECTION

raw CSV (data lake, raw zone: 5GB / 10GB / 20GB)
  1. ingest                    explicit schema, explicit file list
  2. data-quality profiling    missing, duplicates, invalid values
  3. cleaning                  drop missing / duplicate / invalid rows
  4. feature engineering       broadcast join with the meter master + load-shape table,
                               load ratio, voltage / frequency deviation, baseline ratio,
                               P/(V.I.PF) consistency
  5. anomaly analytics         counts by municipality / hour / month / customer type,
                               feature profile of every anomaly type, top anomalous meters
  6. meter-level split         1 meter in 5 is held out for testing (no leakage of long
                               theft / fault episodes between train and test)
  7. training sample           train meters: every anomaly row (up to a cap per type) +
                               a random sample of Normal rows (majority undersampling)
     test sample               test meters: natural distribution (what the model meets live)
  8. curated Parquet           full cleaned + featured data, partitioned by month
  -> MySQL: tbl_batch_anomaly_stats, tbl_batch_feature_profile, tbl_batch_top_meters,
            tbl_data_quality, tbl_pipeline_runs

    python -m pipeline.spark_batch_etl --dataset 20GB
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from common.spark import (add_features_spark, get_spark, read_raw_readings, shape_table,  # noqa: E402
                          spark_path, test_meter_flag, write_parquet)

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CRITICAL_COLS = ["timestamp", "meter_id", "municipality", "customer_type", "contracted_kva", "phase",
                 "voltage_v", "current_a", "power_kw", "power_factor", "frequency_hz", "temperature_c",
                 "anomaly_type"]
PROFILE_FEATURES = ["voltage_dev_pct", "freq_dev_hz", "power_factor", "load_ratio", "baseline_ratio",
                    "consistency_ratio", "temperature_c", "current_a", "power_kw"]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.glob("part-*.csv"))


def profile_quality(raw, n_raw: int) -> list[dict]:
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(f"missing__{c}") for c in CRITICAL_COLS]
    for col, (lo, hi) in D.VALID_RANGES.items():
        exprs.append(F.sum((F.col(col).isNotNull() & ~F.col(col).between(lo, hi)).cast("int")).alias(f"invalid__{col}"))
    exprs.append(F.sum((~F.col("anomaly_type").isin(D.ANOMALY_TYPES)).cast("int")).alias("invalid__anomaly_type"))
    counts = raw.agg(*exprs).collect()[0].asDict()
    n_unique = raw.dropDuplicates(["meter_id", "timestamp"]).count()
    checks = [{"check_name": k.split("__")[0], "column_name": k.split("__")[1], "n_rows": int(v or 0)}
              for k, v in counts.items()]
    checks.append({"check_name": "duplicate", "column_name": "meter_id+timestamp", "n_rows": int(n_raw - n_unique)})
    for c in checks:
        c["pct"] = round(c["n_rows"] / n_raw * 100, 4) if n_raw else 0.0
    return checks


def clean(raw):
    df = raw.dropna(subset=CRITICAL_COLS).dropDuplicates(["meter_id", "timestamp"])
    for col, (lo, hi) in D.VALID_RANGES.items():
        df = df.filter(F.col(col).between(lo, hi))
    return df.filter(F.col("anomaly_type").isin(D.ANOMALY_TYPES))


def anomaly_stats(cur, label: str) -> list[dict]:
    dims = {"municipality": F.col("municipality"), "hour": F.col("hour").cast("string"),
            "month": F.month("timestamp").cast("string"), "customer_type": F.col("customer_type")}
    out = []
    for name, expr in dims.items():
        rows = cur.groupBy(expr.alias("dim_value"), "anomaly_type").count().collect()
        out += [{"dataset": label, "dimension": name, "dim_value": r["dim_value"],
                 "anomaly_type": r["anomaly_type"], "n_readings": int(r["count"])} for r in rows]
    return out


def feature_profile(cur, label: str) -> list[dict]:
    aggs = []
    for f in PROFILE_FEATURES:
        aggs += [F.avg(f).alias(f"{f}__mean"), F.stddev(f).alias(f"{f}__std"),
                 F.percentile_approx(f, [0.05, 0.5, 0.95], 1000).alias(f"{f}__pct")]
    out = []
    for r in cur.groupBy("anomaly_type").agg(*aggs).collect():
        for f in PROFILE_FEATURES:
            p = r[f"{f}__pct"] or [None, None, None]
            out.append({"dataset": label, "anomaly_type": r["anomaly_type"], "feature": f,
                        "mean": r[f"{f}__mean"], "std": r[f"{f}__std"], "p05": p[0], "p50": p[1], "p95": p[2]})
    return out


def top_meters(cur, label: str, n: int = 60) -> list[dict]:
    lost = F.when(F.col("anomaly_type") == "Energy Theft",
                  F.greatest(F.col("baseline_kw") - F.col("power_kw"), F.lit(0.0))).otherwise(F.lit(0.0))
    rows = (cur.filter(F.col("anomaly_type") != D.NORMAL)
            .groupBy("meter_id", "municipality", "customer_type", "anomaly_type")
            .agg(F.count("*").alias("n_hours"), F.min("timestamp").alias("first_ts"),
                 F.max("timestamp").alias("last_ts"), F.round(F.sum(lost), 2).alias("lost_kwh_est"))
            .filter(F.col("anomaly_type").isin("Energy Theft", "Meter Fault", "Overload"))
            .orderBy(F.desc("n_hours")).limit(n).collect())
    return [{"dataset": label, **r.asDict()} for r in rows]


# ---------------------------------------------------------------------------
def run(label: str, normal_rows: int, cap_per_type: int, test_rows: int, write_curated: bool) -> dict:
    raw_dir = config.RAW_DIR / f"readings_{label}"
    if not raw_dir.exists():
        raise SystemExit(f"{raw_dir} not found - generate it first")
    run_id = db.start_run("spark_batch_etl", "apache-spark", label)
    t0 = time.time()
    timings: dict[str, float] = {}
    spark = get_spark(f"PowerPredict-BatchETL-{label}")
    log(f"Spark {spark.version} | {spark.sparkContext.defaultParallelism} cores | UI {spark.sparkContext.uiWebUrl}")
    try:
        t = time.time()
        raw = read_raw_readings(spark, label).persist(StorageLevel.MEMORY_AND_DISK)
        n_raw = raw.count()
        timings["1_read_csv"] = time.time() - t
        n_parts = raw.rdd.getNumPartitions()
        # scale shuffle partitions with the input so cached blocks stay small (20GB: ~2 x 208 partitions)
        shuffle_parts = max(int(config.SPARK_SHUFFLE_PARTITIONS), 2 * n_parts)
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
        log(f"raw rows: {n_raw:,} ({dir_size(raw_dir)/1024**2:,.0f} MB, {n_parts} partitions, "
            f"{shuffle_parts} shuffle partitions)")

        t = time.time()
        checks = profile_quality(raw, n_raw)
        timings["2_quality_profile"] = time.time() - t
        log("quality issues: " + json.dumps({f"{c['check_name']}:{c['column_name']}": c["n_rows"]
                                             for c in checks if c["n_rows"]}))

        t = time.time()
        meters = (spark.read.option("header", True).option("inferSchema", True)
                  .csv([spark_path(config.REFERENCE_DIR / f"meters_{label}.csv")]))
        cur = (add_features_spark(clean(raw), meters, shape_table(spark))
               .withColumn("month", F.month("timestamp"))
               .withColumn("is_test_meter", test_meter_flag().cast("int"))
               .persist(StorageLevel.MEMORY_AND_DISK))
        n_clean = cur.count()
        raw.unpersist()
        timings["3_clean_features_join"] = time.time() - t
        log(f"clean rows: {n_clean:,} (removed {n_raw - n_clean:,} = {(n_raw - n_clean) / n_raw * 100:.2f}%)")

        t = time.time()
        dist = {r["anomaly_type"]: int(r["count"]) for r in cur.groupBy("anomaly_type").count().collect()}
        db.replace_dataset_rows(db.batch_anomaly_stats, label, anomaly_stats(cur, label))
        db.replace_dataset_rows(db.batch_feature_profile, label, feature_profile(cur, label))
        db.replace_dataset_rows(db.batch_top_meters, label, top_meters(cur, label))
        timings["4_anomaly_analytics"] = time.time() - t
        log(f"class distribution: {dist}")

        # training sample: train meters, all anomalies (capped) + undersampled Normal
        t = time.time()
        train = cur.filter(F.col("is_test_meter") == 0)
        train_counts = {r["anomaly_type"]: int(r["count"]) for r in train.groupBy("anomaly_type").count().collect()}
        fractions = {a: min(1.0, (normal_rows if a == D.NORMAL else cap_per_type) / max(n, 1))
                     for a, n in train_counts.items()}
        cols = ["reading_id", "timestamp", "meter_id", "phase"] + D.FEATURES + ["baseline_kw", "anomaly_type"]
        n_train = write_parquet(train.sampleBy("anomaly_type", fractions, seed=42).select(*cols).coalesce(8),
                                config.CURATED_DIR / f"train_sample_{label}")
        test = cur.filter(F.col("is_test_meter") == 1)
        n_test_all = test.count()
        n_test = write_parquet(test.sample(fraction=min(1.0, test_rows / max(n_test_all, 1)), seed=7)
                               .select(*cols).coalesce(8), config.CURATED_DIR / f"test_sample_{label}")
        timings["5_train_test_samples"] = time.time() - t
        log(f"train sample {n_train:,} rows (undersampled Normal) | test sample {n_test:,} rows "
            f"(natural distribution, from {n_test_all:,} test-meter rows)")

        n_written = None
        if write_curated:
            t = time.time()
            out_cols = ["reading_id", "timestamp", "meter_id", "municipality", "customer_type", "contracted_kva",
                        "phase", "voltage_v", "current_a", "power_kw", "energy_kwh", "power_factor", "frequency_hz",
                        "temperature_c", "hour", "is_weekend", "load_ratio", "voltage_dev_pct", "freq_dev_hz",
                        "baseline_kw", "baseline_ratio", "consistency_ratio", "is_test_meter", "anomaly_type", "month"]
            n_written = write_parquet(cur.select(*out_cols), config.CURATED_DIR / f"readings_{label}", "month")
            timings["6_write_curated_parquet"] = time.time() - t
            log(f"curated parquet rows: {n_written:,}")
        cur.unpersist()

        with db.begin() as conn:
            conn.execute(db.data_quality.delete().where(db.data_quality.c.dataset == label))
        db.bulk_insert(db.data_quality, [{**c, "run_id": run_id, "dataset": label} for c in checks])
        details = {"timings_s": {k: round(v, 2) for k, v in timings.items()},
                   "removed_rows": n_raw - n_clean, "class_distribution": dist,
                   "train_class_counts": train_counts, "train_sample_rows": n_train, "test_sample_rows": n_test,
                   "test_meter_rows": n_test_all, "curated_rows": n_written,
                   "cores": spark.sparkContext.defaultParallelism, "spark_version": spark.version}
        db.finish_run(run_id, "success", input_rows=n_raw, output_rows=n_clean, input_bytes=dir_size(raw_dir),
                      details=details, message=f"{n_raw:,} raw -> {n_clean:,} clean rows")
        log(f"done in {time.time() - t0:.1f}s  timings={details['timings_s']}")
        return details
    except Exception as exc:
        db.finish_run(run_id, "failed", message=str(exc)[:2000])
        raise
    finally:
        spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", default="20GB")
    ap.add_argument("--normal-rows", type=int, default=600_000, help="Normal rows in the training sample")
    ap.add_argument("--cap-per-type", type=int, default=80_000, help="max rows per anomaly type in the training sample")
    ap.add_argument("--test-rows", type=int, default=1_000_000, help="rows in the natural-distribution test sample")
    ap.add_argument("--no-curated", action="store_true", help="skip writing the full curated Parquet")
    args = ap.parse_args()
    run(args.dataset, args.normal_rows, args.cap_per_type, args.test_rows, not args.no_curated)


if __name__ == "__main__":
    main()
