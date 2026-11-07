"""
Spark helpers.

* get_spark()        - SparkSession tuned for one machine (local[*]).
* write_parquet()    - distributed Parquet writer. Uses Spark's native writer
                       when Hadoop's native Windows libraries are installed
                       (HADOOP_HOME / winutils.exe) or on Linux/macOS; otherwise every
                       Spark partition writes its own Parquet file with Apache
                       Arrow (mapInArrow), so the job is still distributed.
* raw_schema()       - explicit schema of the raw meter readings.
* add_features_spark() - the 16 model features (same formulas as
                       common.domain.add_features_pandas), used by the batch
                       ETL and by Spark Streaming.

All timestamps are treated as naive Timor-Leste local time (Asia/Dili) and
the Spark session runs in UTC so no hidden time-zone shifts happen.
"""
from __future__ import annotations

import os
import shutil
import sys
import uuid
from pathlib import Path

import config

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402


def native_hadoop_available() -> bool:
    if os.name != "nt":
        return True
    home = os.environ.get("HADOOP_HOME")
    return bool(home) and (Path(home) / "bin" / "winutils.exe").exists()


def get_spark(app: str, kafka: bool = False, cores: str | None = None,
              memory: str | None = None, ui: bool = True) -> SparkSession:
    builder = (SparkSession.builder.appName(app)
               .master(cores or config.SPARK_MASTER)
               .config("spark.driver.memory", memory or config.SPARK_DRIVER_MEMORY)
               .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.execution.arrow.pyspark.enabled", "true")
               .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
               .config("spark.ui.enabled", "true" if ui else "false")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.driver.extraJavaOptions", "-Dlog4j2.level=WARN"))
    if config.SPARK_LOCAL_DIR:
        builder = builder.config("spark.local.dir", config.SPARK_LOCAL_DIR)
    if kafka:
        jars = sorted(str(p) for p in (config.ROOT / "infra" / "jars").glob("*.jar"))
        if jars:
            # put the Kafka connector straight on the classpath (no jar copy -> no winutils chmod)
            builder = builder.config("spark.driver.extraClassPath", os.pathsep.join(jars))
        else:
            builder = builder.config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def spark_path(p: Path | str) -> str:
    """Local path in the form Spark/Hadoop expects on Windows (forward slashes)."""
    return str(Path(p)).replace("\\", "/")


def raw_schema() -> T.StructType:
    return T.StructType([
        T.StructField("reading_id", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("meter_id", T.StringType()),
        T.StructField("municipality", T.StringType()),
        T.StructField("customer_type", T.StringType()),
        T.StructField("contracted_kva", T.DoubleType()),
        T.StructField("phase", T.StringType()),
        T.StructField("voltage_v", T.DoubleType()),
        T.StructField("current_a", T.DoubleType()),
        T.StructField("power_kw", T.DoubleType()),
        T.StructField("energy_kwh", T.DoubleType()),
        T.StructField("power_factor", T.DoubleType()),
        T.StructField("frequency_hz", T.DoubleType()),
        T.StructField("temperature_c", T.DoubleType()),
        T.StructField("anomaly_type", T.StringType()),
    ])


def list_files(folder: Path | str, pattern: str) -> list[str]:
    """Explicit file list (recursive). On Windows without Hadoop's native
    library, Spark's own glob / directory listing hangs, so we always give
    Spark the exact files to read."""
    files = sorted(spark_path(p) for p in Path(folder).rglob(pattern) if p.is_file())
    if not files:
        raise FileNotFoundError(f"no {pattern} files in {folder}")
    return files


def read_raw_readings(spark: SparkSession, label: str) -> DataFrame:
    files = list_files(config.RAW_DIR / f"readings_{label}", "part-*.csv")
    return (spark.read.option("header", True).option("mode", "PERMISSIVE")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(raw_schema()).csv(files))


def read_parquet(spark: SparkSession, folder: Path | str) -> DataFrame:
    """Read a Parquet folder written by write_parquet()."""
    return spark.read.parquet(*list_files(folder, "*.parquet"))


def write_parquet(df: DataFrame, out_dir: Path | str, partition_col: str | None = None) -> int:
    """Write `df` as Parquet; returns the number of rows written."""
    out_dir = Path(out_dir)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    if native_hadoop_available() and os.environ.get("PP_NATIVE_PARQUET") == "1":
        writer = df.write.mode("overwrite")
        if partition_col:
            # keep the column inside the files too, so explicit-file reads still see it
            df = df.withColumn(f"_{partition_col}", F.col(partition_col))
            writer = df.write.mode("overwrite").partitionBy(f"_{partition_col}")
        writer.parquet(spark_path(out_dir))
        return df.count()

    out_dir.mkdir(parents=True, exist_ok=True)
    target = str(out_dir)
    pcol = partition_col

    def _write_partition(batches):
        import os as _os
        import uuid as _uuid

        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        writers: dict = {}
        total = 0
        tag = _uuid.uuid4().hex[:12]
        for batch in batches:
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            # the partition column stays inside the file (no directory listing needed to read it)
            groups = [(None, table)] if pcol is None else [
                (v, table.filter(pc.equal(table[pcol], v)))
                for v in pc.unique(table[pcol]).to_pylist()]
            for value, part in groups:
                if value is None and pcol is None:
                    folder = target
                else:
                    folder = _os.path.join(target, f"{pcol}={value}")
                if folder not in writers:
                    _os.makedirs(folder, exist_ok=True)
                    writers[folder] = pq.ParquetWriter(_os.path.join(folder, f"part-{tag}.parquet"),
                                                       part.schema, compression="snappy")
                writers[folder].write_table(part)
                total += part.num_rows
        for w in writers.values():
            w.close()
        yield pa.RecordBatch.from_pydict({"rows": [total]})

    written = df.mapInArrow(_write_partition, "rows long").agg(F.sum("rows").alias("n")).collect()[0]["n"]
    return int(written or 0)


def new_run_tag() -> str:
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Feature engineering (Spark) - identical formulas to domain.add_features_pandas
# ---------------------------------------------------------------------------
def shape_table(spark: SparkSession) -> DataFrame:
    """Small reference table (customer_type, hour) -> load-shape value, broadcast-joined."""
    from common import domain as D
    rows = [(t, h, float(D.LOAD_SHAPES[t][h])) for t in D.CUSTOMER_TYPES for h in range(24)]
    return spark.createDataFrame(rows, ["customer_type", "hour", "shape"])


def add_features_spark(df: DataFrame, meters: DataFrame, shapes: DataFrame) -> DataFrame:
    """df needs: timestamp/ts, meter_id, customer_type, contracted_kva, phase, voltage_v,
    current_a, power_kw, power_factor, frequency_hz, temperature_c.
    meters: meter_id, peak_util, pf_base (meter master, broadcast join)."""
    from itertools import chain

    from common import domain as D

    def cmap(d):
        return F.create_map(*[F.lit(x) for x in chain(*d.items())])

    ts = F.col("ts") if "ts" in df.columns else F.col("timestamp")
    pf = F.greatest(F.col("power_factor"), F.lit(0.3))
    out = (df.withColumn("hour", F.hour(ts))
             .withColumn("is_weekend", F.when(F.dayofweek(ts).isin(1, 7), 1).otherwise(0))
             .join(F.broadcast(meters.select("meter_id", "peak_util", "pf_base")), "meter_id", "left")
             .join(F.broadcast(shapes), ["customer_type", "hour"], "left"))
    peak = F.coalesce(F.col("peak_util"), cmap(D.PEAK_UTILISATION)[F.col("customer_type")])
    pfb = F.coalesce(F.col("pf_base"), cmap({k: v[0] for k, v in D.POWER_FACTOR_BASE.items()})[F.col("customer_type")])
    wk = F.when(F.col("is_weekend") == 1, cmap(D.WEEKEND_FACTOR)[F.col("customer_type")]).otherwise(F.lit(1.0))
    beta = cmap(D.TEMPERATURE_SENSITIVITY)[F.col("customer_type")]
    temp_f = F.least(F.greatest(1 + beta * (F.col("temperature_c") - 27.0), F.lit(0.85)), F.lit(1.45))
    base = F.col("contracted_kva") * peak * F.col("shape") * wk * temp_f * pfb
    div = F.when(F.col("phase") == "three", F.lit(3.0)).otherwise(F.lit(1.0))
    vip = F.col("voltage_v") * F.col("current_a") * pf * div / 1000.0
    cons = (F.when(vip > 1e-6, F.col("power_kw") / vip)
             .when(F.col("power_kw") > 0.01, F.lit(D.RATIO_CAP)).otherwise(F.lit(1.0)))
    return (out
            .withColumn("load_ratio", F.round(F.col("power_kw") / pf / F.col("contracted_kva"), 4))
            .withColumn("voltage_dev_pct", F.round((F.col("voltage_v") - D.NOMINAL_VOLTAGE) / D.NOMINAL_VOLTAGE * 100, 3))
            .withColumn("freq_dev_hz", F.round(F.col("frequency_hz") - D.NOMINAL_FREQUENCY, 3))
            .withColumn("baseline_kw", F.round(base, 4))
            .withColumn("baseline_ratio",
                        F.round(F.least(F.greatest(F.col("power_kw") / F.greatest(base, F.lit(0.01)), F.lit(0.0)),
                                        F.lit(D.RATIO_CAP)), 4))
            .withColumn("consistency_ratio", F.round(F.least(F.greatest(cons, F.lit(0.0)), F.lit(D.RATIO_CAP)), 4))
            .drop("shape", "peak_util", "pf_base"))


def test_meter_flag(col: str = "meter_id"):
    """Meter-level train/test split: 1 in 5 meters is held out for testing (crc32 so
    pandas can reproduce it with zlib.crc32)."""
    return (F.crc32(F.col(col)) % 5) == 0
