"""
Serving-layer database (MySQL / MariaDB on XAMPP; SQLite also works) for the
PowerPredict TL anomaly-detection system.

All tables use the `tbl_` prefix. The schema is defined once with
SQLAlchemy Core so the same code runs on MySQL and SQLite.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Float, Index, Integer,
                        MetaData, String, Table, Text, UniqueConstraint, create_engine, delete,
                        func, insert, select, text, update)
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine

import config

LongText = Text().with_variant(mysql.LONGTEXT(), "mysql")
metadata = MetaData()


def _id():
    return Column("id", Integer, primary_key=True, autoincrement=True)


def _created():
    return Column("created_at", DateTime, nullable=False, server_default=func.now())


settings = Table(
    "tbl_settings", metadata,
    Column("key", String(80), primary_key=True),
    Column("value", Text),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

# --- Reference data ----------------------------------------------------------
municipalities = Table(
    "tbl_municipalities", metadata, _id(),
    Column("name", String(40), nullable=False, unique=True),
    Column("capital", String(60), nullable=False),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("population_weight", Integer, nullable=False),
    Column("grid", String(20), nullable=False),
    Column("voltage_drop", Float, nullable=False),
)

meters = Table(
    "tbl_meters", metadata, _id(),
    Column("meter_id", String(20), nullable=False, unique=True),
    Column("customer_name", String(120), nullable=False),
    Column("customer_type", String(20), nullable=False),
    Column("municipality", String(40), nullable=False),
    Column("contracted_kva", Float, nullable=False),
    Column("phase", String(10), nullable=False),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("seed_profile", Integer),
    Column("peak_util", Float),
    Column("pf_base", Float),
    Column("distance_k", Float),
    Column("is_live", Boolean, nullable=False, server_default="0"),
    _created(),
    Index("ix_meters_muni", "municipality"),
)

# --- Speed layer (real-time) --------------------------------------------------
readings_live = Table(
    "tbl_readings_live", metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("reading_id", String(40), nullable=False),
    Column("meter_id", String(20), nullable=False),
    Column("ts", DateTime, nullable=False),
    Column("municipality", String(40), nullable=False),
    Column("customer_type", String(20), nullable=False),
    Column("contracted_kva", Float),
    Column("voltage_v", Float), Column("current_a", Float), Column("power_kw", Float),
    Column("energy_kwh", Float), Column("power_factor", Float), Column("frequency_hz", Float),
    Column("temperature_c", Float),
    Column("voltage_dev_pct", Float), Column("freq_dev_hz", Float), Column("load_ratio", Float),
    Column("baseline_ratio", Float), Column("consistency_ratio", Float),
    Column("predicted_type", String(24), nullable=False),     # Random Forest
    Column("anomaly_score", Float),                           # 1 - P(Normal)
    Column("confidence", Float),                              # P(predicted type)
    Column("is_anomaly", Boolean, nullable=False, server_default="0"),
    Column("true_type", String(24)),                          # generator ground truth (evaluation only)
    Column("latency_ms", Float),
    Column("batch_id", Integer),
    Column("ingested_at", DateTime, server_default=func.now()),
    Index("ix_live_ts", "ts"), Index("ix_live_meter", "meter_id"), Index("ix_live_anom", "is_anomaly"),
)

stream_agg = Table(
    "tbl_stream_agg", metadata, _id(),
    Column("window_start", DateTime, nullable=False),
    Column("window_end", DateTime, nullable=False),
    Column("municipality", String(40), nullable=False),
    Column("n_readings", Integer), Column("n_anomalies", Integer),
    Column("n_sag", Integer), Column("n_overvoltage", Integer), Column("n_frequency", Integer),
    Column("n_overload", Integer), Column("n_theft", Integer), Column("n_fault", Integer),
    Column("total_kwh", Float), Column("avg_voltage", Float), Column("min_voltage", Float),
    Column("avg_frequency", Float), Column("avg_score", Float),
    UniqueConstraint("window_start", "municipality", name="uq_agg_window_muni"),
)
AGG_TYPE_COLS = {"Voltage Sag": "n_sag", "Over-Voltage": "n_overvoltage", "Frequency Deviation": "n_frequency",
                 "Overload": "n_overload", "Energy Theft": "n_theft", "Meter Fault": "n_fault"}

stream_batches = Table(
    "tbl_stream_batches", metadata, _id(),
    Column("batch_id", Integer, nullable=False),
    Column("engine", String(30), nullable=False),
    Column("started_at", DateTime, nullable=False),
    Column("n_messages", Integer), Column("n_valid", Integer), Column("n_invalid", Integer),
    Column("n_anomalies", Integer),
    Column("processing_ms", Float), Column("avg_latency_ms", Float), Column("max_latency_ms", Float),
    Column("throughput_mps", Float),
    Column("offsets", Text),
    Index("ix_batches_started", "started_at"),
)

stream_offsets = Table(
    "tbl_stream_offsets", metadata, _id(),
    Column("topic", String(80), nullable=False),
    Column("partition_no", Integer, nullable=False),
    Column("next_offset", BigInteger, nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    UniqueConstraint("topic", "partition_no", name="uq_topic_partition"),
)

heartbeats = Table(
    "tbl_heartbeats", metadata,
    Column("component", String(40), primary_key=True),
    Column("last_seen", DateTime, nullable=False),
    Column("info", Text),
)

anomaly_events = Table(
    "tbl_anomaly_events", metadata, _id(), _created(),
    Column("ts", DateTime, nullable=False),
    Column("meter_id", String(20), nullable=False),
    Column("municipality", String(40), nullable=False),
    Column("customer_type", String(20)),
    Column("anomaly_type", String(24), nullable=False),
    Column("severity", String(12), nullable=False),
    Column("anomaly_score", Float), Column("confidence", Float),
    Column("true_type", String(24)),
    Column("reason", Text),
    Column("voltage_v", Float), Column("current_a", Float), Column("power_kw", Float),
    Column("power_factor", Float), Column("frequency_hz", Float),
    Column("load_ratio", Float), Column("baseline_ratio", Float), Column("consistency_ratio", Float),
    Column("acknowledged", Boolean, nullable=False, server_default="0"),
    Column("acknowledged_at", DateTime),
    Column("note", Text),
    Index("ix_events_ts", "ts"), Index("ix_events_type", "anomaly_type"), Index("ix_events_ack", "acknowledged"),
)

# --- Machine learning ----------------------------------------------------------
models = Table(
    "tbl_models", metadata, _id(), _created(),
    Column("task", String(24), nullable=False),           # supervised (Random Forest)
    Column("algorithm", String(40), nullable=False),      # random_forest
    Column("engine", String(20), nullable=False),         # scikit-learn | spark-mllib
    Column("dataset", String(20)),
    Column("params", Text),
    Column("n_train", Integer), Column("n_test", Integer),
    # multi-class (which anomaly type)
    Column("accuracy", Float), Column("precision_macro", Float), Column("recall_macro", Float), Column("f1_macro", Float),
    # binary (anomaly vs normal)
    Column("anomaly_precision", Float), Column("anomaly_recall", Float), Column("anomaly_f1", Float),
    Column("roc_auc", Float), Column("pr_auc", Float), Column("false_positive_rate", Float),
    Column("train_seconds", Float),
    Column("feature_importance", LongText),
    Column("confusion_matrix", LongText),
    Column("class_report", LongText),
    Column("file_path", String(255)),
    Column("is_active", Boolean, nullable=False, server_default="0"),
    Column("notes", Text),
)

# --- Website accounts (login admin / user) ------------------------------------
users = Table(                         # password_hash = pbkdf2_sha256$iterations$salt$hash (never plain text)
    "tbl_users", metadata, _id(), _created(),
    Column("username", String(40), nullable=False, unique=True),
    Column("full_name", String(80)),
    Column("role", String(10), nullable=False),            # admin | user
    Column("password_hash", String(200), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default="1"),
    Column("last_login", DateTime),
)

user_sessions = Table(                 # login survives a page reload (cookie holds a random token, DB its SHA-256)
    "tbl_user_sessions", metadata, _id(), _created(),
    Column("token_hash", String(64), nullable=False, unique=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("expires_at", DateTime, nullable=False),
)

manual_checks = Table(
    "tbl_manual_checks", metadata, _id(), _created(),
    Column("model_id", Integer),
    Column("meter_id", String(20)),
    Column("inputs", Text, nullable=False),
    Column("predicted_type", String(24), nullable=False),
    Column("anomaly_score", Float),
    Column("confidence", Float),
    Column("probabilities", Text),
    Column("votes", Text),
)

# --- Batch layer (Spark results) ---------------------------------------------
pipeline_runs = Table(
    "tbl_pipeline_runs", metadata, _id(),
    Column("job_name", String(60), nullable=False),
    Column("engine", String(30), nullable=False),
    Column("dataset", String(20)),
    Column("status", String(20), nullable=False, server_default="running"),
    Column("started_at", DateTime, nullable=False),
    Column("finished_at", DateTime),
    Column("seconds", Float),
    Column("input_rows", BigInteger), Column("output_rows", BigInteger), Column("input_bytes", BigInteger),
    Column("throughput_rows_s", Float),
    Column("details", LongText),
    Column("message", Text),
)

data_quality = Table(
    "tbl_data_quality", metadata, _id(), _created(),
    Column("run_id", Integer),
    Column("dataset", String(20)),
    Column("check_name", String(40), nullable=False),
    Column("column_name", String(40)),
    Column("n_rows", BigInteger),
    Column("pct", Float),
)

benchmarks = Table(
    "tbl_benchmarks", metadata, _id(), _created(),
    Column("engine", String(20), nullable=False),
    Column("dataset_label", String(20), nullable=False),
    Column("n_rows", BigInteger), Column("size_mb", Float),
    Column("seconds", Float), Column("peak_memory_mb", Float),
    Column("run_no", Integer),
    Column("result_check", String(160)),
)

batch_anomaly_stats = Table(          # anomaly counts by a dimension (Spark groupBy)
    "tbl_batch_anomaly_stats", metadata, _id(),
    Column("dataset", String(20), nullable=False),
    Column("dimension", String(20), nullable=False),       # municipality | hour | month | customer_type
    Column("dim_value", String(40), nullable=False),
    Column("anomaly_type", String(24), nullable=False),
    Column("n_readings", BigInteger),
    Index("ix_bstats", "dataset", "dimension"),
)

batch_feature_profile = Table(        # how each anomaly type looks in each feature
    "tbl_batch_feature_profile", metadata, _id(),
    Column("dataset", String(20), nullable=False),
    Column("anomaly_type", String(24), nullable=False),
    Column("feature", String(30), nullable=False),
    Column("mean", Float), Column("std", Float), Column("p05", Float), Column("p50", Float), Column("p95", Float),
)

batch_top_meters = Table(             # meters with the most anomalous hours (e.g. theft suspects)
    "tbl_batch_top_meters", metadata, _id(),
    Column("dataset", String(20), nullable=False),
    Column("meter_id", String(20), nullable=False),
    Column("municipality", String(40)), Column("customer_type", String(20)),
    Column("anomaly_type", String(24), nullable=False),
    Column("n_hours", Integer), Column("first_ts", DateTime), Column("last_ts", DateTime),
    Column("lost_kwh_est", Float),
)


# ---------------------------------------------------------------------------
_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        kwargs: dict[str, Any] = {"pool_pre_ping": True, "future": True}
        if config.DB_URL.startswith("mysql"):
            kwargs.update(pool_recycle=1800, pool_size=10, max_overflow=20)
        _engine = create_engine(config.DB_URL, **kwargs)
    return _engine


def create_database_if_missing() -> None:
    """Create the MySQL database itself (CREATE DATABASE) when it does not exist."""
    if not config.DB_URL.startswith("mysql"):
        return
    from sqlalchemy.engine import make_url
    url = make_url(config.DB_URL)
    server = create_engine(url._replace(database=None), future=True)
    with server.begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{url.database}` "
                          "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
    server.dispose()


def create_all() -> None:
    create_database_if_missing()
    metadata.create_all(get_engine())


@contextmanager
def begin():
    with get_engine().begin() as conn:
        yield conn


def to_json(value: Any) -> str:
    def default(o):
        if hasattr(o, "isoformat"):
            return o.isoformat()
        if hasattr(o, "item"):
            return o.item()
        return str(o)
    return json.dumps(value, default=default, ensure_ascii=False)


def rows(result) -> list[dict]:
    return [dict(r._mapping) for r in result]


def bulk_insert(table: Table, records: Iterable[dict], chunk: int = 2000) -> int:
    records = list(records)
    n = 0
    with begin() as conn:
        for i in range(0, len(records), chunk):
            part = records[i:i + chunk]
            if part:
                conn.execute(insert(table), part)
                n += len(part)
    return n


def replace_dataset_rows(table: Table, dataset: str, records: list[dict]) -> int:
    """Delete the rows of one dataset and insert the new ones (idempotent Spark output)."""
    with begin() as conn:
        conn.execute(delete(table).where(table.c.dataset == dataset))
    return bulk_insert(table, records)


def get_setting(key: str, default: str | None = None) -> str | None:
    with begin() as conn:
        v = conn.execute(select(settings.c.value).where(settings.c.key == key)).scalar()
    return default if v is None else v


def set_setting(key: str, value: str) -> None:
    with begin() as conn:
        exists = conn.execute(select(settings.c.key).where(settings.c.key == key)).scalar()
        if exists:
            conn.execute(update(settings).where(settings.c.key == key).values(value=value, updated_at=datetime.now()))
        else:
            conn.execute(insert(settings).values(key=key, value=value))


def heartbeat(component: str, info: dict | None = None) -> None:
    now = datetime.now()
    payload = to_json(info or {})
    with begin() as conn:
        n = conn.execute(update(heartbeats).where(heartbeats.c.component == component)
                         .values(last_seen=now, info=payload)).rowcount
        if not n:
            conn.execute(insert(heartbeats).values(component=component, last_seen=now, info=payload))


def start_run(job_name: str, engine: str, dataset: str | None) -> int:
    with begin() as conn:
        res = conn.execute(insert(pipeline_runs).values(job_name=job_name, engine=engine, dataset=dataset,
                                                         status="running", started_at=datetime.now()))
        return int(res.inserted_primary_key[0])


def finish_run(run_id: int, status: str = "success", **values) -> None:
    with begin() as conn:
        started = conn.execute(select(pipeline_runs.c.started_at).where(pipeline_runs.c.id == run_id)).scalar()
        now = datetime.now()
        secs = (now - started).total_seconds() if started else None
        if "details" in values and not isinstance(values["details"], str):
            values["details"] = to_json(values["details"])
        rows_in = values.get("input_rows")
        if rows_in and secs:
            values.setdefault("throughput_rows_s", rows_in / secs)
        conn.execute(update(pipeline_runs).where(pipeline_runs.c.id == run_id)
                     .values(status=status, finished_at=now, seconds=secs, **values))
