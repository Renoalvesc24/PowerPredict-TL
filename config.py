"""
PowerPredict TL - central configuration.

Every setting can be overridden in a `.env` file next to this module
(see `.env.example`) or with an environment variable of the same name.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file(ROOT / ".env")


def env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key, default)


# --- Paths -----------------------------------------------------------------
DATA_DIR = Path(env("PP_DATA_DIR", str(ROOT / "data")))
RAW_DIR = DATA_DIR / "raw"                 # raw zone of the data lake (CSV)
REFERENCE_DIR = RAW_DIR / "reference"      # meters, municipalities, weather
LAKE_DIR = DATA_DIR / "lake"               # curated + stream zones (Parquet)
CURATED_DIR = LAKE_DIR / "curated"
STREAM_LAKE_DIR = LAKE_DIR / "stream"
BENCH_DIR = DATA_DIR / "benchmarks"
MODEL_DIR = ROOT / "models"
LOG_DIR = ROOT / "logs"

# real seed data (UCI ElectricityLoadDiagrams20112014, monthly consumption of 264 customers) - inside the project
UCI_SEED_DIR = Path(env("PP_UCI_SEED_DIR", str(ROOT / "data" / "seed" / "uci_real")))

# --- Database (serving layer) ------------------------------------------------
DB_URL = env(
    "PP_DB_URL",
    "mysql+pymysql://root:@localhost:3306/powerpredict_db?charset=utf8mb4",
)

# --- Kafka (ingestion layer) -------------------------------------------------
KAFKA_HOME = Path(env("PP_KAFKA_HOME", r"C:\kafka\kafka_2.13-3.9.1"))
KAFKA_BOOTSTRAP = env("PP_KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = env("PP_KAFKA_TOPIC", "smart-meter-readings")
KAFKA_PARTITIONS = int(env("PP_KAFKA_PARTITIONS", "3"))

# --- Spark (processing layer) ------------------------------------------------
SPARK_MASTER = env("PP_SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = env("PP_SPARK_DRIVER_MEMORY", "12g")
SPARK_SHUFFLE_PARTITIONS = env("PP_SPARK_SHUFFLE_PARTITIONS", "64")
# folder for Spark spill / shuffle files (default: system temp). Put it on a big disk for the 20GB dataset.
SPARK_LOCAL_DIR = env("PP_SPARK_LOCAL_DIR")
SPARK_KAFKA_PACKAGE = env(
    "PP_SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
)

# --- Streaming ---------------------------------------------------------------
STREAM_TRIGGER_SECONDS = float(env("PP_STREAM_TRIGGER_SECONDS", "3"))
PRODUCER_INTERVAL_SECONDS = float(env("PP_PRODUCER_INTERVAL_SECONDS", "2"))
LIVE_FLEET_SIZE = int(env("PP_LIVE_FLEET_SIZE", "300"))
LIVE_RETENTION_HOURS = int(env("PP_LIVE_RETENTION_HOURS", "24"))

# --- Website (Streamlit) ------------------------------------------------------
DASHBOARD_PORT = int(env("PP_DASHBOARD_PORT", "8505"))
# public website: a "guest" button on the login page gives read-only access without an account
PUBLIC_GUEST = env("PP_PUBLIC_GUEST", "0") == "1"
# public copy of the website with NO account login at all (guest view only; admins use the local website)
PUBLIC_NO_LOGIN = env("PP_PUBLIC_NO_LOGIN", "0") == "1"

TIMEZONE = "Asia/Dili"
PYTHON = sys.executable

for _d in (RAW_DIR, REFERENCE_DIR, CURATED_DIR, STREAM_LAKE_DIR, BENCH_DIR, MODEL_DIR, LOG_DIR,
           *([Path(SPARK_LOCAL_DIR)] if SPARK_LOCAL_DIR else [])):
    _d.mkdir(parents=True, exist_ok=True)
