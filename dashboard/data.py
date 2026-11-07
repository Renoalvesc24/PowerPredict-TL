"""
data.py - kuneksaun MySQL no query sira ne'ebé dashboard uza (ho cache).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(1, str(ROOT))

import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from dash_config import CONFIG  # noqa: E402

COUNTABLE = {"tbl_readings_live", "tbl_anomaly_events", "tbl_stream_batches", "tbl_stream_agg", "tbl_models",
             "tbl_meters", "tbl_manual_checks", "tbl_batch_anomaly_stats", "tbl_benchmarks", "tbl_pipeline_runs"}


@st.cache_resource
def engine():
    return db.get_engine()


def q(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def ping() -> bool:
    try:
        with engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@st.cache_data(ttl=5)
def heartbeats() -> dict:
    """component -> {"age_s": seconds since last heartbeat, "info": dict}"""
    try:
        df = q("SELECT component, last_seen, info FROM tbl_heartbeats")
    except Exception:
        return {}
    out = {}
    for r in df.itertuples():
        try:
            info = json.loads(r.info or "{}")
        except ValueError:
            info = {}
        out[r.component] = {"age_s": (datetime.now() - pd.Timestamp(r.last_seen).to_pydatetime()).total_seconds(),
                            "info": info}
    return out


def is_alive(component: str) -> bool:
    hb = heartbeats().get(component)
    return bool(hb) and hb["age_s"] <= CONFIG["stream_stale_seconds"]


@st.cache_data(ttl=10)
def table_count(name: str) -> int:
    if name not in COUNTABLE:
        return 0
    try:
        with engine().connect() as conn:
            return int(conn.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar() or 0)
    except Exception:
        return 0


@st.cache_data(ttl=5)
def live_window(limit: int) -> pd.DataFrame:
    """Readings ikus `limit` (orden ID = orden tama)."""
    try:
        df = q("""SELECT id, ts, meter_id, municipality, customer_type, contracted_kva, voltage_v, current_a, power_kw,
                         energy_kwh, power_factor, frequency_hz, temperature_c, voltage_dev_pct, freq_dev_hz,
                         load_ratio, baseline_ratio, consistency_ratio, predicted_type, anomaly_score, confidence,
                         is_anomaly, true_type, latency_ms
                  FROM tbl_readings_live ORDER BY id DESC LIMIT :n""", n=int(limit))
        df["ts"] = pd.to_datetime(df["ts"])
        df["is_anomaly"] = df["is_anomaly"].astype(bool)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def trend(minutes: int) -> pd.DataFrame:
    """Agregasaun 1 minutu (Spark) - minutu sira la iha dadus hatudu 0."""
    try:
        df = q("""SELECT window_start, SUM(n_readings) n_readings, SUM(n_anomalies) n_anomalies,
                         SUM(n_sag) n_sag, SUM(n_overvoltage) n_overvoltage, SUM(n_frequency) n_frequency,
                         SUM(n_overload) n_overload, SUM(n_theft) n_theft, SUM(n_fault) n_fault,
                         SUM(total_kwh) total_kwh, AVG(avg_voltage) avg_voltage
                  FROM tbl_stream_agg
                  WHERE window_start >= (SELECT MAX(window_start) FROM tbl_stream_agg) - INTERVAL :m MINUTE
                  GROUP BY window_start ORDER BY window_start""", m=int(minutes))
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.iloc[:-1] if len(df) > 1 else df                     # minutu ikus seidauk kompletu
    full = pd.date_range(df["window_start"].min(), df["window_start"].max(), freq="1min")
    return df.set_index("window_start").reindex(full).fillna(0).rename_axis("window_start").reset_index()


@st.cache_data(ttl=30)
def models() -> pd.DataFrame:
    try:
        return q("SELECT * FROM tbl_models ORDER BY id")
    except Exception:
        return pd.DataFrame()


def active_model(task: str = "supervised") -> dict | None:
    m = models()
    if m.empty:
        return None
    a = m[(m.task == task) & (m.is_active == 1)]
    return a.iloc[-1].to_dict() if len(a) else None


@st.cache_data(ttl=10)
def events(limit: int = 2000) -> pd.DataFrame:
    try:
        df = q("SELECT * FROM tbl_anomaly_events ORDER BY id DESC LIMIT :n", n=int(limit))
        df["ts"] = pd.to_datetime(df["ts"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def live_meters() -> pd.DataFrame:
    try:
        return q("SELECT * FROM tbl_meters WHERE is_live = 1 ORDER BY municipality, meter_id")
    except Exception:
        return pd.DataFrame()


def jloads(v, default=None):
    try:
        return json.loads(v) if isinstance(v, str) else (v if v is not None else default)
    except ValueError:
        return default


def setting(key: str, default: str = "") -> str:
    try:
        return db.get_setting(key, default) or default
    except Exception:
        return default


def set_setting(key: str, value: str) -> None:
    db.set_setting(key, value)
    st.cache_data.clear()


def manifest(label: str) -> dict | None:
    p = config.RAW_DIR / f"readings_{label}" / "_manifest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


ANOMALY_TET = D.ANOMALY_TET
