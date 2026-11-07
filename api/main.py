"""
main.py - FastAPI ingest gateway  (Bot -> API -> Kafka)

The bot (or any real smart-meter head-end) POSTs readings here; the gateway
checks the structure and publishes every reading to the Kafka topic
(key = meter_id, so one meter always lands in the same partition).

    uvicorn api.main:app --port 8000          (from the project folder)
    docs: http://localhost:8000/docs
"""
from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import func, select, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from api.schemas import DetectRequest, Reading, ReadingBatch  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402

APP_VERSION = "2.0.0"
app = FastAPI(title="PowerPredict TL - Ingest API", version=APP_VERSION,
              description="Gateway between the smart-meter bot and Apache Kafka, plus on-demand "
                          "anomaly detection with the active Random Forest.")

_state = {"received": 0, "published": 0, "batches": 0, "started": time.time(), "last_hb": 0.0,
          "last_bot": None, "last_scenario": None}
_kafka = {"producer": None, "error": None}
_lock = threading.Lock()


def ensure_topic() -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP, client_id="powerpredict-api")
    try:
        if config.KAFKA_TOPIC not in admin.list_topics():
            admin.create_topics([NewTopic(config.KAFKA_TOPIC, num_partitions=config.KAFKA_PARTITIONS,
                                          replication_factor=1)])
    finally:
        admin.close()


def producer():
    """One long-lived Kafka producer (created on first use, re-created after an error)."""
    with _lock:
        if _kafka["producer"] is None:
            from kafka import KafkaProducer
            ensure_topic()
            _kafka["producer"] = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP, acks=1, linger_ms=20,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            _kafka["error"] = None
        return _kafka["producer"]


def _reset_producer(exc: Exception) -> None:
    with _lock:
        _kafka["error"] = f"{type(exc).__name__}: {exc}"
        try:
            if _kafka["producer"] is not None:
                _kafka["producer"].close(timeout=1)
        except Exception:  # noqa: BLE001
            pass
        _kafka["producer"] = None


def publish(readings: list[Reading]) -> int:
    try:
        p = producer()
        now_ms = int(time.time() * 1000)
        for r in readings:
            msg = r.model_dump()
            msg.setdefault("producer_ts", now_ms)
            if msg["producer_ts"] is None:
                msg["producer_ts"] = now_ms
            p.send(config.KAFKA_TOPIC, key=r.meter_id, value=msg)
        p.flush(timeout=10)
    except Exception as exc:  # broker down -> 503, bot retries
        _reset_producer(exc)
        raise HTTPException(status_code=503, detail=f"Kafka not available: {exc}") from exc
    with _lock:
        _state["published"] += len(readings)
    return len(readings)


def _heartbeat() -> None:
    if time.time() - _state["last_hb"] < 5:
        return
    _state["last_hb"] = time.time()
    up = max(time.time() - _state["started"], 1)
    info = {k: _state[k] for k in ("received", "published", "batches")}
    info.update(rate_mps=round(_state["published"] / up, 1), topic=config.KAFKA_TOPIC, version=APP_VERSION)
    try:
        db.heartbeat("api", info)
        if _state["last_bot"]:
            db.heartbeat("bot", {"bot_name": _state["last_bot"], "scenario": _state["last_scenario"],
                                 "messages": _state["received"]})
    except Exception:  # noqa: BLE001 - the DB is optional for ingest
        pass


# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"service": "PowerPredict TL ingest API", "version": APP_VERSION, "kafka_topic": config.KAFKA_TOPIC,
            "flow": "bot -> FastAPI -> Kafka -> Spark Streaming (features + Random Forest) -> MySQL -> Streamlit",
            "endpoints": ["GET /health", "GET /api/stats", "GET /api/meters/live", "GET /api/scenario",
                          "POST /api/readings", "POST /api/readings/batch", "POST /api/detect",
                          "GET /api/anomalies/latest"]}


@app.get("/health")
def health():
    out = {"status": "ok", "timestamp": datetime.now().isoformat(timespec="seconds"),
           "counters": {k: _state[k] for k in ("received", "published", "batches")}}
    try:
        with db.begin() as conn:
            conn.execute(text("SELECT 1"))
        out["database"] = "connected"
    except Exception as exc:  # noqa: BLE001
        out["database"], out["status"] = f"error: {exc}", "degraded"
    try:
        producer()
        out["kafka"] = "connected"
    except Exception as exc:  # noqa: BLE001
        _reset_producer(exc)
        out["kafka"], out["status"] = f"error: {exc}", "degraded"
    return out


@app.get("/api/stats")
def stats():
    with db.begin() as conn:
        n_live = conn.execute(select(func.count()).select_from(db.readings_live)).scalar()
        since = conn.execute(select(func.max(db.readings_live.c.ts))).scalar()
        by_type = {}
        if since:
            rows = conn.execute(text("SELECT predicted_type, COUNT(*) FROM tbl_readings_live "
                                     "WHERE ts >= :s GROUP BY predicted_type"),
                                {"s": since - timedelta(minutes=10)})
            by_type = {r[0]: int(r[1]) for r in rows}
        n_events = conn.execute(select(func.count()).select_from(db.anomaly_events)).scalar()
    return {"readings_live": int(n_live or 0), "last_reading": since, "last_10_min_by_type": by_type,
            "anomaly_events": int(n_events or 0), "api": {k: _state[k] for k in ("received", "published", "batches")}}


@app.get("/api/meters/live")
def live_meters():
    cols = [db.meters.c[k] for k in ("meter_id", "customer_type", "municipality", "contracted_kva", "phase",
                                     "peak_util", "pf_base", "distance_k", "latitude", "longitude")]
    with db.begin() as conn:
        rows = db.rows(conn.execute(select(*cols).where(db.meters.c.is_live.is_(True))))
    return {"count": len(rows), "meters": rows}


@app.get("/api/scenario")
def scenario():
    return {"scenario": db.get_setting("scenario", "normal") or "normal",
            "trigger": db.get_setting("trigger", "") or "",
            "interval_s": float(db.get_setting("producer_interval_s", str(config.PRODUCER_INTERVAL_SECONDS)))}


@app.post("/api/readings", status_code=201)
def post_reading(reading: Reading):
    _state["received"] += 1
    n = publish([reading])
    _heartbeat()
    return {"status": "ok", "published": n, "topic": config.KAFKA_TOPIC}


@app.post("/api/readings/batch", status_code=201)
def post_batch(batch: ReadingBatch):
    _state["received"] += len(batch.readings)
    _state["batches"] += 1
    _state["last_bot"], _state["last_scenario"] = batch.bot_name, batch.scenario
    n = publish(batch.readings)
    _heartbeat()
    return {"status": "ok", "published": n, "topic": config.KAFKA_TOPIC, "total_published": _state["published"]}


@app.post("/api/detect")
def detect(req: DetectRequest):
    from ml.predict import predict_one
    inp = req.model_dump()
    if not inp.get("meter_id") and not all(inp.get(k) for k in ("customer_type", "municipality", "contracted_kva")):
        raise HTTPException(422, "give meter_id, or customer_type + municipality + contracted_kva")
    if inp.get("customer_type") and inp["customer_type"] not in D.CUSTOMER_TYPES:
        raise HTTPException(422, f"customer_type must be one of {D.CUSTOMER_TYPES}")
    try:
        return predict_one(inp)
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/api/anomalies/latest")
def latest_anomalies(limit: int = Query(50, ge=1, le=500)):
    with db.begin() as conn:
        rows = db.rows(conn.execute(select(db.anomaly_events).order_by(db.anomaly_events.c.id.desc()).limit(limit)))
    return {"count": len(rows), "events": rows}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
