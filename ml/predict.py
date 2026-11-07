"""
Model serving for anomaly detection.

* load_active()          - the active Random Forest, cached, re-checked every 10 s
* predict_frame(df)      - batch scoring for Spark Streaming: predicted_type, confidence,
                           anomaly_score (= 1 - P(Normal)), is_anomaly
* predict_one(dict)      - one reading: probabilities per anomaly type, the vote of every
                           tree of the forest and the evidence
"""
from __future__ import annotations

import threading
import time
from collections import Counter

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import select

import config
from common import db
from common import domain as D

_lock = threading.Lock()
_cache: dict = {}


def active_model_row(task: str = "supervised") -> dict | None:
    with db.begin() as conn:
        r = conn.execute(select(db.models).where(db.models.c.task == task, db.models.c.is_active.is_(True))
                         .order_by(db.models.c.id.desc()).limit(1)).first()
    return dict(r._mapping) if r else None


def load_active(task: str = "supervised", force: bool = False, max_age_s: float = 10.0):
    """Return (model_row, model_object) for a task; activating another model in the
    database takes effect in the stream within a few seconds."""
    with _lock:
        c = _cache.setdefault(task, {"id": None, "obj": None, "row": None, "checked": 0.0})
        now = time.time()
        if not force and c["obj"] is not None and now - c["checked"] < max_age_s:
            return c["row"], c["obj"]
        row = active_model_row(task)
        c["checked"] = now
        if row is None:
            c.update(id=None, obj=None, row=None)
            return None, None
        if row["id"] != c["id"]:
            c.update(id=row["id"], obj=joblib.load(config.ROOT / row["file_path"]), row=row)
        return c["row"], c["obj"]


def prepare(df: pd.DataFrame, meters: pd.DataFrame | None = None) -> pd.DataFrame:
    need = set(D.NUMERIC_FEATURES)
    return df if need.issubset(df.columns) else D.add_features_pandas(df, meters)


def predict_frame(df: pd.DataFrame, model_obj=None) -> pd.DataFrame:
    if model_obj is None:
        _row, model_obj = load_active("supervised")
    if model_obj is None:
        raise RuntimeError("No active Random Forest model - run `python -m ml.train` first.")
    df = prepare(df)
    pipe = model_obj["pipeline"]
    proba = pipe.predict_proba(df[D.FEATURES])
    classes = list(pipe.classes_)
    idx = proba.argmax(axis=1)
    out = df.copy()
    out["predicted_type"] = np.array(classes, dtype=object)[idx]
    out["confidence"] = proba.max(axis=1).round(4)
    out["anomaly_score"] = (1 - proba[:, classes.index(D.NORMAL)]).round(4)
    out["is_anomaly"] = out["predicted_type"] != D.NORMAL
    return out


def meter_info(meter_id: str) -> dict | None:
    with db.begin() as conn:
        r = conn.execute(select(db.meters).where(db.meters.c.meter_id == meter_id)).first()
    return dict(r._mapping) if r else None


def complete_inputs(inp: dict) -> dict:
    """Fill a manual reading: meter master values and current from P, V and PF when missing."""
    x = {k: v for k, v in inp.items() if v not in (None, "")}
    if x.get("meter_id"):
        m = meter_info(x["meter_id"])
        if m:
            for k in ("customer_type", "municipality", "contracted_kva", "phase", "peak_util", "pf_base"):
                x.setdefault(k, m[k])
    x.setdefault("timestamp", pd.Timestamp.now(tz=config.TIMEZONE).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"))
    for k in ("voltage_v", "power_kw", "power_factor", "frequency_hz", "temperature_c", "contracted_kva"):
        x[k] = float(x[k])
    x.setdefault("phase", "three" if (x["customer_type"] != "residential" or x["contracted_kva"] > 5.5) else "single")
    if not x.get("current_a"):
        div = 3.0 if x["phase"] == "three" else 1.0
        x["current_a"] = round(x["power_kw"] * 1000 / (x["voltage_v"] * max(x["power_factor"], 0.3)) / div, 2)
    x["current_a"] = float(x["current_a"])
    x.setdefault("energy_kwh", x["power_kw"])
    return x


def predict_one(inputs: dict) -> dict:
    row, obj = load_active("supervised")
    if obj is None:
        raise RuntimeError("No active Random Forest model - run `python -m ml.train` first.")
    x = complete_inputs(inputs)
    df = D.add_features_pandas(pd.DataFrame([x]))
    pipe = obj["pipeline"]
    proba = pipe.predict_proba(df[D.FEATURES])[0]
    classes = list(pipe.classes_)
    probs = {c: float(proba[classes.index(c)]) if c in classes else 0.0 for c in D.ANOMALY_TYPES}
    pred = max(probs, key=probs.get)
    model = pipe.named_steps["model"]
    votes = None
    if hasattr(model, "estimators_"):                       # each tree of the forest votes
        Xt = pipe.named_steps["pre"].transform(df[D.FEATURES])
        c = Counter(model.classes_[int(t.predict(Xt)[0])] for t in model.estimators_)
        votes = {a: int(c.get(a, 0)) for a in D.ANOMALY_TYPES}
    f = df.iloc[0]
    return {
        "model": {"id": row["id"], "algorithm": row["algorithm"], "anomaly_f1": row["anomaly_f1"]},
        "inputs": {k: (v.item() if isinstance(v, np.generic) else v) for k, v in x.items()},
        "features": {k: (f[k].item() if isinstance(f[k], np.generic) else f[k])
                     for k in D.NUMERIC_FEATURES + ["baseline_kw"]},
        "predicted_type": pred,
        "predicted_type_tet": D.ANOMALY_TET[pred],
        "is_anomaly": pred != D.NORMAL,
        "anomaly_score": round(1 - probs[D.NORMAL], 4),
        "confidence": round(probs[pred], 4),
        "probabilities": probs,
        "votes": votes,
        "n_trees": len(model.estimators_) if hasattr(model, "estimators_") else None,
        "evidence": D.explain_anomaly(f),
    }
