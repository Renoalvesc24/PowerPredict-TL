"""Automated tests: domain rules, feature engineering, Random Forest serving, API, dashboard ML.

    python -m pytest tests -q
"""
import sys
import zlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common import domain as D  # noqa: E402

SAMPLES = ROOT / "data" / "samples"


# --------------------------------------------------------------------------- domain
def test_label_priority():
    shape = 4
    masks = {"Overload": np.array([1, 1, 0, 0], bool), "Energy Theft": np.array([0, 1, 0, 0], bool),
             "Meter Fault": np.array([0, 0, 0, 1], bool), "Voltage Sag": np.array([0, 0, 0, 1], bool)}
    labels = [D.ANOMALY_TYPES[i] for i in D.assign_label(masks, shape)]
    assert labels == ["Overload", "Energy Theft", "Normal", "Meter Fault"]


def test_baseline_scalar_and_vector_agree():
    one = float(D.baseline_kw("residential", 2.2, 0.6, 0.95, 20, 0, 30.0))
    vec = D.baseline_kw(np.array(["residential", "commercial"]), np.array([2.2, 11.0]), np.array([0.6, 0.65]),
                        np.array([0.95, 0.9]), np.array([20, 11]), np.array([0, 1]), np.array([30.0, 30.0]))
    assert abs(one - vec[0]) < 1e-9
    # 2.2 kVA x 0.6 x shape(20h)=1.0 x temp factor(1+0.015*3) x 0.95
    assert abs(one - 2.2 * 0.6 * 1.0 * 1.045 * 0.95) < 1e-9


def test_feature_engineering():
    df = pd.DataFrame([{"timestamp": "2026-11-07 20:00:00", "meter_id": "M1", "customer_type": "residential",
                        "contracted_kva": 2.2, "phase": "single", "voltage_v": 198.0, "current_a": 2.0,
                        "power_kw": 0.38, "power_factor": 0.95, "frequency_hz": 49.4, "temperature_c": 27.0,
                        "peak_util": 0.6, "pf_base": 0.95}])
    f = D.add_features_pandas(df).iloc[0]
    assert f.hour == 20 and f.is_weekend == 1                        # 7 Nov 2026 is a Saturday
    assert abs(f.voltage_dev_pct - (-10.0)) < 1e-6
    assert abs(f.freq_dev_hz - (-0.6)) < 1e-6
    assert abs(f.consistency_ratio - 0.38 / (198 * 2.0 * 0.95 / 1000)) < 1e-3
    base = 2.2 * 0.6 * 1.0 * D.WEEKEND_FACTOR["residential"] * 1.0 * 0.95
    assert abs(f.baseline_ratio - 0.38 / base) < 1e-3


def test_explain_anomaly_mentions_theft_and_fault():
    r = {"voltage_v": 220, "frequency_hz": 50, "load_ratio": 0.1, "baseline_ratio": 0.12, "consistency_ratio": 2.5}
    text = " ".join(D.explain_anomaly(r)).lower()
    assert "na'ok" in text and "sensor" in text


def test_meter_split_is_reproducible():
    """Spark's crc32 split and Python's zlib.crc32 must agree (pandas can rebuild the split)."""
    ids = [f"TL-DIL-{i:06d}" for i in range(1, 501)]
    share = np.mean([zlib.crc32(m.encode()) % 5 == 0 for m in ids])
    assert 0.12 < share < 0.28


# --------------------------------------------------------------------------- samples
def test_samples_exist_and_cover_every_type():
    s = pd.read_csv(SAMPLES / "sample_readings.csv")
    assert len(s) == 100 and set(s.anomaly_type) == set(D.ANOMALY_TYPES)
    m = pd.read_csv(SAMPLES / "sample_meters.csv")
    assert set(s.meter_id) <= set(m.meter_id)


# --------------------------------------------------------------------------- model (needs DB + trained model)
@pytest.fixture(scope="module")
def model():
    try:
        from ml.predict import load_active
        row, obj = load_active("supervised", force=True)
    except Exception as exc:
        pytest.skip(f"database not available: {exc}")
    if obj is None:
        pytest.skip("no active model - run python -m ml.train")
    return obj


def test_random_forest_on_samples(model):
    from ml.predict import predict_frame
    s = pd.read_csv(SAMPLES / "sample_readings.csv")
    m = pd.read_csv(SAMPLES / "sample_meters.csv")
    out = predict_frame(D.add_features_pandas(s, m), model)
    acc = (out.predicted_type == s.anomaly_type).mean()
    assert acc >= 0.85, acc
    assert out.loc[s.anomaly_type == "Normal", "anomaly_score"].median() < 0.2
    assert out.loc[s.anomaly_type != "Normal", "anomaly_score"].median() > 0.8


def test_predict_one_theft_votes(model):
    from ml.predict import predict_one
    r = predict_one({"customer_type": "residential", "municipality": "Dili", "contracted_kva": 3.5,
                     "voltage_v": 221, "power_kw": 0.08, "power_factor": 0.95, "frequency_hz": 50.0,
                     "temperature_c": 29, "timestamp": "2026-11-06 20:00:00"})
    assert r["predicted_type"] == "Energy Theft"
    assert sum(r["votes"].values()) == r["n_trees"]
    assert r["anomaly_score"] > 0.6


# --------------------------------------------------------------------------- API
def test_api_root_and_validation():
    try:
        from fastapi.testclient import TestClient

        from api.main import app
    except Exception as exc:
        pytest.skip(f"fastapi not available: {exc}")
    c = TestClient(app)
    assert "endpoints" in c.get("/").json()
    bad = c.post("/api/readings/batch", json={"readings": [{"meter_id": "X"}]})
    assert bad.status_code == 422                                   # structure is validated by pydantic


# --------------------------------------------------------------------------- dashboard ML modules
def test_live_rf_metrics_and_theft_suspects():
    sys.path.insert(0, str(ROOT / "dashboard"))
    from ml_anomaly import run_live_rf, score_threshold_table, theft_suspects
    n = 200
    true = ["Normal"] * 180 + ["Energy Theft"] * 20
    pred = list(true)
    pred[0] = "Energy Theft"                                        # one false alarm
    pred[199] = "Normal"                                            # one miss
    df = pd.DataFrame({"true_type": true, "predicted_type": pred, "is_anomaly": [p != "Normal" for p in pred],
                       "anomaly_score": np.r_[np.full(180, 0.05), np.full(20, 0.95)],
                       "meter_id": ["M%d" % (i % 10) for i in range(n)], "municipality": "Dili",
                       "customer_type": "residential", "baseline_ratio": 0.5, "power_kw": 0.3})
    rf = run_live_rf(df)
    assert abs(rf["anomaly_precision"] - 19 / 20) < 1e-9 and abs(rf["anomaly_recall"] - 19 / 20) < 1e-9
    th = score_threshold_table(df)
    assert th.loc[th.Threshold == 0.5, "Recall"].iloc[0] == 1.0
    sus = theft_suspects(df.assign(meter_id=["T" if i >= 180 else "M%d" % (i % 10) for i in range(n)]))
    assert list(sus.meter_id) == ["T"]
