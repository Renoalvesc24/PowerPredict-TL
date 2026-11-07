"""
ml_anomaly.py - Random Forest (supervised) iha stream live

Modelu la treina fali iha kada refresh (diferente husi projetu referénsia): Random
Forest treinu ona iha batch layer (Spark + scikit-learn, dataset 20GB) no Spark
Streaming uza nia ba kada reading. Módulu ne'e kompara prediksaun sira ho
ground truth husi jeradór (true_type) iha readings ikus sira.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (accuracy_score, confusion_matrix, f1_score, precision_recall_fscore_support,
                             precision_score, recall_score)

from common import domain as D


def run_live_rf(df: pd.DataFrame) -> dict:
    data = df.dropna(subset=["predicted_type", "true_type"])
    data = data[data["true_type"].isin(D.ANOMALY_TYPES)]
    if len(data) < 50:
        return {"error": "Seidauk iha readings stream ho ground truth (mínimu 50)"}
    yt, yp = data["true_type"].to_numpy(), data["predicted_type"].to_numpy()
    tb, pb = yt != D.NORMAL, yp != D.NORMAL
    p, r, f, s = precision_recall_fscore_support(yt, yp, labels=D.ANOMALY_TYPES, zero_division=0)
    per = pd.DataFrame({"Tipu": [D.ANOMALY_TET[a] for a in D.ANOMALY_TYPES],
                        "Precision": p, "Recall": r, "F1-Score": f, "Readings (atuál)": s,
                        "Prediction": [int((yp == a).sum()) for a in D.ANOMALY_TYPES]})
    return {
        "n": len(data),
        "accuracy": float(accuracy_score(yt, yp)),
        "anomaly_precision": float(precision_score(tb, pb, zero_division=0)),
        "anomaly_recall": float(recall_score(tb, pb, zero_division=0)),
        "anomaly_f1": float(f1_score(tb, pb, zero_division=0)),
        "true_rate": float(tb.mean()), "pred_rate": float(pb.mean()),
        "per_type": per,
        "confusion": confusion_matrix(yt, yp, labels=D.ANOMALY_TYPES).tolist(),
    }


def sample_predictions(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Anomalous readings ikus (prediksaun ka atuál) ho rezultadu."""
    a = df[(df["predicted_type"] != D.NORMAL) | (df["true_type"].fillna(D.NORMAL) != D.NORMAL)].head(n)
    out = a[["ts", "meter_id", "municipality", "true_type", "predicted_type", "anomaly_score"]].copy()
    out["Loos"] = out["true_type"] == out["predicted_type"]
    out["true_type"] = out["true_type"].map(lambda v: D.ANOMALY_TET.get(v, v))
    out["predicted_type"] = out["predicted_type"].map(lambda v: D.ANOMALY_TET.get(v, v))
    return out.rename(columns={"ts": "Tempu", "meter_id": "Meter", "municipality": "Municipality",
                               "true_type": "Atuál", "predicted_type": "Prediction", "anomaly_score": "Anomaly Score"})


def score_threshold_table(df: pd.DataFrame, thresholds=(0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) -> pd.DataFrame:
    """Precision / Recall / F1 of the flag `Anomaly Score >= threshold` vs ground truth (live)."""
    data = df.dropna(subset=["anomaly_score", "true_type"])
    tb = (data["true_type"] != D.NORMAL).to_numpy()
    rows = []
    for th in thresholds:
        pb = (data["anomaly_score"] >= th).to_numpy()
        rows.append({"Threshold": th, "Precision": float(precision_score(tb, pb, zero_division=0)),
                     "Recall": float(recall_score(tb, pb, zero_division=0)),
                     "F1-Score": float(f1_score(tb, pb, zero_division=0)), "Flag": int(pb.sum())})
    return pd.DataFrame(rows)


def run_live_score(df: pd.DataFrame, threshold: float) -> dict:
    """Anomaly Score (= 1 - P(Normal) from the Random Forest) on the live window."""
    from sklearn.metrics import average_precision_score, roc_auc_score
    data = df.dropna(subset=["anomaly_score", "true_type"])
    if len(data) < 50:
        return {"error": "Seidauk iha Anomaly Score ho ground truth"}
    tb = (data["true_type"] != D.NORMAL).to_numpy()
    pb = (data["anomaly_score"] >= threshold).to_numpy()
    both = 0 < tb.sum() < len(tb)
    return {"n": len(data), "flagged": int(pb.sum()),
            "precision": float(precision_score(tb, pb, zero_division=0)),
            "recall": float(recall_score(tb, pb, zero_division=0)),
            "f1": float(f1_score(tb, pb, zero_division=0)),
            "roc_auc": float(roc_auc_score(tb, data["anomaly_score"])) if both else None,
            "pr_auc": float(average_precision_score(tb, data["anomaly_score"])) if both else None,
            "table": score_threshold_table(data),
            "scores": pd.DataFrame({"Anomaly Score": data["anomaly_score"].to_numpy(),
                                    "Ground truth": np.where(tb, "Anomalia", "Normal")})}


def theft_suspects(df: pd.DataFrame, min_share: float = 0.5, min_readings: int = 10) -> pd.DataFrame:
    """Meter ne'ebé Random Forest dehan 'Energy Theft' iha >= 50 % husi nia readings ikus."""
    g = df.groupby(["meter_id", "municipality", "customer_type"])
    s = g.agg(n=("predicted_type", "size"),
              theft=("predicted_type", lambda x: float((x == "Energy Theft").mean())),
              baseline=("baseline_ratio", "median"), kw=("power_kw", "median")).reset_index()
    s = s[(s.n >= min_readings) & (s.theft >= min_share)].sort_values("theft", ascending=False)
    return s.assign(theft=lambda d: (d.theft * 100).round(0), baseline=lambda d: (d.baseline * 100).round(0))
