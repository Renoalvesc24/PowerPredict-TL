"""
Anomaly detection model: Random Forest (scikit-learn), trained on the samples Spark prepares.

Task: for every smart-meter reading decide WHICH anomaly it is
      (Normal, Voltage Sag, Over-Voltage, Frequency Deviation, Overload,
       Energy Theft, Meter Fault)  ->  supervised multi-class classification.
      anomaly score = 1 - P(Normal)  ->  also a binary anomaly detector.

Random Forest = bagging of 150 CART trees, sqrt(features) per split, Gini impurity,
majority voting. It is the only machine-learning method of the system: on the same 20GB
test sample (1M readings of held-out meters) it was the most accurate (anomaly F1 0.954 vs
Decision Tree 0.925, Logistic Regression 0.896, Isolation Forest 0.144).

Honest evaluation
  * Train / test are split BY METER (1 in 5 meters held out by Spark), so a long
    theft episode can never be in both sets.
  * The training sample undersamples Normal; the TEST sample keeps the natural
    distribution (~3.9 % anomalies), i.e. what the model meets in production.
  * 3-fold StratifiedGroupKFold (groups = meters) cross-validation.

    python -m ml.train --dataset 20GB
    python -m ml.train --dataset 20GB --trees 200 --depth 30
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, average_precision_score, classification_report, confusion_matrix,
                             f1_score, precision_recall_fscore_support, precision_score, recall_score,
                             roc_auc_score)
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import insert, update

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402

# normal_weight: class_weight of Normal. The training sample undersamples Normal (~56 % instead of ~96 %),
# so without a weight the forest says "anomaly" too easily on real data (low precision). Weight 4 brings
# the effective Normal share to ~83 %.
DEFAULT_RF = {"n_estimators": 150, "max_depth": 24, "min_samples_leaf": 2, "max_features": "sqrt",
              "normal_weight": 4.0}


def read_parquet_folder(folder: Path) -> pd.DataFrame:
    files = sorted(str(p) for p in folder.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}. Run pipeline.spark_batch_etl first.")
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)
    return df


def load_samples(dataset: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = read_parquet_folder(config.CURATED_DIR / f"train_sample_{dataset}")
    test = read_parquet_folder(config.CURATED_DIR / f"test_sample_{dataset}")
    return train.reset_index(drop=True), test.reset_index(drop=True)


def build_pipeline(params: dict) -> Pipeline:
    pre = ColumnTransformer([("num", "passthrough", D.NUMERIC_FEATURES),
                             ("cat", OneHotEncoder(handle_unknown="ignore"), D.CATEGORICAL_FEATURES)])
    depth = params.get("max_depth")
    depth = None if depth in (None, "", "none", "None", 0, "0") else int(depth)
    w = float(params.get("normal_weight", 1.0) or 1.0)
    model = RandomForestClassifier(n_estimators=int(params.get("n_estimators", 150)), max_depth=depth,
                                   min_samples_leaf=int(params.get("min_samples_leaf", 2)),
                                   max_features=params.get("max_features", "sqrt"), criterion="gini",
                                   class_weight={D.NORMAL: w} if w != 1.0 else None,
                                   bootstrap=True, n_jobs=-1, random_state=42)
    return Pipeline([("pre", pre), ("model", model)])


def grouped_importance(pipe: Pipeline) -> dict[str, float]:
    """Gini importance per ORIGINAL feature (one-hot columns are summed back), in %."""
    names = pipe.named_steps["pre"].get_feature_names_out()
    raw = pipe.named_steps["model"].feature_importances_
    out = {f: 0.0 for f in D.FEATURES}
    for n, v in zip(names, raw):
        n = n.split("__", 1)[1]
        key = n if n in out else next((c for c in D.CATEGORICAL_FEATURES if n.startswith(c + "_")), n)
        out[key] = out.get(key, 0.0) + float(v)
    total = sum(out.values()) or 1.0
    return dict(sorted(((k, round(v / total * 100, 3)) for k, v in out.items()), key=lambda kv: -kv[1]))


def evaluate(y_true, y_pred, score) -> dict:
    labels = D.ANOMALY_TYPES
    p, r, f, s = precision_recall_fscore_support(y_true, y_pred, labels=labels, zero_division=0)
    yb = (np.asarray(y_true) != D.NORMAL).astype(int)
    pb = (np.asarray(y_pred) != D.NORMAL).astype(int)
    tn, fp = int(((yb == 0) & (pb == 0)).sum()), int(((yb == 0) & (pb == 1)).sum())
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision_macro": float(p.mean()), "recall_macro": float(r.mean()), "f1_macro": float(f.mean()),
        "anomaly_precision": float(precision_score(yb, pb, zero_division=0)),
        "anomaly_recall": float(recall_score(yb, pb, zero_division=0)),
        "anomaly_f1": float(f1_score(yb, pb, zero_division=0)),
        "false_positive_rate": fp / max(fp + tn, 1),
        "roc_auc": float(roc_auc_score(yb, score)), "pr_auc": float(average_precision_score(yb, score)),
        "binary": {"tn": tn, "fp": fp, "fn": int(((yb == 1) & (pb == 0)).sum()), "tp": int(((yb == 1) & (pb == 1)).sum())},
        "per_class": {lab: {"precision": float(p[i]), "recall": float(r[i]), "f1": float(f[i]), "support": int(s[i])}
                      for i, lab in enumerate(labels)},
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=labels).tolist(),
        "report_text": classification_report(y_true, y_pred, labels=labels, digits=4, zero_division=0),
    }


def threshold_table(y_true, score) -> list[dict]:
    """Precision / recall of the anomaly flag for different score thresholds."""
    yb = (np.asarray(y_true) != D.NORMAL).astype(int)
    out = []
    for th in (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9):
        pb = (score >= th).astype(int)
        out.append({"threshold": th, "precision": float(precision_score(yb, pb, zero_division=0)),
                    "recall": float(recall_score(yb, pb, zero_division=0)),
                    "f1": float(f1_score(yb, pb, zero_division=0)), "flagged": int(pb.sum())})
    return out


def cross_validate(train: pd.DataFrame, params: dict, rows: int = 120_000) -> dict:
    df = train.sample(min(rows, len(train)), random_state=3)
    cv = StratifiedGroupKFold(n_splits=3, shuffle=True, random_state=42)
    macro, anom = [], []
    for tr_idx, va_idx in cv.split(df, df[D.TARGET], groups=df["meter_id"]):
        pipe = build_pipeline({**params, "n_estimators": 80})
        pipe.fit(df.iloc[tr_idx][D.FEATURES], df.iloc[tr_idx][D.TARGET])
        pred = pipe.predict(df.iloc[va_idx][D.FEATURES])
        yv = df.iloc[va_idx][D.TARGET]
        macro.append(f1_score(yv, pred, average="macro"))
        anom.append(f1_score(yv != D.NORMAL, pred != D.NORMAL))
    return {"folds": 3, "rows": len(df), "macro_f1_mean": float(np.mean(macro)), "macro_f1_std": float(np.std(macro)),
            "anomaly_f1_mean": float(np.mean(anom)), "anomaly_f1_std": float(np.std(anom)),
            "split": "StratifiedGroupKFold (groups = meter_id)"}


def train_random_forest(dataset: str = "20GB", params: dict | None = None, cv: bool = True, activate: bool = True,
                        progress=None) -> dict:
    params = {**DEFAULT_RF, **(params or {})}
    say = progress or (lambda msg: print(msg, flush=True))
    train, test = load_samples(dataset)
    natural_rate = float((test[D.TARGET] != D.NORMAL).mean())
    say(f"train {len(train):,} rows {train[D.TARGET].value_counts().to_dict()}")
    say(f"test  {len(test):,} rows (held-out meters, natural anomaly rate {natural_rate * 100:.2f}%)")
    say("Training Random Forest ...")
    pipe = build_pipeline(params)
    t = time.time()
    pipe.fit(train[D.FEATURES], train[D.TARGET])
    train_s = time.time() - t
    t = time.time()
    proba = pipe.predict_proba(test[D.FEATURES])
    infer_ms = (time.time() - t) / len(test) * 1e6
    classes = list(pipe.classes_)
    pred = np.array(classes, dtype=object)[proba.argmax(axis=1)]
    score = 1 - proba[:, classes.index(D.NORMAL)]
    m = evaluate(test[D.TARGET], pred, score)
    extra = {"thresholds": threshold_table(test[D.TARGET], score), "inference_ms_per_1000": round(infer_ms, 3)}
    if cv:
        say("  cross-validation (3-fold, grouped by meter) ...")
        extra["cv"] = cross_validate(train, params)
    path = config.MODEL_DIR / f"anomaly_random_forest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
    joblib.dump({"pipeline": pipe, "features": D.FEATURES, "classes": classes, "algorithm": "random_forest",
                 "dataset": dataset}, path, compress=3)
    record = {
        "task": "supervised", "algorithm": "random_forest", "engine": "scikit-learn", "dataset": dataset,
        "params": db.to_json({k: params[k] for k in DEFAULT_RF}), "n_train": len(train), "n_test": len(test),
        **{k: m[k] for k in ("accuracy", "precision_macro", "recall_macro", "f1_macro", "anomaly_precision",
                             "anomaly_recall", "anomaly_f1", "roc_auc", "pr_auc", "false_positive_rate")},
        "train_seconds": round(train_s, 3),
        "feature_importance": db.to_json(grouped_importance(pipe)),
        "confusion_matrix": db.to_json({"labels": D.ANOMALY_TYPES, "matrix": m["confusion_matrix"], "binary": m["binary"]}),
        "class_report": db.to_json({"per_class": m["per_class"], "text": m["report_text"],
                                    "natural_anomaly_rate": natural_rate,
                                    "model_size_mb": round(path.stat().st_size / 1024**2, 2), **extra}),
        "file_path": str(path.relative_to(config.ROOT)),
        "notes": f"Random Forest - train: Spark sample of {dataset} (train meters), test: held-out meters, natural distribution",
    }
    with db.begin() as conn:
        if activate:
            conn.execute(update(db.models).where(db.models.c.task == "supervised").values(is_active=False))
        model_id = int(conn.execute(insert(db.models).values(**record, is_active=activate)).inserted_primary_key[0])
    say(f"  Random Forest #{model_id}: anomaly P {m['anomaly_precision']:.4f} R {m['anomaly_recall']:.4f} "
        f"F1 {m['anomaly_f1']:.4f} PR-AUC {m['pr_auc']:.4f} | type macro-F1 {m['f1_macro']:.4f} ({train_s:.1f}s)")
    return {"id": model_id, "anomaly_f1": m["anomaly_f1"], "pr_auc": m["pr_auc"], "f1_macro": m["f1_macro"],
            "train_seconds": train_s}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", default="20GB")
    ap.add_argument("--trees", type=int, default=DEFAULT_RF["n_estimators"])
    ap.add_argument("--depth", type=int, default=DEFAULT_RF["max_depth"])
    ap.add_argument("--normal-weight", type=float, default=DEFAULT_RF["normal_weight"],
                    help="class_weight of Normal (corrects the undersampled training set)")
    ap.add_argument("--no-cv", action="store_true")
    ap.add_argument("--no-activate", action="store_true", help="register the model without making it active")
    args = ap.parse_args()
    res = train_random_forest(args.dataset, {"n_estimators": args.trees, "max_depth": args.depth,
                                             "normal_weight": args.normal_weight},
                              cv=not args.no_cv, activate=not args.no_activate)
    print(json.dumps(res, indent=2, default=str))


if __name__ == "__main__":
    main()
