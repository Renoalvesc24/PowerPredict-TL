"""Collect every number used in the report from the running system -> facts.json (anomaly-detection version)

Datasets 5 GB / 10 GB / 20 GB. The 20 GB dataset is the main dataset (training, analytics, manual example).
"""
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from ml.train import read_parquet_folder  # noqa: E402

OUT = Path(__file__).resolve().parent / "facts.json"
SIZES = ["5GB", "10GB", "20GB"]
MAIN = "20GB"
F: dict = {"sizes": SIZES, "main": MAIN, "data_dir": str(config.DATA_DIR)}


def q(sql, **kw):
    with db.begin() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql), kw)]


def jl(v):
    return json.loads(v) if isinstance(v, str) and v else v


# ---- datasets
F["datasets"] = []
for label in SIZES:
    m = json.loads((config.RAW_DIR / f"readings_{label}" / "_manifest.json").read_text(encoding="utf-8"))
    row = {k: m[k] for k in ("label", "size_mb", "rows", "meters", "parts", "anomaly_pct", "anomaly_counts",
                             "seconds", "injected_anomalies") if k in m}
    row["bytes"] = m.get("bytes", int(m["size_mb"] * 1024 * 1024))
    F["datasets"].append(row)
F["weather_rows"] = int(pd.read_csv(config.REFERENCE_DIR / "weather_hourly_2025.csv").shape[0])

# ---- sample raw rows: one Normal + one of every anomaly type (main dataset, raw zone)
raw = pd.read_csv(sorted((config.RAW_DIR / f"readings_{MAIN}").glob("part-*.csv"))[0], nrows=400000).dropna()
samp = []
for i, a in enumerate(D.ANOMALY_TYPES):
    r = raw[raw.anomaly_type == a].sample(1, random_state=i + 5).iloc[0]
    samp.append({k: (v.item() if hasattr(v, "item") else v) for k, v in r.to_dict().items()})
F["sample_rows"] = samp

# ---- Spark runs, quality, batch analytics
runs = q("SELECT id, job_name, engine, dataset, status, seconds, input_rows, output_rows, input_bytes, details "
         "FROM tbl_pipeline_runs WHERE status='success' ORDER BY id")
for r in runs:
    r["details"] = jl(r["details"]) or {}
F["runs"] = runs
F["quality"] = {ds: q("SELECT check_name, column_name, n_rows, pct FROM tbl_data_quality WHERE dataset=:d AND n_rows > 0 "
                      "ORDER BY check_name, column_name", d=ds) for ds in SIZES}
st = pd.DataFrame(q("SELECT dimension, dim_value, anomaly_type, n_readings FROM tbl_batch_anomaly_stats WHERE dataset=:d", d=MAIN))
mu = st[st.dimension == "municipality"].pivot_table(index="dim_value", columns="anomaly_type", values="n_readings", fill_value=0)
mu["total"] = mu.sum(axis=1)
mu["pct_anom"] = (1 - mu["Normal"] / mu["total"]) * 100
F["muni"] = mu.sort_values("pct_anom", ascending=False).reset_index().rename(columns={"dim_value": "municipality"}).round(3).to_dict("records")
hr = st[(st.dimension == "hour") & (st.anomaly_type != "Normal")]
F["hour_peak"] = {a: int(g.sort_values("n_readings").iloc[-1]["dim_value"]) for a, g in hr.groupby("anomaly_type")}
F["profile"] = q("SELECT anomaly_type, feature, mean, p05, p50, p95 FROM tbl_batch_feature_profile WHERE dataset=:d", d=MAIN)
F["top_meters"] = q("SELECT meter_id, municipality, customer_type, anomaly_type, n_hours, lost_kwh_est "
                    "FROM tbl_batch_top_meters WHERE dataset=:d ORDER BY n_hours DESC LIMIT 8", d=MAIN)

# ---- models
models = q("SELECT * FROM tbl_models ORDER BY id")
for m in models:
    for k in ("params", "feature_importance", "confusion_matrix", "class_report"):
        m[k] = jl(m[k])
F["models"] = models
cmp_file = ROOT / "docs" / "results" / f"compare_algorithms_{MAIN}.json"
F["compare"] = json.loads(cmp_file.read_text(encoding="utf-8"))

# ---- benchmarks (a Pandas run that hit the memory guard is stored as "FAILED: out of memory (...)")
F["benchmarks"] = q("""SELECT dataset_label vol, engine, AVG(seconds) avg_s, MIN(seconds) min_s, MAX(seconds) max_s,
                       AVG(peak_memory_mb) mem_mb, MAX(n_rows) n_rows, MAX(size_mb) size_mb, COUNT(*) runs,
                       SUM(result_check LIKE 'FAILED%%') failed, MAX(result_check) result_check
                       FROM tbl_benchmarks GROUP BY dataset_label, engine""")
for b in F["benchmarks"]:
    b["failed"] = int(b["failed"] or 0)

# ---- speed layer
F["stream"] = q("""SELECT COUNT(*) batches, SUM(n_messages) msgs, SUM(n_invalid) invalid, SUM(n_anomalies) anomalies,
                   AVG(processing_ms) proc_ms, AVG(avg_latency_ms) lat_ms, MAX(max_latency_ms) max_lat_ms,
                   AVG(n_messages) per_batch, MIN(started_at) first, MAX(started_at) last
                   FROM tbl_stream_batches WHERE n_messages > 0""")[0]
live = pd.DataFrame(q("SELECT predicted_type, true_type, is_anomaly, anomaly_score FROM tbl_readings_live "
                      "WHERE true_type IS NOT NULL"))
from sklearn.metrics import (average_precision_score, confusion_matrix, f1_score, precision_score,  # noqa: E402
                             recall_score, roc_auc_score)
yt, yp = live.true_type.to_numpy(), live.predicted_type.to_numpy()
tb, pb = yt != D.NORMAL, yp != D.NORMAL
F["live"] = {
    "n": int(len(live)), "meters": int(q("SELECT COUNT(DISTINCT meter_id) n FROM tbl_readings_live")[0]["n"]),
    "true_rate": float(tb.mean()), "accuracy": float((yt == yp).mean()),
    "rf": {"precision": float(precision_score(tb, pb)), "recall": float(recall_score(tb, pb)), "f1": float(f1_score(tb, pb))},
    "roc_auc": float(roc_auc_score(tb, live.anomaly_score)), "pr_auc": float(average_precision_score(tb, live.anomaly_score)),
    "per_type_recall": {a: {"rf": float((yp[yt == a] == a).mean()), "n": int((yt == a).sum())}
                        for a in D.ANOMALIES if (yt == a).any()},
    "confusion": confusion_matrix(yt, yp, labels=D.ANOMALY_TYPES).tolist(),
}
del live
F["events"] = q("SELECT anomaly_type, COUNT(*) n, SUM(true_type = anomaly_type) correct FROM tbl_anomaly_events GROUP BY anomaly_type")
F["tables"] = q("SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema='powerpredict_db' ORDER BY table_name")
F["users"] = q("SELECT role, COUNT(*) n FROM tbl_users WHERE is_active = 1 GROUP BY role")
try:
    from pipeline.spark_streaming import OffsetReader
    r = OffsetReader(config.KAFKA_TOPIC)
    ends = r.end_offsets(retries=1)
    r.close()
    F["kafka"] = {"topic": config.KAFKA_TOPIC, "partitions": len(ends), "total_messages": int(sum(ends.values()))}
except Exception as e:  # noqa: BLE001
    # Kafka stopped: keep the count measured by the last run (it only grows while the system runs)
    old = json.loads(OUT.read_text(encoding="utf-8")).get("kafka", {}) if OUT.exists() else {}
    F["kafka"] = old if old.get("total_messages") else {"error": str(e)}

# ---- outliers (manual 14.C): IQR rule on the natural test sample; are the outliers real anomalies?
tsn = read_parquet_folder(config.CURATED_DIR / f"test_sample_{MAIN}")
is_anom = (tsn.anomaly_type != D.NORMAL).to_numpy()
any_out = np.zeros(len(tsn), bool)
orows = []
for c in ["voltage_v", "current_a", "power_kw", "power_factor", "frequency_hz", "temperature_c"]:
    x = tsn[c]
    q1, q3 = x.quantile([0.25, 0.75])
    lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    m = ((x < lo) | (x > hi)).to_numpy()
    any_out |= m
    orows.append({"feature": c, "low": float(lo), "high": float(hi), "n": int(m.sum()), "pct": float(m.mean() * 100),
                  "anom_share": float(is_anom[m].mean() * 100) if m.any() else 0.0})
F["outliers"] = {"rows": int(len(tsn)), "features": orows, "any_n": int(any_out.sum()), "any_pct": float(any_out.mean() * 100),
                 "any_anom_share": float(is_anom[any_out].mean() * 100),
                 "anom_caught": float(any_out[is_anom].mean() * 100),
                 "per_type_caught": {a: float(any_out[(tsn.anomaly_type == a).to_numpy()].mean() * 100) for a in D.ANOMALIES}}
del tsn

# ---- CPU / memory usage measured by measure_cpu.py (manual 14.B)
cpu_file = Path(__file__).resolve().parent / "cpu_usage.json"
F["cpu"] = json.loads(cpu_file.read_text(encoding="utf-8")) if cpu_file.exists() else []

# ---- manual Entropy / Gain / Gini example on 12 real training rows (3 classes)
ts = read_parquet_folder(config.CURATED_DIR / f"train_sample_{MAIN}")
CL = ["Normal", "Voltage Sag", "Energy Theft"]
pick = [ts[ts.anomaly_type == "Normal"].sample(5, random_state=21),
        ts[ts.anomaly_type == "Voltage Sag"].sample(4, random_state=21),
        ts[ts.anomaly_type == "Energy Theft"].sample(3, random_state=21)]
ex = pd.concat(pick).sample(frac=1, random_state=4).reset_index(drop=True)
del ts
ex["Voltage"] = np.where(ex.voltage_dev_pct < -6, "Low", "Normal")
ex["Consumption"] = np.where(ex.baseline_ratio < 0.5, "Low", "Normal")
ex["Period"] = np.where(ex.hour.between(18, 22), "Peak", "Off-peak")


def entropy(labels):
    n = len(labels)
    return -sum(((labels == c).sum() / n) * math.log2((labels == c).sum() / n) for c in CL if (labels == c).sum()) if n else 0.0


def gini(labels):
    n = len(labels)
    return 1 - sum(((labels == c).sum() / n) ** 2 for c in CL) if n else 0.0


tot_e, tot_g = entropy(ex.anomaly_type), gini(ex.anomaly_type)
attrs = {}
for a in ["Voltage", "Consumption", "Period"]:
    parts, w_e, w_g = [], 0.0, 0.0
    for v, g in ex.groupby(a):
        e, gg = entropy(g.anomaly_type), gini(g.anomaly_type)
        parts.append({"value": v, "n": len(g), **{c: int((g.anomaly_type == c).sum()) for c in CL}, "entropy": e, "gini": gg})
        w_e += len(g) / len(ex) * e
        w_g += len(g) / len(ex) * gg
    attrs[a] = {"parts": parts, "gain": tot_e - w_e, "gini_gain": tot_g - w_g}
F["manual"] = {"classes": CL,
               "rows": ex[["meter_id", "municipality", "voltage_v", "voltage_dev_pct", "baseline_ratio", "hour",
                           "Voltage", "Consumption", "Period", "anomaly_type"]].round(3).to_dict("records"),
               "counts": {c: int((ex.anomaly_type == c).sum()) for c in CL},
               "total_entropy": tot_e, "total_gini": tot_g, "attributes": attrs}

# ---- one real Random Forest vote example (detect page preset: energy theft, residential meter at 20:00)
from ml.predict import predict_one  # noqa: E402
F["vote_example"] = predict_one({"customer_type": "residential", "municipality": "Dili", "contracted_kva": 3.5,
                                 "voltage_v": 221.0, "power_kw": 0.08, "power_factor": 0.95, "frequency_hz": 50.0,
                                 "temperature_c": 29.0, "timestamp": "2026-11-06 20:00:00"})

# ---- website checks recorded by shoot.py (login / roles), if they exist
chk = Path(__file__).resolve().parent / "web_checks.json"
F["web_checks"] = json.loads(chk.read_text(encoding="utf-8")) if chk.exists() else []

F["generated_at"] = datetime.now().isoformat(timespec="seconds")
OUT.write_text(json.dumps(F, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
print("facts ->", OUT)
print("datasets:", [(x["label"], x["rows"], x["meters"]) for x in F["datasets"]])
print("manual:", {a: (round(v["gain"], 4), round(v["gini_gain"], 4)) for a, v in attrs.items()}, "H=", round(tot_e, 4))
print("stream:", {k: F["stream"][k] for k in ("batches", "msgs", "invalid", "anomalies", "lat_ms")})
print("live:", {k: F["live"][k] for k in ("n", "accuracy", "rf", "roc_auc", "pr_auc")})
print("kafka:", F["kafka"], "| events:", F["events"], "| users:", F["users"])
print("vote:", F["vote_example"]["predicted_type"], F["vote_example"]["votes"])
print("bench:", [(b["vol"], b["engine"], round(b["avg_s"], 1), b["failed"]) for b in F["benchmarks"]])
