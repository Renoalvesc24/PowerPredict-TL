"""Write the report diagrams (HTML, rendered to PNG by ../shoot.py diagrams). Numbers come from ../facts.json."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
F = json.loads((HERE.parent / "facts.json").read_text(encoding="utf-8"))
DS = {d["label"]: d for d in F["datasets"]}
MAIN = F["main"]
ETL = {r["dataset"]: r for r in F["runs"] if r["job_name"] == "spark_batch_etl"}
det = ETL[MAIN]["details"]
rf = next(m for m in F["models"] if m["engine"] == "scikit-learn" and m["is_active"])
ml = [m for m in F["models"] if m["engine"] == "spark-mllib"][-1]
P = rf["params"]
n_tables = len(F["tables"])
total_rows = sum(d["rows"] for d in F["datasets"])
total_gb = sum(d["size_mb"] for d in F["datasets"]) / 1024
sizes_txt = " · ".join(s.replace("GB", " GB") for s in F["sizes"])
part_mb = DS[MAIN]["size_mb"] / DS[MAIN]["parts"]


def fmt(v):
    return f"{v:,.0f}"


HEAD = '<!doctype html><html><head><meta charset="utf-8"><link rel="stylesheet" href="style.css">{css}</head><body>'
TAIL = "</body></html>"
TYPES = ('<span class="pill" style="background:#e8f0fb;color:#2a5fb0">Voltajen Tun</span>'
         '<span class="pill" style="background:#fdeee6;color:#b5491d">Voltajen Sa\'e Liu</span>'
         '<span class="pill" style="background:#e3f6ef;color:#12805a">Frekuénsia La Estável</span>'
         '<span class="pill" style="background:#fdf3dc;color:#9a6a00">Karga Liu</span>'
         '<span class="pill" style="background:#fbe9f0;color:#b03a6a">Na\'ok Eletrisidade</span>'
         '<span class="pill" style="background:#e3f2e3;color:#106b10">Kontador Aat</span>')

D = {}

D["d1_block_diagram"] = f"""
<div class="canvas" style="width:1340px">
  <div class="title">Block Diagram – PowerPredict TL (Deteksaun Anomalia)</div>
  <div class="sub">Husi readings smart meter to'o rezultadu deteksaun anomalia uza algoritmu Random Forest</div>
  <div class="row">
    <div class="box" style="width:200px"><h4>a) Input (smart meter)</h4><ul><li>Voltage (V), Current (A)</li><li>Power (kW), Power Factor</li><li>Frequency (Hz)</li><li>Temperature (°C)</li><li>Meter master: kVA, customer type, baseline</li></ul></div>
    <div class="arrow">→</div>
    <div class="box amber" style="width:205px"><h4>b) Data Processing (Spark)</h4><ul><li>Data Cleaning (missing, duplicate, invalid)</li><li>Feature Engineering: load ratio, baseline ratio, consistency ratio</li><li>Ground truth label husi events</li></ul></div>
    <div class="arrow">→</div>
    <div class="box purple" style="width:205px"><h4>c) Model Training</h4><ul><li>Bootstrap (bagging)</li><li>Random feature selection (√16 ≈ 4)</li><li>Gini Impurity ba split</li><li>Decision tree {P['n_estimators']}</li><li>class_weight Normal = {P.get('normal_weight', 1):g}</li></ul></div>
    <div class="arrow">→</div>
    <div class="box" style="width:185px"><h4>d) Random Forest Model</h4><p>Model (joblib) + Feature Importance, rai iha <b>tbl_models</b>; Spark MLlib Random Forest ba Big Data</p></div>
    <div class="arrow">→</div>
    <div class="box gray" style="width:175px"><h4>e) Testing</h4><p>Test set husi meters separadu (1 husi 5) + live stream husi Kafka</p></div>
    <div class="arrow">→</div>
    <div class="box green" style="width:215px"><h4>f) Output Deteksaun</h4><div class="out" style="flex-direction:column;gap:5px"><span class="pill n">Normal</span>{TYPES}</div><p style="margin-top:6px">+ Anomaly Score = 1 − P(Normal)</p></div>
  </div>
</div>"""

D["d2_architecture"] = f"""
<div class="canvas" style="width:1340px">
  <div class="title">Arkitetura Sistema Big Data (Lambda Architecture)</div>
  <div class="sub">Data Collection → Data Ingestion → Data Storage → Data Processing → Machine Learning → Visualization → Real-Time Application</div>
  <div class="row">
    <div class="col" style="gap:12px;width:220px">
      <div class="lane"><span class="lane-title">1 · Data Source</span>
        <div class="box gray" style="margin-top:6px"><h4>bot/bot_live.py</h4><p>Smart meter 281, reading kada 2 s, anomalia + ground truth</p></div>
        <div class="box gray" style="margin-top:8px"><h4>Open-Meteo API</h4><p>Temperature live (kapitál 14) + arkivu 2025</p></div>
        <div class="box gray" style="margin-top:8px"><h4>UCI dataset (reál)</h4><p>Seed konsumu → Dataset {sizes_txt}</p></div>
      </div>
    </div>
    <div class="arrow">→</div>
    <div class="col" style="gap:14px;width:850px">
      <div class="lane"><span class="lane-title">Speed layer (real-time)</span>
        <div class="row" style="margin-top:6px">
          <div class="box dark" style="width:150px"><h4>2 · FastAPI</h4><p>Gateway: POST /api/readings/batch, validasaun structure</p></div>
          <div class="arrow">→</div>
          <div class="box amber" style="width:165px"><h4>2 · Apache Kafka</h4><p>Topic <b>smart-meter-readings</b>, partition 3, key = meter_id</p></div>
          <div class="arrow">→</div>
          <div class="box" style="width:190px"><h4>3 · Spark Streaming</h4><p>Micro-batch 3 s: parse → validation → feature → window 1 min + watermark 2 min</p></div>
          <div class="arrow">→</div>
          <div class="box purple" style="width:170px"><h4>5 · Random Forest</h4><p>Tipu anomalia + Anomaly Score = 1 − P(Normal)</p></div>
        </div>
      </div>
      <div class="lane"><span class="lane-title">Batch layer (big data)</span>
        <div class="row" style="margin-top:6px">
          <div class="box green" style="width:180px"><h4>4 · Data Lake (D:)</h4><p>Raw zone (CSV {total_gb:.1f} GB) · Curated zone (Parquet) · Stream zone</p></div>
          <div class="arrow">→</div>
          <div class="box" style="width:210px"><h4>3 · Spark Batch ETL</h4><p>Data Quality, Cleaning, Feature Engineering, broadcast join, anomaly analytics, train/test split</p></div>
          <div class="arrow">→</div>
          <div class="box purple" style="width:245px"><h4>5 · Random Forest training</h4><p>scikit-learn (training sample {fmt(rf['n_train'])}) + Spark MLlib (readings {ml['n_train'] / 1e6:.1f} millaun) → model ativu</p></div>
        </div>
      </div>
    </div>
    <div class="arrow">→</div>
    <div class="col" style="gap:12px;width:200px">
      <div class="box green"><h4>4 · MySQL (XAMPP)</h4><p>Database <b>powerpredict_db</b>: table {n_tables} (readings, events, models, users, Spark results)</p></div>
      <div class="arrow down">↓</div>
      <div class="box dark"><h4>6 · Streamlit Dashboard</h4><p>Login Admin / User, pájina 9, Refresh, Night mode, alerta, simulasaun</p></div>
    </div>
  </div>
</div>"""

D["d3_data_scaling"] = f"""
<div class="canvas" style="width:1340px">
  <div class="title">Métodu Kria Dataset Big Data ho Anomalia ({sizes_txt})</div>
  <div class="sub">Hahú husi dadus reál (seed) → modelu fíziku rede → hatama events anomalia (ground truth) → eskala to'o tamañu alvu</div>
  <div class="row">
    <div class="col" style="gap:10px;width:225px">
      <div class="box gray"><h4>Seed 1 · UCI (reál)</h4><p>ElectricityLoadDiagrams: padraun fulan-fulan kliente 264</p></div>
      <div class="box gray"><h4>Seed 2 · Open-Meteo (reál)</h4><p>Temperature oras-oras 2025, kapitál municipality 14 ({fmt(F['weather_rows'])} valór)</p></div>
    </div>
    <div class="arrow">→</div>
    <div class="box" style="width:215px"><h4>Meter master</h4><ul><li>Meters distribui tuir populasaun</li><li>Customer type 78/15/5/2 %</li><li>kVA, power factor, distance</li></ul></div>
    <div class="arrow">→</div>
    <div class="box" style="width:215px"><h4>Modelu fíziku rede</h4><ul><li>Load shape × weekend × temperature × fulan</li><li>Voltage drop iha feeder</li><li>Frequency 50 Hz (AR)</li></ul></div>
    <div class="arrow">→</div>
    <div class="box red" style="width:250px"><h4>Hatama events anomalia</h4><div class="out" style="flex-wrap:wrap;gap:4px">{TYPES}</div><p style="margin-top:6px">Label = event ne'ebé ativu (ground truth), la'ós threshold</p></div>
    <div class="arrow">→</div>
    <div class="col" style="gap:8px;width:200px">
      <div class="box amber"><h4>Measured values</h4><p>Valór loos + sensor noise; missing ~0.3 %, duplicate ~0.2 %, invalid ~0.1 %</p></div>
      <div class="box green"><h4>CSV part-file ~{part_mb:.0f} MB</h4><p>{sizes_txt} ({total_rows / 1e6:.1f} M records, {total_gb:.1f} GB)</p></div>
    </div>
  </div>
</div>"""

D["d4_random_forest"] = f"""
<div class="canvas" style="width:1340px">
  <div class="title">Lala'ok Algoritmu Random Forest ba Deteksaun Anomalia</div>
  <div class="sub">Bagging + random feature selection + Gini Impurity + majority voting → tipu anomalia no Anomaly Score</div>
  <div class="row">
    <div class="box" style="width:210px"><h4>Training set ({MAIN})</h4><p>{fmt(rf['n_train'])} readings husi meters treinu: Normal 600k (undersampled) + anomalia hotu (to'o 80k / tipu); class_weight Normal = {P.get('normal_weight', 1):g}</p></div>
    <div class="arrow">→</div>
    <div class="col" style="gap:8px;width:230px">
      <div class="box amber"><h4>Bootstrap sample 1</h4><p>Tree 1: kada split haree feature aleatóriu 4</p></div>
      <div class="box amber"><h4>Bootstrap sample 2</h4><p>Tree 2: kada split haree feature aleatóriu 4</p></div>
      <div class="box amber"><h4>… sample {P['n_estimators']}</h4><p>Tree {P['n_estimators']} (max_depth {P['max_depth']}, Gini)</p></div>
    </div>
    <div class="arrow">→</div>
    <div class="box purple" style="width:230px"><h4>Majority voting</h4><p>Kada tree vota tipu ida. P(tipu) = média probabilidade tree sira</p><p style="margin-top:6px"><b>Ezemplu:</b> reading 0.08 kW iha 20:00 → tree {P['n_estimators']} hotu vota Na'ok Eletrisidade</p></div>
    <div class="arrow">→</div>
    <div class="box green" style="width:260px"><h4>Output</h4><ul><li>Tipu = argmax P(tipu)</li><li>Anomaly Score = 1 − P(Normal)</li><li>Event se Score ≥ threshold 0.6</li><li>Feature Importance (Gini)</li></ul></div>
  </div>
</div>"""

D["d5_streaming"] = """
<div class="canvas" style="width:1340px">
  <div class="title">Fluxu Real-Time: Bot → FastAPI → Kafka → Spark Streaming → Random Forest → MySQL → Streamlit</div>
  <div class="sub">Kada 2 s bot haruka readings 281; kada 3 s Spark prosesa offset foun hotu (micro-batch, at-least-once)</div>
  <div class="row">
    <div class="box gray" style="width:150px"><h4>Bot</h4><p>Meters 281, JSON, true_type (evaluation only)</p></div>
    <div class="arrow">→</div>
    <div class="box dark" style="width:150px"><h4>FastAPI</h4><p>Pydantic validation, publish ba Kafka</p></div>
    <div class="arrow">→</div>
    <div class="box amber" style="width:150px"><h4>Kafka</h4><p>Partition 0 · 1 · 2, offset kada mensajen</p></div>
    <div class="arrow">→</div>
    <div class="box" style="width:250px"><h4>Spark micro-batch</h4><ol style="margin:0;padding-left:16px;font-size:12.5px;color:#33425a"><li>read offsets → from_json</li><li>validation + dropDuplicates</li><li>broadcast join meter master</li><li>feature engineering</li><li>window 1 min, watermark 2 min</li></ol></div>
    <div class="arrow">→</div>
    <div class="box purple" style="width:150px"><h4>Random Forest</h4><p>Tipu anomalia + Anomaly Score; event se Score ≥ 0.6</p></div>
    <div class="arrow">→</div>
    <div class="box green" style="width:185px"><h4>MySQL</h4><p>tbl_readings_live, tbl_stream_agg, tbl_anomaly_events, tbl_stream_batches, offsets</p></div>
  </div>
</div>"""


def _cls(r):
    return ' class="k"' if r.endswith("PK") else (' class="f"' if "FK*" in r else "")


def T(name, rows, cls=""):
    cells = "".join("<tr><td" + _cls(r) + ">" + r + "</td></tr>" for r in rows)
    return f'<div class="erd-wrap {cls}"><table class="erd"><tr><th>{name}</th></tr>' + cells + "</table></div>"


D["d6_erd"] = (f'<div class="canvas" style="width:1340px"><div class="title">Relasaun Entre Table – Database powerpredict_db (MySQL)</div>'
               f'<div class="sub">Table {n_tables}. <span style="color:#a11d1d;font-weight:700">PK</span> = primary key · '
               '<span style="color:#7b5cf0;font-weight:700">FK*</span> = relasaun lójiku</div>'
               '<div style="display:grid;grid-template-columns:repeat(5,auto);gap:24px 26px;align-items:start">'
               + T("tbl_municipalities", ["id PK", "name (unique)", "capital · latitude · longitude", "grid · voltage_drop"])
               + T("tbl_meters", ["id PK", "meter_id (unique)", "municipality FK*", "customer_type · contracted_kva", "peak_util · pf_base (baseline)", "is_live"])
               + T("tbl_readings_live", ["id PK", "meter_id FK*", "ts · voltage_v · current_a · power_kw", "load_ratio · baseline_ratio · consistency_ratio",
                                         "predicted_type · anomaly_score", "confidence · true_type", "batch_id FK*"], "amber")
               + T("tbl_anomaly_events", ["id PK", "meter_id FK*", "anomaly_type · severity", "anomaly_score · reason", "acknowledged"], "amber")
               + T("tbl_stream_agg", ["id PK", "window_start · municipality FK*", "n_readings · n_anomalies", "n_sag … n_fault"], "amber")
               + T("tbl_models", ["id PK", "algorithm · engine · dataset", "precision · recall · anomaly_f1", "pr_auc · roc_auc", "feature_importance (JSON)", "file_path · is_active"], "purple")
               + T("tbl_manual_checks", ["id PK", "model_id FK*", "meter_id FK*", "inputs · predicted_type", "probabilities · votes"], "purple")
               + T("tbl_stream_batches / offsets", ["id PK", "batch_id · n_messages", "processing_ms · latency_ms", "topic · partition_no · next_offset"], "amber")
               + T("tbl_heartbeats / settings", ["component PK · last_seen", "key PK · value (scenario, threshold)"], "gray")
               + T("tbl_users", ["id PK", "username (unique)", "full_name · role (admin / user)", "password_hash (PBKDF2)", "is_active · last_login"], "red")
               + T("tbl_user_sessions", ["id PK", "user_id FK* → tbl_users", "token_hash (SHA-256, unique)", "expires_at (8 oras)"], "red")
               + T("tbl_pipeline_runs", ["id PK", "job_name · engine · dataset", "input_rows · output_rows · seconds", "details (timings JSON)"], "green")
               + T("tbl_data_quality", ["id PK", "run_id FK*", "check_name · column_name", "n_rows · pct"], "green")
               + T("tbl_batch_anomaly_stats", ["id PK", "dataset · dimension · dim_value", "anomaly_type · n_readings"], "green")
               + T("tbl_batch_feature_profile", ["id PK", "dataset · anomaly_type · feature", "mean · p05 · p50 · p95"], "green")
               + T("tbl_batch_top_meters", ["id PK", "meter_id FK*", "anomaly_type · n_hours", "lost_kwh_est"], "green")
               + T("tbl_benchmarks", ["id PK", "engine · dataset_label", "seconds · peak_memory_mb", "result_check (FAILED: out of memory)"], "green")
               + '</div></div>')

D["d7_pages"] = """
<div class="canvas" style="width:1340px">
  <div class="title">Estrutura Website (Streamlit) – Login + Pájina 9</div>
  <div class="sub">Browser ida de'it: http://localhost:8505. Login ho tab User ka tab Admin; menu grupu tolu ba User, grupu haat ba Admin. Kada pájina iha butaun Refresh; Night mode / Day mode iha sidebar.</div>
  <div class="row" style="margin-bottom:16px">
    <div class="box dark" style="width:100%"><h4>Login (tbl_users + tbl_user_sessions)</h4><p>Tab User / tab Admin · password hash PBKDF2-SHA256 · cookie sesaun 8 oras · role tenke tuir tab · Logout hamoos sesaun</p></div>
  </div>
  <div style="display:grid;grid-template-columns:1.25fr 1.25fr 1fr 0.8fr;gap:16px">
    <div class="lane"><span class="lane-title">Monitorizasaun (User + Admin)</span>
      <div class="col" style="gap:8px;margin-top:6px">
        <div class="box"><h4>Dashboard</h4><p>Alerta → Análitika Deskritiva → Tendénsia → Estatístika → Anomaly Score → Klasifikasaun → Real-Time (refresh 10 s)</p></div>
        <div class="box red"><h4>Monitor Real-Time</h4><p>Mapa meters, estadu kada municipality, feed anomalia, Voltage no Frequency (refresh 5 s)</p></div>
        <div class="box amber"><h4>Anomaly Events</h4><p>Filter, download CSV, lista suspeita Na'ok Eletrisidade; konfirma event: Admin de'it</p></div>
      </div></div>
    <div class="lane"><span class="lane-title">Análize (User + Admin)</span>
      <div class="col" style="gap:8px;margin-top:6px">
        <div class="box purple"><h4>Deteksaun Manual</h4><p>Hatama reading ida → tipu anomalia, votu tree 150, evidénsia</p></div>
        <div class="box purple"><h4>Model Random Forest</h4><p>Rezultadu test, tanba sa Random Forest, Confusion Matrix, threshold; retrain / ativa: Admin de'it</p></div>
        <div class="box green"><h4>Big Data Pipeline</h4><p>Dataset 5 / 10 / 20 GB, Spark ETL, Data Quality, anomalia batch, Pandas vs Spark, streaming</p></div>
      </div></div>
    <div class="lane"><span class="lane-title">Admin de'it</span>
      <div class="col" style="gap:8px;margin-top:6px">
        <div class="box gray"><h4>Simulasaun</h4><p>Hamosu Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu, Na'ok, Kontador Aat; threshold</p></div>
        <div class="box gray"><h4>Jestaun Uzuáriu</h4><p>Kria konta, muda role, ativa / dezativa, reset password, hamoos konta</p></div>
      </div></div>
    <div class="lane"><span class="lane-title">Informasaun</span>
      <div class="col" style="gap:8px;margin-top:6px">
        <div class="box gray"><h4>Metodolojia</h4><p>Data source, arkitetura, Random Forest (Gini, voting), feature, teknolojia</p></div>
      </div></div>
  </div>
</div>"""

clean = DS[MAIN]
D["d8_split"] = f"""
<div class="canvas" style="width:1340px">
  <div class="title">Dezeñu Evaluasaun: Fahe Dadus Tuir Meter (Meter-Level Split)</div>
  <div class="sub">Episódiu Na'ok Eletrisidade ka Kontador Aat naruk (loron to'o semana) la bele iha training no test hotu</div>
  <div class="row">
    <div class="box" style="width:230px"><h4>Dataset {MAIN.replace('GB', ' GB')} (clean)</h4><p>{fmt(det['curated_rows'])} readings · meters {fmt(clean['meters'])} · anomalia {100 - clean['anomaly_pct']['Normal']:.2f} %</p></div>
    <div class="arrow">→</div>
    <div class="col" style="gap:12px;width:430px">
      <div class="box purple"><h4>Meters training (4 husi 5) → Training set</h4><p>Normal undersampled ba 600,000 + anomalia hotu (to'o 80,000 / tipu) = {fmt(det['train_sample_rows'])} readings. Cross-validation 3-fold StratifiedGroupKFold (group = meter)</p></div>
      <div class="box green"><h4>Meters test (1 husi 5, crc32(meter_id) mod 5 = 0) → Test set</h4><p>Distribuisaun natural: {fmt(det['test_sample_rows'])} readings (sample) — Spark MLlib test iha readings {fmt(det['test_meter_rows'])} hotu</p></div>
    </div>
    <div class="arrow">→</div>
    <div class="box amber" style="width:300px"><h4>Métrika</h4><ul><li>Precision, Recall, F1-Score (anomalia vs normal)</li><li>PR-AUC, ROC-AUC, False Positive Rate</li><li>Macro F1 + Confusion Matrix (tipu 7)</li><li>Live: prediction vs ground truth bot</li></ul></div>
  </div>
</div>"""

# ---------------------------------------------------------------- Data Flow Diagram (level 1), drawn as SVG
def _box(x, y, w, h, kind, title, sub=""):
    """kind: ent (external entity), proc (process), store (data store)."""
    fill, stroke, tc = {"ent": ("#f5f7fa", "#64748b", "#334155"), "proc": ("#f3f6ff", "#2459e0", "#12307e"),
                        "store": ("#ecf8f1", "#15924a", "#0d6a35")}[kind]
    cx = x + w / 2
    if kind == "proc":
        shape = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="22" fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
    elif kind == "store":   # open-ended rectangle with an id cell, the usual data-store symbol
        shape = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" stroke="none"/>'
                 f'<path d="M{x + w} {y} H{x} V{y + h} H{x + w}" fill="none" stroke="{stroke}" stroke-width="2"/>'
                 f'<line x1="{x + 34}" y1="{y}" x2="{x + 34}" y2="{y + h}" stroke="{stroke}" stroke-width="1.5"/>')
        cx = x + 34 + (w - 34) / 2
    else:
        shape = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
    ty = y + h / 2 - (7 if sub else -5)
    text = f'<text x="{cx}" y="{ty}" text-anchor="middle" font-size="14.5" font-weight="700" fill="{tc}">{title}</text>'
    if sub:
        text += f'<text x="{cx}" y="{ty + 19}" text-anchor="middle" font-size="12.5" fill="#33425a">{sub}</text>'
    return shape + text


def _store_id(x, y, h, sid):
    return f'<text x="{x + 17}" y="{y + h / 2 + 5}" text-anchor="middle" font-size="13" font-weight="700" fill="#0d6a35">{sid}</text>'


def _flow(points, label="", lx=None, ly=None, anchor="middle"):
    d = "M" + " L".join(f"{px} {py}" for px, py in points)
    out = f'<path d="{d}" fill="none" stroke="#6b7a90" stroke-width="1.8" marker-end="url(#arr)"/>'
    if label:
        out += (f'<text x="{lx}" y="{ly}" text-anchor="{anchor}" font-size="12" fill="#5b3fd0" '
                f'paint-order="stroke" stroke="#fff" stroke-width="4">{label}</text>')
    return out


svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1290" height="760" font-family="Segoe UI, Arial, sans-serif">',
       '<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
       '<path d="M0 0 L10 5 L0 10 z" fill="#6b7a90"/></marker></defs>']
# shapes
svg += [_box(10, 70, 170, 64, "ent", "Smart meter bot", "meters 281, kada 2 s"),
        _box(240, 66, 190, 72, "proc", "1.0 Ingest", "FastAPI + Pydantic"),
        _box(490, 76, 210, 52, "store", "Kafka topic", "smart-meter-readings"), _store_id(490, 76, 52, "D1"),
        _box(760, 66, 220, 72, "proc", "2.0 Stream + RF", "Spark Streaming 3 s"),
        _box(1040, 240, 240, 60, "store", "MySQL", "powerpredict_db, table 19"), _store_id(1040, 240, 60, "D3"),
        _box(10, 260, 170, 56, "ent", "UCI dataset", "seed konsumu (reál)"),
        _box(10, 350, 170, 56, "ent", "Open-Meteo API", "temperature"),
        _box(240, 290, 190, 72, "proc", "0.0 Jera dataset", "5 / 10 / 20 GB"),
        _box(490, 300, 210, 52, "store", "Data Lake (D:)", "raw · curated · stream"), _store_id(490, 300, 52, "D2"),
        _box(760, 290, 220, 72, "proc", "3.0 Spark batch ETL", "quality, feature, analytics"),
        _box(490, 470, 210, 72, "proc", "4.0 Treina Random Forest", "scikit-learn + MLlib"),
        _box(760, 480, 220, 52, "store", "Model file", "joblib (model ativu)"), _store_id(760, 480, 52, "D4"),
        _box(1040, 470, 240, 72, "proc", "5.0 Website", "Streamlit + login"),
        _box(1010, 650, 125, 56, "ent", "Admin"), _box(1155, 650, 125, 56, "ent", "User")]
# flows
svg += [_flow([(180, 102), (240, 102)], "readings", 210, 92),
        _flow([(430, 102), (490, 102)], "publish", 460, 92),
        _flow([(700, 102), (760, 102)], "offset", 730, 92),
        _flow([(980, 102), (1160, 102), (1160, 240)], "readings + tipu, events", 1068, 92),
        _flow([(830, 138), (830, 205), (650, 205), (650, 300)], "stream Parquet", 740, 198),
        _flow([(180, 288), (240, 312)], "konsumu", 205, 285),
        _flow([(180, 378), (240, 344)], "temperature 2025", 200, 395),
        _flow([(430, 326), (490, 326)], "raw CSV", 460, 316),
        _flow([(700, 316), (760, 316)], "raw CSV", 730, 308),
        _flow([(760, 348), (700, 348)], "Parquet", 730, 370),
        _flow([(980, 316), (1040, 280)], "quality, analytics", 986, 350, "start"),
        _flow([(595, 352), (595, 470)], "train / test sample", 600, 420, "start"),
        _flow([(700, 506), (760, 506)], "model", 730, 498),
        _flow([(980, 506), (1005, 506), (1005, 175), (905, 175), (905, 138)], "model ativu", 958, 168),
        _flow([(1130, 300), (1130, 470)], "dadus dashboard", 1124, 400, "end"),
        _flow([(1220, 470), (1220, 300)], "login, settings", 1175, 430, "middle"),
        _flow([(1072, 650), (1100, 542)], "simulasaun, retrain, konta", 1040, 610, "middle"),
        _flow([(1218, 650), (1200, 542)], "monitorizasaun", 1230, 610, "middle"),
        _flow([(1270, 240), (1270, 36), (335, 36), (335, 66)], "senáriu simulasaun (bot lee liuhusi API)", 800, 30)]
svg.append("</svg>")
D["d9_dfd"] = ('<div class="canvas" style="width:1340px"><div class="title">Data Flow Diagram (Level 1) – PowerPredict TL</div>'
               '<div class="sub">Retángulu = entidade esterna · retángulu kantu kabuar = prosesu · retángulu loke = data store (D1–D4) · '
               'seta = fluxu dadus</div>' + "".join(svg) + "</div>")

for name, body in D.items():
    (HERE / f"{name}.html").write_text(HEAD.format(css="") + body + TAIL, encoding="utf-8")
print("written", list(D))
