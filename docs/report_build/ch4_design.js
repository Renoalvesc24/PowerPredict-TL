// Kapitulu IV - Dezeñu Sistema no Implementasaun (manual: System Architecture, Data Flow Diagram, Database Design,
// Processing Pipeline, Dashboard Design) + implementation of Random Forest, website and API
const L = require("./lib");
const { p, pn, h1, h2, h3, bullets, numbers, table, figure, fmt, AlignmentType } = L;
const R = AlignmentType.RIGHT, C = AlignmentType.CENTER, LEFT = AlignmentType.LEFT;

module.exports = function (F) {
  const rf = F.models.find((m) => m.engine === "scikit-learn" && m.is_active);
  const ml = F.models.filter((m) => m.engine === "spark-mllib").slice(-1)[0];
  const P = rf.params, MP = ml.params;
  const nT = F.tables.length;
  const ETL = Object.fromEntries(F.runs.filter((r) => r.job_name === "spark_batch_etl").map((r) => [r.dataset, r]));
  const out = [];

  out.push(h1("KAPITULU IV", "DEZEÑU SISTEMA NO IMPLEMENTASAUN"));

  // ---------------------------------------------------------------- 4.1 architecture
  out.push(h2("4.1. Arkitetura Sistema (System Architecture)"));
  out.push(p("Tuir manual seksaun 7, imajen tuir mai hatudu fluxu husi fonte dadus to'o dashboard real-time. Sistema uza arkitetura Lambda: *speed layer* ba readings real-time no *batch layer* ba dataset boot; MySQL mak *serving layer*. Estrutura folder tuir etapa pipeline (bot → api → pipeline → dashboard):"));
  out.push(...figure("d2_architecture.png", "Arkitetura sistema Big Data (Lambda Architecture)"));
  out.push(...table(["Etapa", "Implementasaun"], [
    ["Data Collection", "Bot smart meter (meters 281, Open-Meteo live) + jeradór dataset (seed UCI + Open-Meteo)"],
    ["Data Ingestion", "FastAPI gateway (`POST /api/readings/batch`) → Apache Kafka topic `smart-meter-readings`, partition 3, key = meter_id"],
    ["Data Storage", `Data Lake iha D: (raw CSV, curated Parquet, stream Parquet) + MySQL \`powerpredict_db\` (table ${nT})`],
    ["Data Processing", "Spark batch ETL (cleaning, feature engineering, broadcast join, aggregation) + Spark Streaming (micro-batch 3 s, window 1 min, watermark 2 min)"],
    ["Data Analytics / ML", "Descriptive, trend, statistical analysis + Random Forest (scikit-learn no Spark MLlib)"],
    ["Visualization", "Website Streamlit: login Admin / User, pájina 9, Refresh, Night mode (KPI, gráfiku, mapa, table)"],
    ["Real-Time Application", "Anomaly Score kada reading, anomaly events, lista suspeita Na'ok Eletrisidade, simulasaun"],
  ], [24, 76], { caption: "Etapa pipeline Big Data", firstColBold: true, size: 18 }));
  out.push(p("Block diagram tuir mai hatudu prosesu husi input readings to'o output deteksaun anomalia uza Random Forest:"));
  out.push(...figure("d1_block_diagram.png", "Block diagram Sistema Deteksaun Anomalia Rede Eletrisidade"));
  out.push(...bullets([
    "**a) Input:** reading smart meter (voltage, current, power, power factor, frequency), temperature no meter master (kVA, customer type, baseline).",
    "**b) Data Processing:** Spark hamoos dadus, kria feature foun (load ratio, baseline ratio, consistency ratio) no uza label ground truth husi events.",
    `**c) Model Training:** bootstrap (bagging), random feature selection, Gini Impurity, decision tree ${P.n_estimators}, class weight Normal = ${P.normal_weight}.`,
    "**d) Random Forest Model:** model rai iha file joblib no rejista iha `tbl_models` ho métrika no Feature Importance.",
    "**e) Testing:** test set husi meters separadu no readings real-time husi Kafka.",
    "**f) Output:** Normal ka tipu anomalia neen (Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu, Na'ok Eletrisidade, Kontador Aat) + Anomaly Score.",
  ]));

  // ---------------------------------------------------------------- 4.2 DFD
  out.push(h2("4.2. Data Flow Diagram"));
  out.push(p("Data Flow Diagram (level 1) hatudu entidade esterna (bot, UCI, Open-Meteo, Admin, User), prosesu (0.0–5.0), data store (D1 Kafka, D2 Data Lake, D3 MySQL, D4 file model) no dadus ne'ebé sulin entre sira:"));
  out.push(...figure("d9_dfd.png", "Data Flow Diagram (level 1) PowerPredict TL"));
  out.push(...table(["Prosesu", "Input", "Output"], [
    ["0.0 Jera dataset", "Seed UCI, temperature Open-Meteo", "CSV raw 5 / 10 / 20 GB (D2)"],
    ["1.0 Ingest", "Readings JSON husi bot; senáriu husi D3", "Mensajen Kafka (D1)"],
    ["2.0 Stream + Random Forest", "Offset foun husi D1; model ativu (D4)", "Readings + tipu anomalia, events, batch stats (D3); Parquet stream (D2)"],
    ["3.0 Spark batch ETL", "CSV raw (D2)", "Parquet curated, sample train / test (D2); Data Quality no analítika (D3)"],
    ["4.0 Treina Random Forest", "Sample train / test (D2)", "File model (D4); métrika model (D3)"],
    ["5.0 Website", "Dadus dashboard (D3); asaun Admin / User", "Login, settings, konfirma events (D3)"],
  ], [24, 36, 40], { caption: "Prosesu iha Data Flow Diagram", firstColBold: true, size: 18 }));

  // ---------------------------------------------------------------- 4.3 processing pipeline
  out.push(h2("4.3. Processing Pipeline"));
  out.push(h3("4.3.1. Batch Pipeline (Spark ETL)"));
  out.push(p(`Job \`pipeline/spark_batch_etl.py\` hala'o etapa neen: (1) read CSV (part-file hotu, schema explícitu); (2) Data Quality profile (missing, duplicate, invalid kada koluna); (3) cleaning + feature engineering + broadcast join ho meter master; (4) anomaly analytics (agregasaun tuir municipality, oras, fulan, feature profile, top meters); (5) sample training / test tuir meter; (6) write curated Parquet (partition tuir fulan). Ba dataset 20 GB, Spark uza \`local[${ETL["20GB"].details.cores}]\` no shuffle partitions = 2 × partition input (416) hodi Java heap la to'o limite; spill no ficheiru temporáriu rai iha D: (\`PP_SPARK_LOCAL_DIR\`).`));
  out.push(h3("4.3.2. Streaming Pipeline (Spark Streaming)"));
  out.push(...figure("d5_streaming.png", "Fluxu real-time Bot → FastAPI → Kafka → Spark → Random Forest → MySQL"));
  out.push(p(`Spark Streaming (\`pipeline/spark_streaming.py\`) hala'o kada segundu 3: (1) lee offset foun husi partition hotu; (2) parse JSON ho schema; (3) validation no dropDuplicates; (4) broadcast join ho meter master no feature engineering; (5) window 1 minutu kada municipality ho **watermark 2 minutu** (reading tarde liu la tama iha window count); (6) Random Forest klasifika kada reading; (7) rai rezultadu iha MySQL, kria anomaly event (Anomaly Score ≥ 0.6, máximu 1 kada meter/tipu kada minutu 5), rai iha data lake stream zone; (8) commit offset ba \`tbl_stream_offsets\` (at-least-once). \`true_type\` uza de'it ba evaluasaun, nunka sai input model.${F.kafka && F.kafka.total_messages ? ` Topic Kafka iha mensajen ${fmt(F.kafka.total_messages)} iha partition ${F.kafka.partitions}.` : ""}`));
  out.push(h3("4.3.3. Integrasaun Sistema (Launcher)"));
  out.push(p("Launcher `start_all.bat` hahú Kafka (KRaft), FastAPI, Spark Streaming, bot no website iha janela rasik. Script ne'e verifika uluk se komponente ida ativu ona (`setup/check_running.ps1`) no la hahú dala rua, tanba Kafka broker rua iha log dir hanesan bele estraga dadus Kafka. Se dadus Kafka estraga (`CorruptRecordException`), `setup\\start_kafka.bat reset` hamoos dadus Kafka de'it — readings iha MySQL la lakon."));

  // ---------------------------------------------------------------- 4.4 database
  out.push(h2("4.4. Dezeñu Database (Database Design)"));
  out.push(p("Imajen tuir mai hatudu oinsá table sira liga ba malu iha database `powerpredict_db`: meter liga ba municipality; readings live no anomaly events liga ba meter; model rejista iha tbl_models; rezultadu Spark (Data Quality, statistics anomalia, top meters) rai tuir dataset; konta website (tbl_users) iha sesaun login (tbl_user_sessions)."));
  out.push(...figure("d6_erd.png", "Relasaun entre table database powerpredict_db"));
  out.push(p(`Database iha table ${nT}. Table importante liu mak:`));
  const dbt = (cap, rows) => table(["No", "Field", "Type", "Deskrisaun"], rows.map((r, i) => [i + 1, ...r]), [7, 30, 15, 48], { caption: cap, size: 18 });
  out.push(...dbt("tbl_meters", [["meter_id", "Varchar(20)", "Kódigu meter (unique)"], ["customer_type", "Varchar(20)", "Customer type"], ["municipality", "Varchar(40)", "Municipality"], ["contracted_kva", "Float", "Contracted kVA"], ["peak_util, pf_base", "Float", "Ba baseline meter"], ["is_live", "Boolean", "Haruka stream real-time"]]));
  out.push(...dbt("tbl_readings_live", [["id", "BigInt", "Primary key"], ["meter_id, ts", "Varchar / Datetime", "Meter no tempu"], ["voltage_v … temperature_c", "Float", "Valór medidu"], ["load_ratio, baseline_ratio, consistency_ratio", "Float", "Feature engineering"], ["predicted_type", "Varchar(24)", "Output Random Forest"], ["anomaly_score, confidence", "Float", "1 − P(Normal), P(tipu)"], ["true_type", "Varchar(24)", "Ground truth (evaluation only)"], ["latency_ms", "Float", "Latency bot → MySQL"]]));
  out.push(...dbt("tbl_anomaly_events", [["id", "Integer", "Primary key"], ["meter_id, municipality", "Varchar", "Fatin"], ["anomaly_type, severity", "Varchar", "Tipu no severity"], ["anomaly_score", "Float", "Score Random Forest"], ["reason", "Text", "Evidénsia (ezemplu voltage −14%)"], ["acknowledged", "Boolean", "Admin konfirma ona"]]));
  out.push(...dbt("tbl_models", [["id", "Integer", "Primary key"], ["algorithm, engine, dataset", "Varchar", "random_forest, scikit-learn / spark-mllib"], ["anomaly_precision, anomaly_recall, anomaly_f1", "Float", "Métrika binary"], ["pr_auc, roc_auc, f1_macro", "Float", "Métrika seluk"], ["feature_importance, class_report", "LongText", "JSON"], ["file_path, is_active", "Varchar / Boolean", "File joblib, model ativu"]]));
  out.push(...dbt("tbl_users", [["id", "Integer", "Primary key"], ["username", "Varchar(40)", "Naran login (unique)"], ["full_name", "Varchar(80)", "Naran kompletu"], ["role", "Varchar(10)", "admin ka user"], ["password_hash", "Varchar(200)", "pbkdf2_sha256$260000$salt$hash (la rai password)"], ["is_active, last_login", "Boolean / Datetime", "Konta ativu, login ikus"]]));
  out.push(...dbt("tbl_user_sessions", [["id", "Integer", "Primary key"], ["token_hash", "Varchar(64)", "SHA-256 husi token cookie (unique)"], ["user_id", "Integer", "Konta (tbl_users)"], ["expires_at", "Datetime", "Sesaun remata (oras 8)"]]));

  // ---------------------------------------------------------------- 4.5 Random Forest implementation
  out.push(h2("4.5. Implementasaun Random Forest"));
  out.push(p(`Random Forest implementa dala rua: **scikit-learn** (\`ml/train.py\`) ba model ne'ebé sistema uza iha stream no Deteksaun Manual, no **Spark MLlib** (\`pipeline/spark_ml_train.py\`) ba training distribuídu iha readings millaun. Model ida-idak rejista iha \`tbl_models\`; Admin bele ativa model ida husi pájina Model Random Forest.`));
  out.push(...table(["Parameter", `scikit-learn (#${rf.id}, ativu)`, `Spark MLlib (#${ml.id})`], [
    ["Tree", String(P.n_estimators), String(MP.numTrees)],
    ["Kle'an máximu", String(P.max_depth), String(MP.maxDepth)],
    ["Feature kada split", P.max_features, MP.featureSubsetStrategy],
    ["Folla minimu / bins", `min_samples_leaf ${P.min_samples_leaf}`, `maxBins ${MP.maxBins}`],
    ["Class weight", `Normal = ${P.normal_weight}`, "la uza"],
    ["Training data", `${fmt(rf.n_train)} readings (sample)`, `${fmt(ml.n_train)} readings (Normal ${fmt(MP.normal_rows)} + to'o ${fmt(MP.cap_per_type)} / tipu)`],
    ["Test data", `${fmt(rf.n_test)} readings (sample natural)`, `${fmt(ml.n_test)} readings (meters test hotu)`],
  ], [26, 37, 37], { caption: "Konfigurasaun Random Forest", firstColBold: true, size: 18 }));

  // ---------------------------------------------------------------- 4.6 dashboard design
  out.push(h2("4.6. Dezeñu Dashboard (Dashboard Design)"));
  out.push(p("Website dezenvolve ho **Streamlit** hodi implementasaun fasil: operadór EDTL, ekipa inspesaun no estudante loke browser de'it (http://localhost:8505). Website iha pájina login no pájina 9 ne'ebé grupu iha menu haat: Monitorizasaun, Análize, Admin no Informasaun. Pájina prinsipál tuir estrutura projetu capstone referénsia (alerta → deskritiva → tendénsia → estatístika → machine learning → real-time)."));
  out.push(...figure("d7_pages.png", "Estrutura website Streamlit (login + pájina 9)"));
  out.push(p("Funsaun komún iha website:"));
  out.push(...bullets([
    "**Login no role:** konta iha `tbl_users` ho role **Admin** ka **User**. Pájina login iha tab rua (User no Admin); konta tenke uza tab tuir nia role. User haree no analiza (Dashboard, Monitor, Events, Deteksaun Manual, Model, Big Data Pipeline, Metodolojia); Admin bele mós halo simulasaun, retrain no ativa model, konfirma events no jere konta.",
    "**Seguransa:** password rai nu'udar hash PBKDF2-SHA256 (iterasaun 260,000, salt aleatóriu) — sistema nunka rai password. Login kria token aleatóriu; browser rai token iha cookie `pp_session` (oras 8) no database rai de'it nia hash SHA-256. Logout ka muda password hamoos sesaun. Konta Admin no User dahuluk kria husi `.env` (la hakerek iha kódigu).",
    "**Refresh:** kada pájina iha butaun Refresh ho oras atualizasaun ikus; butaun ne'e hamoos cache no foti dadus foun husi MySQL.",
    "**Night mode / Day mode:** butaun iha sidebar muda tema website tomak; login la lakon bainhira pájina reload.",
    "**Ikon profisionál:** ikon Material Design iha menu, títulu no KPI (la uza emoji); sidebar hatudu konta ne'ebé login no estadu sistema (MySQL, FastAPI, bot, Spark Streaming).",
  ]));

  // ---------------------------------------------------------------- 4.7 interface
  out.push(h2("4.7. Implementasaun Interface"));
  const ui = (num, title, text, imgs) => {
    out.push(h3(`4.7.${num}. ${title}`)); out.push(p(text));
    imgs.forEach(([img, cap, opt]) => out.push(...figure(img, cap, opt)));
  };
  ui(1, "Login no Asesu tuir Role", "Pájina login hatudu tab User no tab Admin, username no password, no butaun Night mode. Depois login, sidebar hatudu naran konta no nia role. Ba User, menu Admin (Simulasaun, Jestaun Uzuáriu) la mosu no URL pájina Admin la loke.",
    [["ui_login.png", "Pájina login (tab User / Admin)", { maxHeight: 420 }], ["ui_user_view.png", "Website ba konta User: menu Admin la mosu", { maxHeight: 460 }]]);
  ui(2, "Dashboard (Pájina Prinsipál)", "Dashboard hatudu header ho butaun Refresh no status MySQL no Spark Streaming, alerta (meters ho anomalia agora no suspeita Na'ok Eletrisidade), análitika deskritiva (readings, meters, municipalities, Anomaly Rate, anomaly events), tendénsia anomalia kada minutu, no estatístika tuir tipu no municipality. Refresh automátiku kada segundu 10.",
    [["ui_home_window.png", "Dashboard ho sidebar (menu, konta, Night mode, estadu sistema)", { maxHeight: 460 }], ["ui_home_trend.png", "Dashboard: análize tendénsia no estatístika", { maxHeight: 640 }]]);
  ui(3, "Dashboard: Machine Learning no Real-Time", "Seksaun Deteksaun Anomalia hatudu distribuisaun Anomaly Score no Precision/Recall tuir threshold; seksaun Klasifikasaun hatudu Accuracy, Precision, Recall, F1-Score live, Feature Importance, Confusion Matrix no table readings foun.",
    [["ui_home_score.png", "Dashboard: Anomaly Score Random Forest", { maxHeight: 520 }], ["ui_home_rf.png", "Dashboard: klasifikasaun tipu anomalia (Random Forest)", { maxHeight: 700 }], ["ui_home_realtime.png", "Dashboard: monitorizasaun real-time", { maxHeight: 520 }]]);
  ui(4, "Monitor Real-Time", "Mapa meters 281 ho kór tuir tipu anomalia, estadu kada municipality, feed anomalia ho evidénsia, no gráfiku voltage no frequency. Refresh kada segundu 5.", [["ui_monitor.png", "Monitor Real-Time", { maxHeight: 640 }]]);
  ui(5, "Anomaly Events", "Lista events ho filter (tipu, municipality, severity, estadu), download CSV no lista suspeita Na'ok Eletrisidade ba ekipa inspesaun. Konfirma event: Admin de'it.",
    [["ui_events.png", "Anomaly Events", { maxHeight: 600 }], ["ui_events_theft.png", "Lista suspeita Na'ok Eletrisidade", { maxHeight: 360 }]]);
  ui(6, "Deteksaun Manual", `Utilizadór hili meter, hatama valór reading (ka hili ezemplu) no Random Forest hatán tipu anomalia, Anomaly Score, votu tree ${P.n_estimators} no evidénsia.`, [["ui_detect.png", "Deteksaun Manual (ezemplu Na'ok Eletrisidade)", { maxHeight: 620 }]]);
  ui(7, "Model Random Forest", "Pájina model hatudu rezultadu test model Random Forest (scikit-learn no Spark MLlib), tanba sa Random Forest, Precision/Recall tuir tipu, Confusion Matrix, threshold no Feature Importance. Admin bele retrain ho parameter foun no ativa model; User haree de'it.",
    [["ui_models_compare.png", "Model Random Forest: rezultadu test no tanba sa Random Forest", { maxHeight: 520 }], ["ui_models_detail.png", "Model Random Forest: evaluasaun detalla", { maxHeight: 640 }]]);
  ui(8, "Big Data Pipeline", "Pájina ne'e hatudu dataset 5 GB / 10 GB / 20 GB, Spark ETL no Data Quality, anomalia iha dadus batch (oras, fulan, municipality, feature profile, top meters), benchmark Pandas vs Spark no estatístika streaming.",
    [["ui_bigdata_datasets.png", "Big Data Pipeline: dataset no anomalia ne'ebé hatama", { maxHeight: 560 }]]);
  ui(9, "Simulasaun (Admin)", "Simulasaun permite Admin hamosu Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu, Na'ok Eletrisidade no Kontador Aat ba demonstrasaun, no muda intervalu bot no threshold events.",
    [["ui_simulation.png", "Simulasaun anomalia", { maxHeight: 560 }]]);
  ui(10, "Jestaun Uzuáriu (Admin)", "Admin kria konta foun (password karakter 8 ka liu), muda role, ativa ka dezativa konta, reset password no hamoos konta. Sistema la permite Admin hamoos ka dezativa nia konta rasik ka Admin ativu ikus. (Konta admin_relatoriu no user_relatoriu iha imajen mak konta temporáriu ba teste automátiku; hamoos ona depois teste.)",
    [["ui_users.png", "Jestaun Uzuáriu", { maxHeight: 600 }]]);
  ui(11, "Metodolojia", "Pájina Metodolojia esplika data source, arkitetura Lambda, tipu anomalia, Random Forest no teknolojia.",
    [["ui_about_arch.png", "Metodolojia: data source no arkitetura", { maxHeight: 520 }]]);
  ui(12, "Night Mode", "Butaun Night mode iha sidebar (ka iha pájina login) muda tema website tomak ba dark; gráfiku no kór tipu anomalia hela hanesan. Butaun Day mode fila fali ba tema light.",
    [["ui_home_dark.png", "Dashboard iha Night mode", { maxHeight: 460 }]]);

  // ---------------------------------------------------------------- 4.8 API
  out.push(h2("4.8. API Gateway (FastAPI)"));
  out.push(p("FastAPI (`api/main.py`) mak dalan tama ba dadus real-time no fornese dokumentasaun OpenAPI automátiku iha `/docs`:"));
  out.push(...table(["Method", "Endpoint", "Funsaun"], [
    ["POST", "/api/readings/batch", "Simu readings barak husi bot, valida no publish ba Kafka"],
    ["POST", "/api/readings", "Simu reading ida"],
    ["GET", "/api/scenario", "Senáriu simulasaun ne'ebé bot tenke tuir (Admin muda iha website)"],
    ["GET", "/api/meters/live", "Lista meters live 281 ba bot"],
    ["POST", "/api/detect", "Deteksaun anomalia ba reading ida (Random Forest)"],
    ["GET", "/api/anomalies/latest", "Anomaly events foun"],
    ["GET", "/health, /api/stats", "Estadu MySQL no Kafka, númeru mensajen"],
  ], [12, 30, 58], { caption: "Endpoint FastAPI", size: 18 }));
  out.push(...figure("ui_api_docs.png", "Dokumentasaun FastAPI (OpenAPI /docs) ba ingest gateway", { maxHeight: 520 }));
  return out;
};
