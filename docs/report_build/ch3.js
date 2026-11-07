// Kapitulu III - Metodolojia Peskiza (manual: Data Source → Data Collection → Data Ingestion → Data Storage →
// Data Processing → Data Analytics → Visualization, plus research stages and the testing method)
const L = require("./lib");
const { p, pn, h1, h2, h3, bullets, numbers, table, figure, eq, mr, frac, sub, sup, sum, fmt, AlignmentType, tt, TYPES } = L;
const R = AlignmentType.RIGHT, C = AlignmentType.CENTER, LEFT = AlignmentType.LEFT;

module.exports = function (F) {
  const rf = F.models.find((m) => m.engine === "scikit-learn" && m.is_active);
  const SIZES = F.sizes, MAIN = F.main, MAINT = MAIN.replace("GB", " GB");
  const DS = Object.fromEntries(F.datasets.map((x) => [x.label, x]));
  const ETL = Object.fromEntries(F.runs.filter((r) => r.job_name === "spark_batch_etl").map((r) => [r.dataset, r]));
  const det = ETL[MAIN].details;
  const PC = rf.class_report.per_class;
  const P = rf.params;
  const ANOM = TYPES.slice(1);
  const out = [];

  out.push(h1("KAPITULU III", "METODOLOJIA PESKIZA"));
  // ---------------------------------------------------------------- 3.1
  out.push(h2("3.1. Fatin no Tempu Peskiza (Research Location and Time)"));
  out.push(p("Peskiza ida ne'e hala'o iha Biblioteka Dili Institute of Technology (DIT) no iha uma autór nian iha Dili, durante semestre unidade kurikuler Big Data (semana 16) tuir planu Capstone Project DIT. Dezenvolvimentu, testing no evaluasaun sistema remata iha fulan Novembru 2026."));

  // ---------------------------------------------------------------- 3.2 stages + 16-week plan (manual 8 + 9)
  out.push(h2("3.2. Etapa no Oráriu Peskiza"));
  out.push(p("Peskiza tuir métodu *Project-Based Learning* no etapa dezenvolvimentu projetu iha manual Capstone Project (seksaun 8): husi identifikasaun problema to'o demonstrasaun live. Tabela tuir mai hatudu planu semana 16 (manual seksaun 9) no output ne'ebé PowerPredict TL produz iha kada semana:"));
  out.push(...table(["Semana", "Atividade (manual)", "Output PowerPredict TL"], [
    ["1", "Introdusaun no formasaun grupu", "Grupu no tema: deteksaun anomalia rede eletrisidade"],
    ["2", "Identifikasaun problema", "Problem statement (seksaun 1.2)"],
    ["3", "Literature Review", "Referénsia no peskiza anterior (Kapitulu II)"],
    ["4", "Preparasaun Project Proposal", "Proposta projetu"],
    ["5", "Data Source no API", "UCI, Open-Meteo API, bot smart meter"],
    ["6", "Data Collection", "Raw dataset 5 GB / 10 GB / 20 GB"],
    ["7", "Kafka / Data Ingestion", "FastAPI → Kafka (streaming pipeline)"],
    ["8", "Data Storage", "Data Lake (D:) + MySQL (table 19)"],
    ["9", "Spark Processing", "Spark batch ETL + Spark Streaming"],
    ["10", "Data Analytics", "Analítika deskritiva, tendénsia no estatístika (Spark)"],
    ["11", "Machine Learning", "Random Forest (scikit-learn + Spark MLlib)"],
    ["12", "Dashboard Development", "Website Streamlit: login, pájina 9, Night mode"],
    ["13", "System Integration", "Protótipu: start_all.bat (sistema tomak)"],
    ["14", "Testing no Evaluation", "Functional, performance, data quality, scalability (Kapitulu V)"],
    ["15", "Final Report no Presentation", "Relatóriu finál no aprezentasaun"],
    ["16", "Final Demo & Defense", "Demonstrasaun live no vídeo YouTube"],
  ], [10, 36, 54], { caption: "Etapa no oráriu peskiza (semana 16)", align: [C, LEFT, LEFT], size: 18 }));

  // ---------------------------------------------------------------- 3.3
  out.push(h2("3.3. Komponente Hardware no Software"));
  out.push(h3("3.3.1. Komponente Hardware"));
  out.push(...table(["No", "Hardware", "Deskrisaun"], [
    ["1.", "Laptop", "Lenovo Legion (model 83NN)"], ["2.", "Processor (CPU)", "Intel Core i9-14900HX (thread 32)"],
    ["3.", "Memory (RAM)", "32 GB"], ["4.", "Storage", "SSD 1 TB (C:, sistema no kódigu) + SSD 512 GB (D:, data lake)"],
    ["5.", "Konektividade", "Internet ba Open-Meteo API"],
  ], [8, 32, 60], { caption: "Komponente hardware" }));
  out.push(h3("3.3.2. Komponente Software"));
  out.push(...table(["No", "Software", "Versaun", "Deskrisaun"], [
    ["1.", "Windows 11 Home", "–", "Operating system"], ["2.", "Python", "3.11", "Linguajen programasaun prinsipál"],
    ["3.", "Java (Eclipse Temurin)", "17", "Runtime ba Spark no Kafka"], ["4.", "Apache Kafka", "3.9.1 (KRaft)", "Data ingestion stream"],
    ["5.", "Apache Spark (PySpark)", "3.5.3", "Batch ETL, streaming, MLlib"], ["6.", "FastAPI + Uvicorn", "0.162 / 0.52", "REST API gateway"],
    ["7.", "XAMPP (MariaDB + phpMyAdmin)", "10.4.32", "Database serving"], ["8.", "scikit-learn / pandas / PyArrow", "1.9 / 3.0 / 25", "Random Forest no dadus"],
    ["9.", "Streamlit + Altair", "1.63 / 6.2", "Website dashboard"], ["10.", "Playwright + Microsoft Edge", "–", "Teste website automátiku no screenshot"],
    ["11.", "Visual Studio Code", "–", "Code editor"], ["12.", "Microsoft Office", "2024", "Hakerek relatóriu"],
  ], [8, 34, 20, 38], { caption: "Komponente software", size: 18 }));

  // ---------------------------------------------------------------- 3.4 Data Source
  out.push(h2("3.4. Data Source"));
  out.push(p("Tuir rekezitu manual Capstone Project (seksaun 5.1), sistema uza data source online no real-time. Dadus iha peskiza ida ne'e mai husi fonte haat:"));
  out.push(...numbers([
    "**UCI ElectricityLoadDiagrams20112014** (Trindade, 2015) – dataset reál husi UCI Machine Learning Repository (Open Data) ho konsumu eletrisidade kliente 264. Uza nu'udar seed ba padraun fulan-fulan no nivel konsumu meters sira.",
    `**Open-Meteo Historical Weather API** (Zippenfenig, 2023) – REST API ne'ebé fornese temperature oras-oras reál tinan 2025 ba kapitál municipality 14 (valór ${fmt(F.weather_rows)}).`,
    "**Open-Meteo Forecast API (live)** – temperature real-time ba municipality 14, ne'ebé bot uza kada minutu 15.",
    "**Bot smart meter (Simulated Real-Time Data)** – meters live 281 ne'ebé haruka readings JSON ba FastAPI kada segundu 2, ho events anomalia no ground truth.",
  ]));
  out.push(...table(["Fonte", "Tipu", "Formatu", "Frekuénsia", "Uza ba"], [
    ["UCI ElectricityLoadDiagrams", "Open data (reál)", "CSV", "Fulan-fulan", "Seed konsumu"],
    ["Open-Meteo Archive", "REST API (reál)", "JSON", "Oras-oras 2025", "Seed temperature"],
    ["Open-Meteo Forecast", "REST API (live)", "JSON", "Minutu 15", "Temperature stream"],
    ["Bot smart meter", "Simulated real-time", "JSON → FastAPI → Kafka", "Segundu 2", "Speed layer"],
    ["Jeradór dataset", "Big data sintétiku", "CSV (part-file)", "Oras-oras 2025", "Batch layer"],
  ], [26, 20, 18, 16, 20], { caption: "Data source PowerPredict TL", size: 18 }));

  // ---------------------------------------------------------------- 3.5 Data Collection
  out.push(h2("3.5. Data Collection"));
  out.push(h3("3.5.1. Métodu Kria Dataset Big Data ho Anomalia (5 GB, 10 GB no 20 GB)"));
  const partMb = DS[MAIN].size_mb / DS[MAIN].parts;
  out.push(p(`Tanba dadus kliente EDTL reál privadu, peskiza ne'e uza métodu aprezentasaun individuál: **hahú husi dadus reál, depois eskala to'o volume alvu**. Jeradór (\`data_generator/generate_dataset.py\`) kria meters ne'ebé distribui tuir populasaun municipality, ho customer type (78% residensiál, 15% komersiál, 5% governu, 2% indústria), contracted kVA no power factor. Kada meter kopia padraun fulan-fulan husi kliente UCI ida no uza temperature Open-Meteo husi nia municipality. Modelu fíziku rede kalkula power (load shape × weekend × temperature × fulan), voltage drop iha feeder no frequency 50 Hz. Depois jeradór **hatama events anomalia** no rai label ground truth. Meters aumenta to'o file CSV to'o tamañu alvu (meters ${fmt(DS["5GB"].meters)} ba 5 GB to'o ${fmt(DS["20GB"].meters)} ba 20 GB, kada meter ho oras 8,760 iha tinan 2025); file fahe ba part-file ~${partMb.toFixed(0)} MB ne'ebé jera paralelu ho process 12–16. Rezultadu data collection (tamañu, records, kualidade) iha seksaun 5.1.`));
  out.push(...figure("d3_data_scaling.png", "Métodu kria dataset big data ho anomalia husi seed reál"));
  out.push(...table(["Tipu anomalia", "Oinsá jeradór hatama", `% records (${MAINT})`], ANOM.map((a) => [tt(a), DS[MAIN].injected_anomalies[a], `${(DS[MAIN].anomaly_pct[a] || 0).toFixed(3)}%`]),
    [20, 62, 18], { caption: "Events anomalia ne'ebé jeradór hatama (ground truth)", size: 18, firstColBold: true }));
  out.push(h3("3.5.2. Deskrisaun Dataset"));
  out.push(p(`Dataset ${MAINT} (records ${fmt(DS[MAIN].rows)}) mak dataset prinsipál ba training Random Forest no análize batch, enkuantu 5 GB no 10 GB uza ba scalability testing no Spark ETL komparativu. Target label mak koluna \`anomaly_type\` ho klase 7: Normal no tipu anomalia neen. Dataset iha tipu dadus barak (numeric, categorical, datetime, text) — liu rekezitu mínimu tipu 2 iha manual.`));
  out.push(...table(["No", "Tempu", "Municipality", "Tipu", "kVA", "V", "A", "kW", "PF", "Hz", "Anomalia"],
    F.sample_rows.map((r, i) => [i + 1, String(r.timestamp).slice(0, 16), r.municipality, r.customer_type.slice(0, 5), r.contracted_kva, r.voltage_v, r.current_a, r.power_kw, r.power_factor, r.frequency_hz, tt(r.anomaly_type)]),
    [4, 15, 11, 7, 5, 8, 7, 7, 6, 7, 15], { caption: `Raw data readings smart meter (sample husi dataset ${MAINT}, tipu ida-idak)`, size: 15 }));
  out.push(...table(["No", "Atributu", "Deskrisaun", "Data type"], [
    ["1", "reading_id", "Identifikadór úniku reading", "Numeric"], ["2", "timestamp", "Data no oras reading (Asia/Dili)", "Datetime"],
    ["3", "meter_id", "Kódigu meter (ezemplu TL-DIL-000412)", "Categorical"], ["4", "municipality", "Municipality (14)", "Categorical"],
    ["5", "customer_type", "Residential, commercial, government, industrial", "Categorical"], ["6", "contracted_kva", "Contracted power (kVA)", "Numeric"],
    ["7", "phase", "Single ka three phase", "Categorical"], ["8", "voltage_v", "Voltage (V), nominál 220 V", "Numeric"],
    ["9", "current_a", "Current (A)", "Numeric"], ["10", "power_kw", "Active power (kW)", "Numeric"], ["11", "energy_kwh", "Energy (kWh)", "Numeric"],
    ["12", "power_factor", "Power factor (0–1)", "Numeric"], ["13", "frequency_hz", "Frequency (Hz), nominál 50 Hz", "Numeric"],
    ["14", "temperature_c", "Temperature (°C) husi Open-Meteo", "Numeric"], ["15", "anomaly_type", "Normal ka tipu anomalia 6 (target, ground truth)", "Categorical"],
  ], [7, 22, 51, 20], { caption: "Deskrisaun atributu dataset", size: 18 }));

  // ---------------------------------------------------------------- 3.6 Data Ingestion
  out.push(h2("3.6. Data Ingestion"));
  out.push(p("Tuir manual seksaun 5.2 (ezemplu API → Kafka), dadus real-time tama liuhusi dalan: **Bot → FastAPI → Apache Kafka**. Bot (`bot/bot_live.py`) hamosu readings husi meters live 281 kada segundu 2 no haruka batch ida ba FastAPI (`POST /api/readings/batch`). FastAPI valida structure (Pydantic) no publish kada reading ba topic Kafka `smart-meter-readings` (partition 3) ho key = meter_id, entaun readings husi meter ida sempre iha partition hanesan no tuir orden. Ezemplu mensajen:"));
  out.push(pn('`{"reading_id": "9f2c…", "meter_id": "TL-OEC-000917", "timestamp": "2026-11-07 09:43:17", "municipality": "Oecusse", "customer_type": "residential", "contracted_kva": 2.2, "phase": "single", "voltage_v": 191.4, "current_a": 4.21, "power_kw": 0.76, "power_factor": 0.94, "frequency_hz": 50.02, "temperature_c": 31.8, "true_type": "Voltage Sag", "producer_ts": 1794012197000}`'));
  out.push(p("Ba batch layer, dataset CSV (5 GB / 10 GB / 20 GB) tama direta ba data lake raw zone; bot replay (`bot/bot_replay.py`) bele haruka dataset ne'e ba Kafka hanesan stream."));

  // ---------------------------------------------------------------- 3.7 Data Storage
  out.push(h2("3.7. Data Storage"));
  out.push(p(`Tuir manual seksaun 5.3, sistema uza teknolojia storage rua: **Data Lake** no **MySQL**. Data lake rai iha disk D: (\`PP_DATA_DIR\`) ho zona tolu — raw zone (CSV dataset), curated zone (Parquet husi Spark ETL, partition tuir fulan, no sample training/test) no stream zone (Parquet husi readings real-time). MySQL \`powerpredict_db\` (XAMPP) mak serving layer ho table ${F.tables.length}: readings live, anomaly events, model, rezultadu Spark, benchmark no konta website. Dezeñu database detalla iha seksaun 4.4.`));

  // ---------------------------------------------------------------- 3.8 Data Processing
  out.push(h2("3.8. Data Processing (Apache Spark)"));
  out.push(p("Tuir manual seksaun 5.4, Apache Spark halo Data Cleaning, Transformation, Aggregation, Filtering, Join no Feature Engineering. Batch ETL (`pipeline/spark_batch_etl.py`) no Spark Streaming (`pipeline/spark_streaming.py`) uza funsaun feature hanesan."));
  out.push(h3("3.8.1. Data Cleaning"));
  out.push(...bullets([
    "**Missing value:** reading ho valór mamuk iha koluna krítiku (voltage, current, power factor, temperature) hamoos (`dropna`).",
    "**Duplicate:** reading ho meter_id no timestamp hanesan hamoos (`dropDuplicates`).",
    "**Invalid value:** valór imposível (voltage 0 ka 999.9 V, power factor > 1, power negativu) hamoos tuir limit validasaun (Filtering).",
    "**Anomalia la hamoos:** voltage ne'ebé tun ka sa'e maka'as mak anomalia reál ne'ebé model tenke aprende — outliers la'ós erru (analiza iha seksaun 5.1.3).",
  ]));
  out.push(h3("3.8.2. Feature Engineering no Join"));
  out.push(p("Spark kria feature foun husi koñesimentu domíniu, no halo broadcast join ho meter master (peak utilisation, power factor base) no table load shape ki'ik (customer type × oras). Feature rua ikus mak xave ba anomalia kliente no meter:"));
  out.push(...table(["Feature", "Formula", "Deteta"], [
    ["load_ratio", "(power_kw / power_factor) / contracted_kva", tt("Overload")],
    ["voltage_dev_pct", "(voltage_v − 220) / 220 × 100", `${tt("Voltage Sag")}, ${tt("Over-Voltage")}`],
    ["freq_dev_hz", "frequency_hz − 50", tt("Frequency Deviation")],
    ["baseline_ratio", "power_kw / (kVA × peak util × load shape(oras) × weekend × temperature × PF base)", `${tt("Energy Theft")}, ${tt("Overload")}`],
    ["consistency_ratio", "power_kw / (V × I × PF × faze) ×1000", tt("Meter Fault")],
    ["hour, is_weekend", "hour(timestamp), dayofweek", "Kontestu tempu"],
  ], [20, 52, 28], { caption: "Feature engineering iha Spark", size: 18 }));
  out.push(p("Funsaun feature hanesan iha Spark (batch no stream) no pandas (Deteksaun Manual); teste ida konfirma katak diferensa entre rua mak 0 ba feature hotu. Model uza feature 16 (numeric 14 + categorical 2)."));
  out.push(h3("3.8.3. Data Transform"));
  out.push(p("Feature kategóriku (customer_type, municipality) transforma ho **one-hot encoding**. Random Forest la presiza normalizasaun (scaling) tanba split tree depende ba orden valór, la'ós ba eskala. Valór numériku rai nu'udar float; output curated rai nu'udar Parquet (columnar)."));
  out.push(h3("3.8.4. Data Labeling (Ground Truth)"));
  out.push(p("Label la kalkula husi threshold, maibé husi **events ne'ebé jeradór hatama**. Se events barak akontese iha oras hanesan, label tuir prioridade: Kontador Aat > Na'ok Eletrisidade > Frekuénsia La Estável > Voltajen Tun > Voltajen Sa'e Liu > Karga Liu. Dataset rai valór **medidu** (valór loos + noise sensor), tan ne'e model tenke aprende padraun ho noise:"));
  out.push(...table(["Label", "Deskrisaun", "Severity"], [
    ["Normal", "La iha event", "–"],
    [tt("Voltage Sag"), "Event feeder: voltage tun 14–35 V, oras 1–6", "High"],
    [tt("Over-Voltage"), "Event kalan: voltage sa'e 22–34 V, oras 1–3", "Medium"],
    [tt("Frequency Deviation"), "Generator trip: frequency 0.6–1.6 Hz husi 50 Hz", "High"],
    [tt("Overload"), "Load ratio loos > 1.0 (event ka pico naturál)", "High"],
    [tt("Energy Theft"), "Meter rejista 5–55% husi konsumu loos durante loron 3–30", "High"],
    [tt("Meter Fault"), "Current channel ×0.15–0.5 ka ×1.8–3.0 durante loron 1–7", "Medium"],
  ], [24, 60, 16], { caption: "Data labeling (target classes)", size: 18 }));
  out.push(h3("3.8.5. Divizaun Training no Test (Meter-Level Split)"));
  out.push(p(`Episódiu Na'ok Eletrisidade ka Kontador Aat naruk (loron to'o semana). Se fahe tuir reading aleatóriu, readings husi episódiu hanesan bele mosu iha training no test, no rezultadu sai di'ak liu realidade. Tan ne'e Spark fahe **tuir meter**: meter ne'ebé crc32(meter_id) mod 5 = 0 sai meter test. Training set: Normal undersampled ba 600,000 + anomalia hotu (to'o 80,000 kada tipu) = ${fmt(det.train_sample_rows)} readings. Test set: sample ${fmt(det.test_sample_rows)} readings husi meters test ho **distribuisaun natural**; meters test iha readings ${fmt(det.test_meter_rows)} hotu (uza ba test Spark MLlib).`));
  out.push(...figure("d8_split.png", "Dezeñu evaluasaun: meter-level split no test set ho distribuisaun natural"));
  out.push(...table(["Klase", "Training set (maizumenus)", "Test set (natural)"], TYPES.map((a) => [tt(a), fmt(det.train_class_counts[a] && Math.min(det.train_class_counts[a], a === "Normal" ? 600000 : 80000) || 0), fmt(PC[a].support)]),
    [40, 30, 30], { caption: `Distribuisaun klase iha training set no test set (dataset ${MAINT})`, align: [LEFT, R, R], size: 18 }));
  out.push(p("Nota: númeru training set ba kada klase mak maizumenus, tanba Spark `sampleBy` foti sample aleatóriu tuir fraksaun."));

  // ---------------------------------------------------------------- 3.9 Data Analytics + Random Forest
  out.push(h2("3.9. Data Analytics no Métodu Random Forest"));
  out.push(h3("3.9.1. Analítika Deskritiva, Tendénsia no Estatístika"));
  out.push(p(`Tuir manual seksaun 5.5, Spark halo analítika tolu iha dataset ${MAINT} (\`4_anomaly_analytics\` iha batch ETL) no iha stream (window 1 minutu): (1) **descriptive** — readings no anomalia kada municipality, tipu no customer type; (2) **trend** — anomalia kada oras, kada fulan no kada minutu; (3) **statistical** — média no percentile (p05, p50, p95) feature kada tipu anomalia (\`percentile_approx\`), no top meters ho oras anómala barak liu. Rezultadu rai iha \`tbl_batch_anomaly_stats\`, \`tbl_batch_feature_profile\` no \`tbl_batch_top_meters\`; rezultadu iha seksaun 5.2. Analítika avansadu mak klasifikasaun anomalia ho Random Forest.`));
  out.push(h3("3.9.2. Random Forest"));
  out.push(p("Random Forest mak métodu *ensemble learning* ne'ebé hari'i decision tree barak no kombina rezultadu liuhusi votasaun. Kalkulasaun iha sistema bazeia ba etapa tuir mai:"));
  out.push(...figure("d4_random_forest.png", "Lala'ok algoritmu Random Forest ba deteksaun anomalia"));
  out.push(pn("**a) Bootstrap Aggregating (Bagging).**", { keepNext: true }));
  out.push(p(`Ba kada tree (${P.n_estimators}), sistema foti random sample ho replacement husi training set (${fmt(rf.n_train)} readings). Readings balun mosu dala barak no balun la mosu (kuaze 36.8%), entaun tree ida-idak iha variasaun.`));
  out.push(pn("**b) Random Feature Selection.**", { keepNext: true }));
  out.push(p(`Iha kada node, tree la haree feature hotu, maibé sub-konjuntu aleatóriu √16 ≈ 4 (parameter \`max_features = sqrt\`). Ida ne'e hamenus korelasaun entre tree no variance model. Tree ida-idak bele to'o kle'an ${P.max_depth} (\`max_depth\`) ho folla minimu readings ${P.min_samples_leaf}.`));
  out.push(pn("**c) Gini Impurity no Class Weight.**", { keepNext: true }));
  out.push(eq([mr("Gini(t)=1−"), sum(sup(sub(mr("p"), mr("k")), mr("2")), mr("k=1"), mr("K"))], "3.1"));
  out.push(eq([mr("ΔGini=Gini(parent)−"), frac(sub(mr("n"), mr("L")), mr("n")), mr("Gini(L)−"), frac(sub(mr("n"), mr("R")), mr("n")), mr("Gini(R)")], "3.2"));
  out.push(...bullets([
    "p_k: proporsaun klase k iha node t; K = 7 (Normal + tipu anomalia 6). Split ho ΔGini boot liu mak hili.",
    `**Class weight:** kada reading Normal konta dala ${P.normal_weight} iha kalkulasaun p_k (\`class_weight = {Normal: ${P.normal_weight}}\`). Tanba training set undersample Normal, peza ne'e fó fali importánsia ba Normal hodi model la fó alarme falsu barak iha distribuisaun natural.`,
  ]));
  out.push(pn("**d) Majority Voting no Anomaly Score.**", { keepNext: true }));
  out.push(p("Kada tree vota klase ida. Probability klase mak média probability tree hotu; klase finál mak probability aas liu. **Anomaly Score** mak probability katak reading la'ós Normal:"));
  out.push(eq([mr("P(k|x)="), frac(mr("1"), mr("T")), sum([sub(mr("p"), mr("t,k")), mr("(x)")], mr("t=1"), mr("T")), mr("    Anomaly Score=1−P(Normal|x)")], "3.3"));
  const V = F.vote_example;
  out.push(p(`**Ezemplu reál husi sistema:** meter residensiál iha Dili ho contracted 3.5 kVA, iha oras 20:00, rejista power ${V.inputs.power_kw} kW (baseline ratio ${V.features.baseline_ratio.toFixed(3)}), voltage ${V.inputs.voltage_v} V no frequency ${V.inputs.frequency_hz} Hz. Husi tree ${V.n_trees}, tree ${V.votes[V.predicted_type]} vota **${tt(V.predicted_type)}**; Anomaly Score ${V.anomaly_score.toFixed(2)}. Konsumu ki'ik tebes iha oras pico kalan mak padraun na'ok eletrisidade.`));
  out.push(pn("**e) Feature Importance.**", { keepNext: true }));
  out.push(eq([mr("Imp(f)="), frac(mr("1"), mr("T")), sum(sum([frac(sub(mr("n"), mr("t")), mr("n")), mr("ΔGini(t)")], mr("t: split ho f")), mr("tree"))], "3.4"));
  out.push(p("Feature Importance mak redusaun Gini totál husi split ne'ebé uza feature f, média ba tree hotu; rezultadu iha seksaun 5.3."));

  // ---------------------------------------------------------------- 3.9.3 manual calculation
  const MN = F.manual, cl = MN.classes, mc = MN.counts, man = MN.attributes;
  out.push(h3("3.9.3. Data Training: Kalkulasaun Manuál Root Node"));
  out.push(p(`Hodi hatudu oinsá decision tree hili root node, tuir mai amostra readings reál **12** husi training set ${MAINT} ho klase tolu (Normal ${mc.Normal}, Voltajen Tun ${mc["Voltage Sag"]}, Na'ok Eletrisidade ${mc["Energy Theft"]}) no atributu diskretizadu tolu: Voltage (Low se voltage deviation < −6%), Consumption (Low se baseline ratio < 0.5) no Period (Peak 18:00–22:00):`));
  out.push(...table(["No", "Meter", "Municipality", "V", "Dev %", "Baseline", "Oras", "Voltage", "Consumption", "Period", "Klase"],
    MN.rows.map((r, i) => [i + 1, r.meter_id, r.municipality, r.voltage_v, r.voltage_dev_pct, r.baseline_ratio, r.hour, r.Voltage, r.Consumption, r.Period, tt(r.anomaly_type)]),
    [4, 13, 11, 7, 7, 8, 5, 8, 11, 9, 17], { caption: "Amostra training data 12 (reál)", size: 15 }));
  out.push(pn("**Kalkulasaun Entropy**", { keepNext: true }));
  out.push(eq([mr("Entropy(S)=−"), sum([sub(mr("p"), mr("i")), sub(mr("log"), mr("2")), sub(mr("p"), mr("i"))], mr("i=1"), mr("3"))], "3.5"));
  const term = (n) => (n ? -(n / 12) * Math.log2(n / 12) : 0);
  out.push(eq([mr("Entropy(S)=−("), frac(mr(String(mc.Normal)), mr("12")), mr("×"), sub(mr("log"), mr("2")), frac(mr(String(mc.Normal)), mr("12")), mr(")−("),
    frac(mr(String(mc["Voltage Sag"])), mr("12")), mr("×"), sub(mr("log"), mr("2")), frac(mr(String(mc["Voltage Sag"])), mr("12")), mr(")−("),
    frac(mr(String(mc["Energy Theft"])), mr("12")), mr("×"), sub(mr("log"), mr("2")), frac(mr(String(mc["Energy Theft"])), mr("12")), mr(")")]));
  out.push(eq([mr(`Entropy(S)=${term(mc.Normal).toFixed(4)}+${term(mc["Voltage Sag"]).toFixed(4)}+${term(mc["Energy Theft"]).toFixed(4)}=${MN.total_entropy.toFixed(4)}`)]));
  const partEq = (attr) => man[attr].parts.map((x) => {
    const terms = cl.map((c) => [c, x[c]]).filter(([, n]) => n > 0);
    return pn(`• **${attr} = ${x.value}** (${x.n} records: ${terms.map(([c, n]) => `${tt(c)} ${n}`).join(", ")}) → Entropy = ${terms.map(([, n]) => `−(${n}/${x.n})·log₂(${n}/${x.n})`).join(" ")} = **${x.entropy.toFixed(4)}**`, { align: AlignmentType.LEFT });
  });
  out.push(pn("**A. Atributu: Voltage**", { keepNext: true })); out.push(...partEq("Voltage"));
  out.push(pn("**B. Atributu: Consumption**", { keepNext: true })); out.push(...partEq("Consumption"));
  out.push(pn("**C. Atributu: Period**", { keepNext: true })); out.push(...partEq("Period"));
  out.push(pn("**Kalkula Gain**", { keepNext: true }));
  out.push(eq([mr("Gain(S,A)=Entropy(S)−"), sum([frac(mr("|Sv|"), mr("|S|")), mr("Entropy("), sub(mr("S"), mr("v")), mr(")")], mr("v∈A"))], "3.6"));
  const g = man.Voltage, gp = g.parts;
  out.push(eq([mr(`Gain(Voltage)=${MN.total_entropy.toFixed(4)}−[(`), frac(mr(String(gp[0].n)), mr("12")), mr(`×${gp[0].entropy.toFixed(4)})+(`), frac(mr(String(gp[1].n)), mr("12")), mr(`×${gp[1].entropy.toFixed(4)})]=${g.gain.toFixed(4)}`)]));
  const gainRows = [];
  ["Voltage", "Consumption", "Period"].forEach((a) => {
    gainRows.push([a, "12", mc.Normal, mc["Voltage Sag"], mc["Energy Theft"], "", man[a].gain.toFixed(4), man[a].gini_gain.toFixed(4)]);
    man[a].parts.forEach((x) => gainRows.push([`  ${x.value}`, x.n, x.Normal, x["Voltage Sag"], x["Energy Theft"], x.entropy.toFixed(4), "", x.gini.toFixed(4)]));
  });
  out.push(...table(["Atributu", "Totál", "Normal", "Voltajen Tun", "Na'ok", "Entropy", "Gain", "Gini / ΔGini"], gainRows, [17, 8, 10, 13, 10, 13, 13, 16],
    { caption: "Kalkula Gain root node (summary)", align: [LEFT, C, C, C, C, R, R, R], size: 18 }));
  const best = Object.entries(man).sort((a, b) => b[1].gain - a[1].gain)[0];
  out.push(p(`Atributu **${best[0]}** sai root node tanba nia Information Gain boot liu (${best[1].gain.toFixed(4)}) no redusaun Gini boot liu (${best[1].gini_gain.toFixed(4)}). Depois Consumption (Gain ${man.Consumption.gain.toFixed(4)}) fahe Na'ok Eletrisidade husi Normal. Period fó informasaun uitoan de'it (Gain ${man.Period.gain.toFixed(4)}). Ida ne'e konsistente ho Feature Importance model tomak (seksaun 5.3), iha ne'ebé voltage no baseline ratio mak feature importante.`));

  // ---------------------------------------------------------------- 3.10 Visualization
  out.push(h2("3.10. Visualization"));
  out.push(p("Tuir manual seksaun 5.6, dashboard tenke hatudu KPI, gráfiku, tendénsia, monitorizasaun real-time no alerta. Website Streamlit PowerPredict TL implementa elementu hotu ne'e:"));
  out.push(...table(["Rekezitu manual", "Implementasaun iha website"], [
    ["KPI", "Readings, meters, municipalities, Anomaly Rate, anomaly events, Precision / Recall / F1 live"],
    ["Charts / Grafiku", "Bar empilhadu tuir tipu, histograma Anomaly Score, Confusion Matrix, Feature Importance, mapa meters"],
    ["Trend", "Anomalia kada minutu (window Spark 1 min / 5 min), tuir oras no fulan"],
    ["Real-Time Monitoring", "Monitor Real-Time (refresh 5 s), Dashboard (refresh 10 s), feed anomalia, Voltage no Frequency"],
    ["Alert / Notification", "Alerta meters ho anomalia agora no suspeita Na'ok Eletrisidade; anomaly events ho severity"],
  ], [26, 74], { caption: "Rekezitu visualization no implementasaun", firstColBold: true, size: 18 }));
  out.push(p("Dezeñu dashboard no screenshot interface hotu iha seksaun 4.6 no 4.7."));

  // ---------------------------------------------------------------- 3.11 testing method
  out.push(h2("3.11. Métodu Testing no Evaluasaun"));
  out.push(p("Tuir manual seksaun 14, testing hala'o iha aspeitu haat, plus evaluasaun model machine learning:"));
  out.push(...table(["Aspeitu", "Saida mak sukat", "Oinsá"], [
    ["Functional Testing", "Dadus tama? Prosesa loos? Rai loos? Dashboard funsiona?", "Kontajen Kafka / MySQL, simulasaun anomalia, pytest, teste website automátiku (Playwright)"],
    ["Performance Testing", "Processing time, throughput, latency, CPU / memory usage", "Timer kada etapa Spark ETL, latency kada micro-batch, psutil (CPU % no RAM)"],
    ["Data Quality Testing", "Missing values, duplicate, invalid records, outliers", "Spark profiling iha kada ETL; validasaun stream; regra IQR ba outliers"],
    ["Scalability Testing", "Processing time tuir volume 10,000 – 1,000,000 + 5 / 10 / 20 GB", "Pandas vs Spark, servisu hanesan, run 3 kada engine, memory guard 85% RAM"],
    ["Evaluasaun model", "Precision, Recall, F1, PR-AUC, ROC-AUC, FPR, Macro F1", "Test set husi meters separadu (seksaun 3.8.5), cross-validation 3-fold, stream live"],
  ], [20, 38, 42], { caption: "Métodu testing no evaluasaun", firstColBold: true, size: 17 }));
  return out;
};
