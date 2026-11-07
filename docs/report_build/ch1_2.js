// Abstract, Kapitulu I (Introdusaun) and Kapitulu II (Revizaun Literatura) - datasets 5 GB / 10 GB / 20 GB
const L = require("./lib");
const { p, pn, h1, h1plain, h2, h3, bullets, numbers, table, figure, eq, mr, frac, sub, sup, sum, fmt, pct, tt, TYPES } = L;

module.exports = function (F) {
  const rf = F.models.find((m) => m.engine === "scikit-learn" && m.is_active);
  const ml = F.models.filter((m) => m.engine === "spark-mllib").slice(-1)[0];
  const CM = F.compare.models;
  const DT = CM["Decision Tree"], LR = CM["Logistic Regression"], IF = CM["Isolation Forest"];
  const rows = F.datasets.reduce((a, x) => a + x.rows, 0);
  const totalGB = F.datasets.reduce((a, x) => a + x.size_mb, 0) / 1024;
  const B = (vol, eng) => F.benchmarks.find((b) => b.vol === vol && b.engine === eng);
  const b5p = B("5GB", "pandas"), b20s = B("20GB", "spark");
  const LV = F.live, S = F.stream;
  const main = F.datasets.find((d) => d.label === F.main);
  const anomPct = (100 - main.anomaly_pct.Normal).toFixed(2);
  const out = [];

  // ------------------------------------------------------------------ ABSTRACT
  out.push(h1plain("ABSTRACT"));
  out.push(p(`Rede distribuisaun eletrisidade iha Timor-Leste hasoru anomalia oin-oin: voltajen tun iha área rurál, voltajen sa'e liu iha kalan, frekuénsia la estável bainhira jeradór ida para, kliente ne'ebé uza liu kontratu, na'ok eletrisidade no meter ne'ebé aat. Monitorizasaun agora dala barak sei reativu no seidauk iha sistema ne'ebé bele analiza readings smart meter ho volume boot iha tempu real. Peskiza ida ne'e dezenvolve **Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest** (PowerPredict TL), sistema Big Data ne'ebé integra bot smart meter, FastAPI, Apache Kafka, Apache Spark, data lake Parquet, MySQL no website Streamlit, no uza algoritmu **Random Forest** hodi klasifika kada reading ba Normal ka tipu anomalia neen: Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu, Na'ok Eletrisidade no Kontador Aat.`));
  out.push(p(`Dataset kria husi seed dadus reál (konsumu UCI ElectricityLoadDiagrams no temperature Open-Meteo) no eskala ba 5 GB, 10 GB no 20 GB (records ${fmt(rows / 1e6, 1)} millaun, ${fmt(totalGB, 1)} GB CSV); jeradór hatama events anomalia no rai ground truth ba kada reading (${anomPct}% anomalia). Random Forest ne'ebé treina iha dataset 20 GB no test iha meters ne'ebé model nunka haree atinje precision ${rf.anomaly_precision.toFixed(3)}, recall ${rf.anomaly_recall.toFixed(3)}, F1-score ${rf.anomaly_f1.toFixed(3)} no PR-AUC ${rf.pr_auc.toFixed(3)}. Iha test set hanesan, Random Forest accurate liu Decision Tree (F1 ${DT.f1.toFixed(3)}), Logistic Regression (F1 ${LR.f1.toFixed(3)}) no Isolation Forest (F1 ${IF.f1.toFixed(3)}), tan ne'e Random Forest mak métodu machine learning ida de'it iha sistema. Spark MLlib Random Forest ne'ebé treina iha readings ${fmt(ml.n_train / 1e6, 1)} millaun no test iha readings ${fmt(ml.n_test / 1e6, 1)} millaun atinje F1 ${ml.anomaly_f1.toFixed(3)}. Iha stream real-time, readings ${fmt(LV.n / 1e6, 2)} millaun prosesa ho latency média ${(S.lat_ms / 1000).toFixed(1)} segundu no F1 ${LV.rf.f1.toFixed(3)}. Scalability testing hatudu katak iha komputadór RAM 32 GB, Pandas la konsege prosesa dataset 5 GB, 10 GB no 20 GB (out of memory), enkuantu Apache Spark prosesa 20 GB iha ${b20s.avg_s.toFixed(0)} segundu ho memory ${fmt(b20s.mem_mb / 1024, 1)} GB. Website iha login Admin no User, butaun Refresh no Night mode. Sistema ne'e bele ajuda EDTL deteta problema rede no suspeita na'ok eletrisidade sedu.`));
  out.push(pn("**Keywords:** Big Data, Anomaly Detection, Apache Kafka, Apache Spark, Random Forest, smart meter, real-time, Timor-Leste."));

  out.push(h1plain("LISTA KONTEÚDU"));
  out.push(new L.d.TableOfContents("Lista Konteúdu", { hyperlink: true, headingStyleRange: "1-3" }));

  // ------------------------------------------------------------------ KAPITULU I
  out.push(h1("KAPITULU I", "INTRODUSAUN"));
  out.push(h2("1.1. Antesedente"));
  out.push(p(`Eletrisidade mak fundasaun ba dezenvolvimentu ekonómiku no sosiál iha nasaun ida. Timor-Leste halo progresu boot: tuir World Development Indicators husi World Bank, asesu ba eletrisidade atinje 94.3% husi populasaun iha tinan 2024 (World Bank, 2025). Maibé jerasaun eletrisidade kuaze tomak (99.6% iha tinan 2021) sei depende ba kombustível mina, no konsumu kada ema sei ki'ik, maizumenus 364 kWh kada tinan (Our World in Data, 2025). Rede nasionál ne'ebé EDTL, E.P. jere liga sentrál elétrika Hera no Betano ho municipality sira liuhusi liña distribuisaun naruk, enkuantu Oecusse no Atauro iha sistema rasik.`));
  out.push(p(`Iha rede hanesan ne'e, **anomalia** akontese beibeik. Anomalia mak padraun iha dadus ne'ebé la tuir komportamentu normál (Chandola et al., 2009). Iha rede eletrisidade, anomalia iha grupu rua. Primeiru, anomalia **kualidade enerjia**: Voltajen Tun (voltage sag) iha oras pico kalan, Voltajen Sa'e Liu (over-voltage) bainhira karga ki'ik, no Frekuénsia La Estável bainhira jeradór ida para (generator trip). Daruak, anomalia iha **kliente no meter**: Karga Liu (kliente uza liu kVA kontratu), Na'ok Eletrisidade (bypass ka manipulasaun meter) no Kontador Aat (sensor korrente sala). Na'ok eletrisidade parte husi *non-technical losses* (NTL); iha nasaun balun NTL bele to'o 40% husi eletrisidade ne'ebé distribui (Glauner et al., 2017).`));
  out.push(p(`Smart meter ka *Advanced Metering Infrastructure* (AMI) produz reading kada minutu ka kada oras. Ho meters rihun ba rihun, readings sira sai millaun records kada loron — dadus ho karakterístika Big Data: volume, velocity no variety (Laney, 2001; Wang et al., 2019). Pandas ka database tradisionál ne'ebé prosesa dadus iha memória komputadór ida de'it la to'o ona; presiza Apache Kafka ba data ingestion stream (Kreps et al., 2011) no Apache Spark ba prosesamentu distribuídu (Zaharia et al., 2016). Aprezentasaun individuál Reinaldo N. M. Alves Costa nian hatudu ona katak Pandas lalais liu ba dadus ki'ik, maibé Spark sai lalais liu bainhira dadus boot (Costa, 2026a). Capstone ida ne'e aumenta eskala ba dataset 5 GB, 10 GB no 20 GB.`));
  out.push(p(`Machine learning permite deteta anomalia automatikamente. Algoritmu **Random Forest** (Breiman, 2001) koñesidu nu'udar klasifikadór ne'ebé forte no estável: Fernández-Delgado et al. (2014) avalia klasifikadór 179 iha dataset 121 no hetan katak família Random Forest mak di'ak liu. Peskiza ida ne'e kontinua métodu Random Forest husi peskiza anterior Reinaldo N. M. Alves Costa nian kona-ba jestaun bee (WaterPredict) (Costa, 2026b), maibé aplika ba **deteksaun anomalia** iha rede eletrisidade: kada reading smart meter klasifika ba Normal ka tipu anomalia neen. Ho arkitetura Lambda — *speed layer* ba real-time no *batch layer* ba dataset to'o 20 GB — PowerPredict TL bele fó alerta sedu ba EDTL no lista suspeita na'ok eletrisidade ba ekipa inspesaun, liuhusi website ne'ebé proteje ho login.`));

  out.push(h2("1.2. Formula Problema (Problem Statement)"));
  out.push(pn("Problema prinsipál iha peskiza ida ne'e mak:"));
  out.push(...numbers([
    "**Deteksaun Reativu** – Voltajen Tun, Frekuénsia La Estável no Karga Liu hatene de'it depois kliente keixa ka ekipamentu aat ona.",
    "**Perda Non-Técnika** – Na'ok Eletrisidade no Kontador Aat la hatene too inspesaun manuál, tanba seidauk iha analítika ba readings meter.",
    `**Volume no Velocity Dadus** – Readings smart meter to'o atus millaun records; iha komputadór RAM 32 GB, Pandas la konsege prosesa dataset 5 GB (memory to'o ${fmt(b5p.mem_mb / 1024, 1)} GB antes para).`,
    "**Falta Sistema Inteligente** – Seidauk iha sistema ne'ebé uza machine learning (Random Forest) hodi klasifika tipu anomalia iha readings smart meter Timor-Leste iha tempu real, ho asesu seguru ba operadór no admin.",
  ]));
  out.push(pn("Husi problema sira ne'e, peskiza ida ne'e hakarak hatán:"));
  out.push(p("*Oinsá dezempeñu algoritmu Random Forest, ne'ebé integra iha arkitetura Big Data (FastAPI–Kafka–Spark), hodi deteta no klasifika anomalia iha readings smart meter iha tempu real, kompara ho model seluk?*"));

  out.push(h2("1.3. Pergunta Peskiza (Research Question)"));
  out.push(...numbers([
    "Oinsá dezempeñu Random Forest hodi deteta no klasifika anomalia bazeia ba Precision, Recall, F1-score, PR-AUC no ROC-AUC iha test set no iha stream real-time?",
    "Tanba sa Random Forest fit (adekuadu) ho dataset smart meter ne'ebé iha ground truth label no klase la balansu?",
    "Oinsá arkitetura Big Data (FastAPI, Kafka, Spark, data lake no MySQL) bele prosesa dataset 5 GB, 10 GB, 20 GB no stream real-time, no iha volume ida ne'ebé mak Pandas la konsege ona?",
    "Feature saida mak fó influénsia boot liu (Feature Importance) ba deteksaun kada tipu anomalia?",
    "Oinsá dezenvolve website ne'ebé fasil uza no seguru (login Admin / User) ba monitorizasaun anomalia, alerta no simulasaun?",
  ]));

  out.push(h2("1.4. Objetivu Peskiza (Research Objective)"));
  out.push(h3("1.4.1. Objetivu Jerál (General Objective)"));
  out.push(p("Dezenvolve no avalia sistema Big Data PowerPredict TL ne'ebé uza algoritmu Random Forest hodi deteta anomalia iha readings smart meter rede eletrisidade Timor-Leste iha tempu real."));
  out.push(h3("1.4.2. Objetivu Espesífiku (Specific Objectives)"));
  out.push(...numbers([
    "Kria dataset Big Data (5 GB, 10 GB, 20 GB) husi seed dadus reál ho events anomalia neen no ground truth label.",
    "Implementa pipeline Big Data kompletu: bot → FastAPI → Apache Kafka → Spark Streaming → MySQL, no batch layer Spark ETL → data lake Parquet.",
    "Implementa Random Forest (scikit-learn) no Random Forest distribuídu (Spark MLlib) ba deteksaun anomalia.",
    "Hatudu ho evidénsia tanba sa Random Forest fit ho dataset, inklui meter-level split, komparasaun ho model seluk no cross-validation.",
    "Identifika Feature Importance ba kada tipu anomalia.",
    "Avalia performance: latency, throughput, Data Quality no scalability Pandas kompara ho Spark.",
    "Dezenvolve website Streamlit ho login Admin / User no pájina 9 ba monitorizasaun, events anomalia, deteksaun manuál, simulasaun no jestaun uzuáriu.",
  ]));

  out.push(h2("1.5. Benefísiu Peskiza (Research Benefit)"));
  out.push(h3("1.5.1. Benefísiu Teóriku (Theoretical Benefit)"));
  out.push(...bullets([
    "Kontribui ba literatura Big Data no anomaly detection iha setór enerjia Timor-Leste.",
    "Fornese evidénsia empíriku katak Random Forest fit ho dadus smart meter ne'ebé iha ground truth label no klase la balansu.",
    "Fornese evidénsia bainhira tenke muda husi Pandas ba Apache Spark (iha ne'e: husi dataset 5 GB ba leten).",
  ]));
  out.push(h3("1.5.2. Benefísiu Prátiku (Practical Benefit)"));
  out.push(...bullets([
    "EDTL hetan alerta automátiku ba Voltajen Tun, Voltajen Sa'e Liu no Frekuénsia La Estável antes kliente keixa.",
    "Ekipa inspesaun hetan lista suspeita Na'ok Eletrisidade no Kontador Aat, hodi hamenus perda non-técnika.",
    "Kliente ho Karga Liu bele hetan avizu antes fuse ka transformadór aat.",
    "Operadór (User) no administradór (Admin) iha asesu tuir sira-nia knaar; funsaun sensitivu (simulasaun, retrain model, jestaun konta) Admin de'it.",
    "DIT no estudante sira hetan protótipu Big Data kompletu ba aprendizajen.",
  ]));

  out.push(h2("1.6. Eskopu no Limitasaun Projetu (Project Scope)"));
  out.push(...numbers([
    "**Dadus Sintétiku ho Seed Reál** – Meters kria husi seed reál (UCI husi Portugál no temperature Open-Meteo); la'ós dadus kliente EDTL, tanba dadus ne'e privadu. Events anomalia hatama husi jeradór.",
    "**Stream Simuladu** – Dadus real-time mai husi bot ho meters simuladu 281, la'ós sensor fíziku.",
    "**Komputadór Ida** – Spark hala'o iha modu local (core 16–32) no Kafka ho broker ida; RAM 32 GB mak limite ne'ebé halo Pandas la konsege dataset 5 GB ba leten.",
    "**Structured Streaming** – Iha Windows, checkpoint Structured Streaming presiza Hadoop winutils; peskiza ne'e uza Spark micro-batch ho offset iha MySQL no watermark manuál.",
    "**Métodu Ida** – Sistema uza Random Forest de'it (hili husi komparasaun iha test set hanesan); la inklui deep learning.",
    "**Seguransa Website** – Login uza password hash no cookie sesaun iha rede lokál (http://localhost); seidauk iha HTTPS no autentikasaun faktór rua.",
  ]));

  // ------------------------------------------------------------------ KAPITULU II
  out.push(h1("KAPITULU II", "REVIZAUN LITERATURA"));
  out.push(h2("2.1. Definisaun Revizaun Literatura"));
  out.push(p("Revizaun literatura mak prosesu lee, avalia no rasionaliza teoria no publikasaun sientífiku ne'ebé relevante ho tópiku peskiza, hodi hatene saida mak ema halo ona no pozisiona peskiza ida ne'e iha gap koñesimentu. Iha estudu ida ne'e, revizaun literatura foka ba:"));
  out.push(...bullets([
    "Konseitu Big Data, data streaming, Apache Kafka, Apache Spark, FastAPI no Streamlit.",
    "Anomaly detection no tipu anomalia iha rede eletrisidade.",
    "Algoritmu machine learning Random Forest (no Decision Tree nu'udar nia komponente).",
    "Métrika evaluasaun ba dadus la balansu (Precision, Recall, F1-score, PR-AUC, ROC-AUC).",
    "Estudu empíriku kona-ba analítika smart meter no deteksaun na'ok eletrisidade.",
  ]));
  out.push(h3("2.1.1. Objetivu Revizaun Literatura"));
  out.push(...numbers([
    "**Identifika gap peskiza** – deteksaun anomalia ne'ebé sei reativu no manuál.",
    "**Justifika algoritmu** – tanba sa Random Forest loos ba anomalia.",
    "**Justifika teknolojia Big Data** – tanba sa uza Kafka, Spark no data lake.",
    "**Fornese baze sientífiku** ba tipu anomalia no métrika evaluasaun.",
  ]));
  out.push(h3("2.1.2. Baze Teórika (Theoretical Basis)"));
  out.push(p("Seksaun ida ne'e esplika teoria sira ne'ebé sai baze ba sistema PowerPredict TL: Big Data, ingestaun no prosesamentu dadus, arkitetura Lambda, anomaly detection, algoritmu machine learning, métrika evaluasaun no tools."));

  out.push(h3("2.1.3. Big Data"));
  out.push(p("Big Data mak dadus ne'ebé nia volume, velocity ka variety liu kapasidade komputadór ida no tools tradisionál, entaun servisu tenke fahe ba prosesu ka komputadór barak (Laney, 2001). Tabela tuir mai hatudu \"V\" lima iha PowerPredict TL:"));
  out.push(...table(["V", "Signifikadu", "Iha PowerPredict TL"], [
    ["Volume", "Totál dadus", `${fmt(rows / 1e6, 1)} millaun records, ${fmt(totalGB, 1)} GB CSV (5 GB · 10 GB · 20 GB); readings live ${fmt(LV.n / 1e6, 2)} millaun iha MySQL`],
    ["Velocity", "Velosidade dadus tama", "Meters 281 haruka reading kada 2 segundu (~140 mensajen/s) liuhusi FastAPI ba Kafka"],
    ["Variety", "Formatu oin-oin", "CSV, JSON (REST API + Kafka), Parquet, MySQL table"],
    ["Veracity", "Kualidade dadus", "Missing value, duplicate no invalid value deteta no hamoos husi Spark"],
    ["Value", "Valór ba utilizadór", "Alerta anomalia, lista suspeita Na'ok Eletrisidade, dashboard ho login ba EDTL"],
  ], [14, 26, 60], { caption: "Karakterístika 5V Big Data iha PowerPredict TL", firstColBold: true }));

  out.push(h3("2.1.4. Data Streaming no Apache Kafka"));
  out.push(p("Data streaming mak prosesa dadus kontinuamente bainhira dadus to'o. Apache Kafka mak plataforma *distributed messaging* ne'ebé dezenvolve iha LinkedIn ba log ho volume boot (Kreps et al., 2011). *Producer* haruka mensajen ba *topic*; topic fahe ba *partition* hodi bele prosesa paralelu; kada mensajen iha *offset* ne'ebé permite *consumer* kontinua husi fatin ne'ebé nia para. Kafka 3.9 iha peskiza ne'e hala'o iha modu **KRaft** (la presiza ZooKeeper)."));
  out.push(h3("2.1.5. Apache Spark"));
  out.push(p("Apache Spark mak engine unifikadu ba prosesamentu Big Data ne'ebé fahe dadus ba *partition* no prosesa paralelu iha core ka node barak (Zaharia et al., 2016). Spark fornese DataFrame no Spark SQL, **Spark MLlib** ba machine learning distribuídu (Meng et al., 2016), no **Structured Streaming** ho modelu micro-batch, window no **watermark** ba dadus ne'ebé tarde (Armbrust et al., 2018). Pandas *scale up* (komputadór ida, dadus hotu tenke tama iha RAM), enkuantu Spark *scale out* (dadus fahe ba partition no core barak; partition ne'ebé la tama iha RAM bele *spill* ba disk)."));
  out.push(h3("2.1.6. Arkitetura Lambda"));
  out.push(p("Arkitetura Lambda (Marz & Warren, 2015) kombina **batch layer** ne'ebé prosesa dadus istóriku tomak no **speed layer** ne'ebé prosesa dadus foun iha tempu real. Rezultadu kamada rua ne'e kombina iha **serving layer** (iha ne'e MySQL) ne'ebé dashboard lee. Iha PowerPredict TL, batch layer treina model Random Forest no speed layer uza model ne'e ba kada reading."));
  out.push(h3("2.1.7. Data Lake no Formatu Parquet"));
  out.push(p("Data lake mak repozitóriu ne'ebé rai dadus iha formatu orijinál (raw) no dadus ne'ebé prosesa ona (curated). Data lake PowerPredict TL iha zona tolu: *raw zone* (CSV), *curated zone* (Parquet husi Spark ETL, partition tuir fulan) no *stream zone* (Parquet husi readings real-time). Parquet mak formatu columnar ne'ebé kompresa no lalais ba analítika. Tanba dataset sira boot (CSV ~37 GB), data lake rai iha disk D: (konfigurasaun `PP_DATA_DIR` iha `.env`)."));
  out.push(h3("2.1.8. FastAPI, Streamlit no Autentikasaun"));
  out.push(p("**FastAPI** mak web framework Python ba REST API ne'ebé uza *type hints* no Pydantic hodi valida dadus automatikamente no kria dokumentasaun OpenAPI (/docs). Iha sistema ne'e FastAPI sai *gateway* entre bot smart meter no Kafka. **Streamlit** mak framework Python ne'ebé transforma script analítika ba website interativu la presiza HTML/JavaScript; tan ne'e implementasaun website fasil no lalais. Estrutura dashboard tuir projetu capstone referénsia (alerta → deskritiva → tendénsia → estatístika → machine learning → real-time)."));
  out.push(p("Website ne'ebé hatudu dadus kliente no funsaun hanesan simulasaun ka retrain model presiza **autentikasaun** (sé mak uza) no **autorizasaun** (saida mak nia bele halo). Password la rai nu'udar testu, maibé nu'udar *hash* ho salt no funsaun derivasaun ne'ebé neineik hanesan PBKDF2 (Moriarty et al., 2017), hodi password la fasil atu adivinha maske database lakon. *Role-based access control* fahe asesu tuir knaar: User (operadór) haree no analiza, Admin jere sistema."));

  out.push(h3("2.1.9. Data Analytics"));
  out.push(p("Data analytics mak prosesu analiza dadus hodi hetan informasaun ne'ebé ajuda halo desizaun. Manual Capstone Project (seksaun 5.5) husu nivel analítika tolu no nivel avansadu ida:"));
  out.push(...bullets([
    "**Descriptive Analytics** – saida mak akontese: totál, média, persentajen no distribuisaun (ezemplu readings, meters no Anomaly Rate kada municipality).",
    "**Trend Analysis** – oinsá dadus muda tuir tempu: oras, fulan ka janela minutu (ezemplu anomalia kada minutu iha stream).",
    "**Statistical Analysis** – distribuisaun no relasaun entre variavel: média, percentile (p05, p50, p95) tuir grupu.",
    "**Advanced Analytics** – Classification, Anomaly Detection no Prediction ho machine learning (iha ne'e Random Forest).",
  ]));
  out.push(p("Iha Big Data, agregasaun sira ne'e hala'o distribuídu ho Spark (groupBy, window, percentile_approx) tanba dadus la tama iha memória komputadór ida (Zaharia et al., 2016)."));

  out.push(h3("2.1.10. Anomaly Detection"));
  out.push(p("Chandola et al. (2009) define anomalia nu'udar padraun iha dadus ne'ebé la tuir nosaun komportamentu normál, no fahe ba tipu tolu: *point anomaly* (reading ida la normál, ezemplu voltajen 185 V), *contextual anomaly* (reading normál iha kontestu ida maibé la normál iha kontestu seluk, ezemplu konsumu 0.1 kW normál iha madrugada maibé suspeitu iha 20:00) no *collective anomaly* (sekuénsia readings, ezemplu na'ok durante semana). Métodu deteksaun iha rua: **supervised** — model aprende husi ezemplu ho label (normal no tipu anomalia) — no **unsupervised** — model buka pontu izoladu la ho label. Supervised presiza label maibé bele fó **tipu** anomalia; unsupervised la presiza label maibé dala barak hetan alarme falsu no la hatene tipu."));
  out.push(h3("2.1.11. Tipu Anomalia iha Rede Eletrisidade"));
  out.push(p("Limite normál voltajen no frekuénsia tuir padraun EN 50160 no IEC 60038 (voltajen 220 V ±10%, frekuénsia 50 Hz). Tipu anomalia neen ne'ebé sistema deteta mak:"));
  out.push(...table(["Tipu anomalia", "Saida mak akontese", "Sinál iha dadus", "Se mak tenke halo"], [
    [tt("Voltage Sag"), "Voltajen feeder tun 14–35 V (avaria, transformadór sobrekarga, liña naruk)", "Voltage deviation < −6% ... −10%", "Operadór rede / subestasaun"],
    [tt("Over-Voltage"), "Voltajen sa'e > +10% iha kalan bainhira karga ki'ik", "Voltage deviation > +10%", "Operadór rede"],
    [tt("Frequency Deviation"), "Jeradór trip ka load rejection iha sentrál", "Frequency fora husi 49.5–50.5 Hz", "Sentrál elétrika"],
    [tt("Overload"), "Kliente uza liu kVA kontratu", "Load ratio > 1.0", "Servisu kliente"],
    [tt("Energy Theft"), "Bypass meter: rejista de'it 5–55% husi konsumu loos", "Baseline ratio ki'ik", "Ekipa inspesaun"],
    [tt("Meter Fault"), "Sensor korrente sala durante loron 1–7", "Consistency ratio P/(V·I·PF) ≠ 1", "Manutensaun meter"],
  ], [18, 34, 26, 22], { caption: "Tipu anomalia neen ne'ebé PowerPredict TL deteta", firstColBold: true, size: 18 }));

  out.push(h3("2.1.12. Decision Tree"));
  out.push(p("Decision Tree fahe dadus ho pergunta \"se/entaun\" (ezemplu: voltage deviation < −6%?) to'o folla ho klase ida (Quinlan, 1986; Breiman et al., 1984). Kada split hili ho Entropy (Information Gain) ka Gini Impurity. Fasil interpreta, maibé tree ida bele *overfit* no sensitivu ba noise."));
  out.push(h3("2.1.13. Random Forest (RF)"));
  out.push(p("Random Forest mak algoritmu *ensemble learning* ne'ebé konstrói decision tree barak no kombina rezultadu liuhusi votasaun (Breiman, 2001)."));
  out.push(...bullets([
    "**Mekanizmu:** kada tree treina ho bootstrap sample (*bagging*); iha kada node, tree haree de'it feature aleatóriu balun (*random feature selection*) no hili split ho Gini Impurity ki'ik liu.",
    "**Vantajen ba anomalia:** kaptura relasaun la-lineár no interasaun entre feature (ezemplu konsumu ki'ik *iha oras pico*), robustu ba noise sensor, simu feature numériku no kategóriku la presiza normaliza, fó probabilidade (votu) ne'ebé bele uza nu'udar Anomaly Score, no fó Feature Importance.",
    "**Class weight:** iha dadus la balansu, peza klase ida (iha ne'e Normal) bele aumenta hodi model la fó alarme falsu barak — balansu entre Precision no Recall.",
    "**Dezvantajen:** presiza label (ground truth), model boot liu (tree 150) no susar liu atu interpreta kompara ho tree ida.",
  ]));
  out.push(h3("2.1.14. Tanba sa Random Forest?"));
  out.push(p(`Peskiza ida ne'e uza métodu machine learning ida de'it: **Random Forest**. Razaun mak: (1) dadus iha ground truth label, entaun supervised learning bele fó **tipu** anomalia; (2) anomalia depende ba feature barak hamutuk (ezemplu power ki'ik *kompara ho baseline* iha oras pico), no tree sira aprende relasaun non-linear; (3) votu tree barak fó probability ne'ebé sai Anomaly Score; (4) robustu ba noise sensor no la presiza normaliza; (5) literatura hatudu família Random Forest mak klasifikadór di'ak liu (Fernández-Delgado et al., 2014). Iha komparasaun empíriku ho Decision Tree, Logistic Regression no Isolation Forest iha test set hanesan (dataset 20 GB), Random Forest mós mak accurate liu (F1 ${rf.anomaly_f1.toFixed(3)}); rezultadu kompletu iha seksaun 5.3.`));
  out.push(h3("2.1.15. Métrika Evaluasaun ba Dadus La Balansu"));
  out.push(p(`Iha dataset ne'e kuaze ${(100 - Number(anomPct)).toFixed(0)}% readings Normal. Model ne'ebé dehan \"Normal\" ba buat hotu atinje Accuracy ${(100 - Number(anomPct)).toFixed(1)}% maibé la deteta anomalia ida. Tan ne'e Accuracy la to'o; peskiza ne'e uza Precision, Recall, F1-score no **PR-AUC** ne'ebé informativu liu ROC iha dadus la balansu (Saito & Rehmsmeier, 2015). TP = anomalia ne'ebé deteta loos, FP = alarme falsu, FN = anomalia ne'ebé lakon, TN = normál loos.`));
  out.push(...table(["Métrika", "Definisaun / Objetivu"], [
    ["Precision", "Husi alarme hotu, hira mak anomalia loos (hamenus alarme falsu)."],
    ["Recall", "Husi anomalia loos hotu, hira mak model hetan."],
    ["F1-Score", "Média armóniku Precision no Recall."],
    ["PR-AUC (Average Precision)", "Área iha kurva Precision–Recall ba threshold hotu; la depende ba threshold ida."],
    ["ROC-AUC", "Probabilidade model fó score aas liu ba anomalia kompara ho normál."],
    ["False Positive Rate", "Persentajen readings normál ne'ebé sai alarme falsu: FP/(FP+TN)."],
    ["Macro F1 + Confusion Matrix", "F1 média ba klase 7 (tipu anomalia) no tabela prediction vs atuál."],
  ], [28, 72], { caption: "Métrika evaluasaun deteksaun anomalia", firstColBold: true }));
  out.push(eq([mr("Precision="), frac(mr("TP"), mr("TP+FP")), mr("    Recall="), frac(mr("TP"), mr("TP+FN")), mr("    F1="), frac(mr("2×Precision×Recall"), mr("Precision+Recall"))], "2.1"));
  out.push(eq([mr("FPR="), frac(mr("FP"), mr("FP+TN")), mr("    Macro F1="), frac(mr("1"), mr("K")), sum(sub(mr("F1"), mr("k")), mr("k=1"), mr("K"))], "2.2"));

  out.push(h3("2.1.16. Tools no Linguajen Programasaun"));
  out.push(p("Sistema PowerPredict TL dezenvolve iha **Visual Studio Code** ho teknolojia sira tuir mai:"));
  out.push(...bullets([
    "**Python 3.11** – jeradór dadus, bot, FastAPI, job Spark (PySpark), machine learning no website.",
    "**FastAPI 0.162 + Uvicorn** – REST API gateway ho validasaun Pydantic no dokumentasaun OpenAPI.",
    "**Apache Kafka 3.9.1** – data ingestion stream; topic `smart-meter-readings` ho partition 3.",
    "**Apache Spark 3.5.3 (PySpark)** – batch ETL, stream micro-batch no Spark MLlib.",
    "**scikit-learn 1.9** – Random Forest no métrika evaluasaun (Pedregosa et al., 2011).",
    "**Streamlit 1.63 + Altair** – website dashboard: login, pájina 9, Refresh, Night mode, ikon Material.",
    `**XAMPP (MariaDB/MySQL 10.4 + phpMyAdmin)** – database serving \`powerpredict_db\` ho table ${F.tables.length}.`,
    "**Apache Arrow / Parquet** – formatu data lake.",
  ]));
  out.push(p(`MySQL mak *Relational Database Management System* (RDBMS) open-source. Imajen tuir mai hatudu database \`powerpredict_db\` iha phpMyAdmin ho table ${F.tables.length}:`));
  out.push(...figure("ui_phpmyadmin.png", "Database MySQL powerpredict_db iha phpMyAdmin (XAMPP)"));

  out.push(h2("2.2. Peskiza Anterior"));
  out.push(p("Peskiza husi (Wang et al., 2019), \"Review of Smart Meter Data Analytics: Applications, Methodologies, and Challenges\", revee analítika dadus smart meter ba análize karga, prediksaun karga, jestaun karga no **deteksaun anomalia**, no identifika dezafiu Big Data no privasidade dadus. Revizaun ne'e sai baze atu uza dadus smart meter ho teknolojia Big Data."));
  out.push(p("Peskiza daruak husi (Jokar et al., 2016), \"Electricity Theft Detection in AMI Using Customers' Consumption Patterns\", dezenvolve detetór na'ok eletrisidade bazeia ba padraun konsumu kliente iha AMI. Meter transformadór identifika área ho risku aas, depois anomalia konsumu identifika kliente suspeitu. Eksperimentu ho dadus reál kliente 5,000 hatudu dezempeñu aas. Ida ne'e hatudu katak padraun konsumu kompara ho normál (hanesan `baseline_ratio` iha peskiza ne'e) mak sinál xave ba na'ok."));
  out.push(p("Peskiza datoluk husi (Glauner et al., 2017), \"The Challenge of Non-Technical Loss Detection Using Artificial Intelligence: A Survey\", revee métodu deteksaun perda non-técnika. Autór sira hatudu katak NTL bele to'o 40% husi eletrisidade distribuída iha nasaun balun, no fahe métodu ba *expert system* no *machine learning*; dezafiu prinsipál mak klase la balansu no label ne'ebé limitadu."));
  out.push(p("Peskiza dahaat husi (Buzau et al., 2019), \"Detection of Non-Technical Losses Using Smart Meter Data and Supervised Learning\", propoin métodu supervised learning ba NTL ne'ebé uza dadus smart meter no informasaun auxiliar (lokalizasaun no karakterístika tékniku meter), testa ho dadus reál kliente industriál no komersiál Endesa. Estudu ne'e suporta abordajen supervised ho feature husi meter master hanesan iha PowerPredict TL."));
  out.push(p(`Peskiza dalimak husi (Liu et al., 2008), \"Isolation Forest\", propoin métodu unsupervised ne'ebé izola anomalia ho tree aleatóriu, ho complexity lineár no memory ki'ik. Iha komparasaun peskiza ida ne'e (test set 20 GB), Isolation Forest atinje F1 ${IF.f1.toFixed(3)} de'it, tan ne'e la uza iha sistema.`));
  out.push(p("Peskiza daneen husi (Fernández-Delgado et al., 2014), \"Do we Need Hundreds of Classifiers to Solve Real World Classification Problems?\", avalia klasifikadór 179 iha dataset 121; família Random Forest mak di'ak liu, atinje 94.1% husi accuracy máximu. Tanba kritika kona-ba bias, peskiza ida ne'e uza test set husi meters separadu."));
  out.push(p("Peskiza dahitu mak aprezentasaun individuál Reinaldo N. M. Alves Costa (Costa, 2026a), \"Why and When Should We Move from Traditional to Big Data Processing?\", ne'ebé kompara Pandas ho Apache Spark iha dadus 100 MB, 1 GB no 5 GB. Peskiza ida ne'e uza métodu eskala dadus no métodu sukat hanesan, maibé ba dadus eletrisidade no ho volume boot liu (5 GB, 10 GB no 20 GB)."));
  out.push(...table(["No", "Autór (Tinan)", "Foku", "Métodu", "Rezultadu prinsipál"], [
    ["1", "Wang et al. (2019)", "Analítika smart meter", "Revizaun", "Aplikasaun inklui deteksaun anomalia; dezafiu Big Data"],
    ["2", "Jokar et al. (2016)", "Na'ok eletrisidade", "Padraun konsumu", "Dezempeñu aas ho kliente 5,000"],
    ["3", "Glauner et al. (2017)", "Perda non-técnika", "Survey", "NTL to'o 40%; klase la balansu"],
    ["4", "Buzau et al. (2019)", "Deteksaun NTL", "Supervised learning", "Dadus smart meter + auxiliar (Endesa)"],
    ["5", "Liu et al. (2008)", "Anomaly detection", "Isolation Forest", "Unsupervised, complexity lineár"],
    ["6", "Fernández-Delgado et al. (2014)", "Klasifikadór 179", "Komparasaun", "Família RF di'ak liu"],
    ["7", "Costa (2026a)", "Pandas vs Spark", "Benchmark 100 MB–5 GB", "Spark lalais liu iha dadus boot"],
  ], [5, 23, 20, 20, 32], { caption: "Rezumu peskiza anterior", size: 18 }));
  return out;
};
