// Kapitulu VI, Bibliografia, Anexu - datasets 5 GB / 10 GB / 20 GB, website with login
const L = require("./lib");
const { p, pn, h1, h1plain, h2, bullets, numbers, table, figure, fmt, pct, d, tt } = L;

module.exports = function (F) {
  const rf = F.models.find((m) => m.engine === "scikit-learn" && m.is_active) || F.models.find((m) => m.algorithm === "random_forest");
  const ml = F.models.filter((m) => m.engine === "spark-mllib").slice(-1)[0];
  const B = (vol, eng) => F.benchmarks.find((b) => b.vol === vol && b.engine === eng);
  const s20 = B("20GB", "spark");
  const CM = F.compare.models;
  const S = F.stream, LV = F.live, PC = rf.class_report.per_class;
  const ETL20 = F.runs.find((r) => r.job_name === "spark_batch_etl" && r.dataset === "20GB");
  const checks = F.web_checks || [];
  const fi = Object.entries(rf.feature_importance);
  const out = [];

  out.push(h1("KAPITULU VI", "KONKLUZAUN NO REKOMENDASAUN"));
  out.push(h2("6.1. Konkluzaun"));
  out.push(p("Bazeia ba rezultadu peskiza no dezenvolvimentu Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest, hakerek-na'in sira foti konkluzaun tuir mai:"));
  out.push(...numbers([
    `Sistema Big Data implementa ho susesu pipeline kompletu tuir manual Capstone Project: Data Collection (bot smart meter, Open-Meteo, jeradór dataset) → Data Ingestion (FastAPI + Apache Kafka) → Data Storage (Data Lake Parquet + MySQL) → Data Processing (Spark batch ETL no Spark Streaming ho window no watermark) → Machine Learning (Random Forest) → Visualization (website Streamlit ho login no pájina 9) → Real-Time Application (Anomaly Score, anomaly events no simulasaun).`,
    `Métodu eskala dadus husi aprezentasaun individuál aplika ho susesu: seed reál (UCI no Open-Meteo) eskala ba dataset 5 GB, 10 GB no 20 GB ho records totál ${fmt(F.datasets.reduce((a, x) => a + x.rows, 0))} ho events anomalia neen no ground truth label. Spark ETL prosesa dataset 20 GB (records ${fmt(ETL20.input_rows / 1e6, 1)} millaun) iha ${ETL20.seconds.toFixed(0)} segundu.`,
    `Random Forest — métodu machine learning ida de'it iha sistema — deteta anomalia ho Precision ${rf.anomaly_precision.toFixed(3)}, Recall ${rf.anomaly_recall.toFixed(3)}, F1-score ${rf.anomaly_f1.toFixed(3)} no PR-AUC ${rf.pr_auc.toFixed(3)} iha meters ne'ebé model nunka haree, no klasifika tipu anomalia ho Macro F1 ${rf.f1_macro.toFixed(3)}. Iha test set hanesan, nia accurate liu Decision Tree (F1 ${CM["Decision Tree"].f1.toFixed(3)}), Logistic Regression (F1 ${CM["Logistic Regression"].f1.toFixed(3)}) no Isolation Forest (F1 ${CM["Isolation Forest"].f1.toFixed(3)}). Spark MLlib Random Forest ne'ebé treina iha records ${fmt(ml.n_train)} no test iha records ${fmt(ml.n_test)} atinje F1 ${ml.anomaly_f1.toFixed(3)}, hatudu katak métodu ne'e bele hala'o iha eskala Big Data.`,
    `Random Forest fit ho dadus tanba dadus iha ground truth label, anomalia depende ba kombinasaun feature (non-linear), no votu tree 150 fó Anomaly Score ne'ebé estável (cross-validation F1 ${rf.class_report.cv.anomaly_f1_mean.toFixed(3)} ± ${rf.class_report.cv.anomaly_f1_std.toFixed(3)}). Class weight Normal = ${rf.params.normal_weight} hamenus alarme falsu: Precision sa'e husi ${CM["Random Forest #11 (20GB)"].precision.toFixed(3)} ba ${rf.anomaly_precision.toFixed(3)}.`,
    `Feature **${L.fl(fi[0][0])}** (${fi[0][1].toFixed(2)}%) no **${L.fl(fi[1][0])}** (${fi[1][1].toFixed(2)}%) mak importante liu. Feature engineering ho koñesimentu domíniu (baseline ratio no consistency ratio) mak xave ba deteksaun ${tt("Energy Theft")} no ${tt("Meter Fault")}. ${tt("Energy Theft")} mak tipu susar liu (F1 ${PC["Energy Theft"].f1.toFixed(2)}).`,
    `Scalability testing hatudu katak Pandas lalais liu ba dadus ki'ik (10k–1M records), maibé iha komputadór RAM 32 GB Pandas la konsege prosesa dataset 5 GB, 10 GB no 20 GB (out of memory). Apache Spark prosesa 20 GB iha ${s20.avg_s.toFixed(0)} segundu ho memory ${fmt(s20.mem_mb / 1024, 0)} GB de'it; tan ne'e ba dataset 5 GB ba leten, Spark mak tools ida de'it ne'ebé bele uza.`,
    `Sistema real-time prosesa mensajen ${fmt(Number(S.msgs))} husi meters 281 ho latency média ${(S.lat_ms / 1000).toFixed(1)} segundu no F1 ${LV.rf.f1.toFixed(3)} iha stream. Website Streamlit ho login Admin / User (password hash PBKDF2, cookie sesaun), butaun Refresh no Night mode — Dashboard, Monitor Real-Time, Anomaly Events, Deteksaun Manual, Model Random Forest, Big Data Pipeline, Simulasaun, Jestaun Uzuáriu no Metodolojia — passa teste website ${checks.filter((c) => c.ok).length} husi ${checks.length}, no bele ajuda EDTL deteta problema rede no suspeita na'ok eletrisidade sedu.`,
  ]));
  out.push(h2("6.2. Rekomendasaun"));
  out.push(pn("Rekomendasaun ba futuru:"));
  out.push(...numbers([
    "**Integrasaun Dadus Reál EDTL:** kolabora ho EDTL hodi hetan dadus smart meter reál ne'ebé anonimizadu no rezultadu inspesaun (label reál ba Na'ok Eletrisidade no Kontador Aat).",
    "**Feature Tempu Naruk:** aumenta feature istóriku meter (ezemplu média semana 4 ikus, tendénsia konsumu) hodi hadi'a deteksaun Na'ok Eletrisidade.",
    "**Cluster no Structured Streaming:** implementa iha cluster (Spark node barak, Kafka broker 3 ho replication) ka iha Linux/cloud hodi uza Structured Streaming ho checkpoint.",
    "**Retraining Automátiku:** treina fali Random Forest kada fulan ho dadus foun no monitoriza *model drift*.",
    "**Notifikasaun:** haruka events severity aas liuhusi SMS ka WhatsApp ba tékniku EDTL.",
    "**Edge Computing:** hala'o model iha subestasaun ka gateway lokál hodi kontinua funsiona maske internet mate.",
    "**Seguransa Website:** uza HTTPS, autentikasaun faktór rua no log auditoria ba asaun Admin bainhira sistema hala'o iha rede EDTL.",
  ]));

  // ---------------------------------------------------------------- BIBLIOGRAFIA
  out.push(h1plain("BIBLIOGRAFIA"));
  const refs = [
    "Armbrust, M., Das, T., Torres, J., Yavuz, B., Zhu, S., Xin, R., Ghodsi, A., Stoica, I., & Zaharia, M. (2018). Structured Streaming: A declarative API for real-time applications in Apache Spark. *Proceedings of the 2018 International Conference on Management of Data (SIGMOD '18)*, 601–613. https://doi.org/10.1145/3183713.3190664",
    "Breiman, L. (2001). Random forests. *Machine Learning, 45*(1), 5–32. https://doi.org/10.1023/A:1010933404324",
    "Breiman, L., Friedman, J. H., Olshen, R. A., & Stone, C. J. (1984). *Classification and regression trees*. Wadsworth.",
    "Buzau, M. M., Tejedor-Aguilera, J., Cruz-Romero, P., & Gómez-Expósito, A. (2019). Detection of non-technical losses using smart meter data and supervised learning. *IEEE Transactions on Smart Grid, 10*(3), 2661–2670. https://ieeexplore.ieee.org/document/8299442",
    "Chandola, V., Banerjee, A., & Kumar, V. (2009). Anomaly detection: A survey. *ACM Computing Surveys, 41*(3), Article 15. https://doi.org/10.1145/1541880.1541882",
    "Costa, R. N. M. A. (2026a). *Why and when should we move from traditional to big data processing? An experimental comparison of Pandas and Apache Spark on 100 MB, 1 GB and 5 GB* [Aprezentasaun individuál, unidade kurikuler Big Data]. Dili Institute of Technology.",
    "Costa, R. N. M. A. (2026b). *Analiza preditiva ba manajementu bee sustentável no monitorizasaun analitiku distribuisaun bee uza metodu algoritmu Random Forest* [Proposta Metodolojia Peskiza]. Dili Institute of Technology.",
    "Dili Institute of Technology. (2026). *Manual capstone project: Unidade kurikuler Big Data – projetu online, streaming no real-time*. Programa Estudu Siencia Komputador.",
    "European Committee for Electrotechnical Standardization. (2010). *EN 50160: Voltage characteristics of electricity supplied by public distribution networks*. CENELEC.",
    "Fernández-Delgado, M., Cernadas, E., Barro, S., & Amorim, D. (2014). Do we need hundreds of classifiers to solve real world classification problems? *Journal of Machine Learning Research, 15*, 3133–3181.",
    "Glauner, P., Meira, J. A., Valtchev, P., State, R., & Bettinger, F. (2017). The challenge of non-technical loss detection using artificial intelligence: A survey. *International Journal of Computational Intelligence Systems, 10*(1), 760–775. https://doi.org/10.2991/ijcis.2017.10.1.51",
    "Hastie, T., Tibshirani, R., & Friedman, J. (2009). *The elements of statistical learning: Data mining, inference, and prediction* (2nd ed.). Springer.",
    "International Electrotechnical Commission. (2009). *IEC 60038: IEC standard voltages*. IEC.",
    "Jokar, P., Arianpoo, N., & Leung, V. C. M. (2016). Electricity theft detection in AMI using customers' consumption patterns. *IEEE Transactions on Smart Grid, 7*(1), 216–226. https://doi.org/10.1109/TSG.2015.2425222",
    "Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. *Proceedings of the NetDB Workshop*.",
    "Laney, D. (2001). *3D data management: Controlling data volume, velocity and variety*. META Group.",
    "Liu, F. T., Ting, K. M., & Zhou, Z.-H. (2008). Isolation forest. *Proceedings of the 8th IEEE International Conference on Data Mining (ICDM 2008)*, 413–422. https://doi.org/10.1109/ICDM.2008.17",
    "Marz, N., & Warren, J. (2015). *Big data: Principles and best practices of scalable realtime data systems*. Manning.",
    "Moriarty, K., Kaliski, B., & Rusch, A. (2017). *PKCS #5: Password-based cryptography specification version 2.1* (RFC 8018). Internet Engineering Task Force. https://doi.org/10.17487/RFC8018",
    "Meng, X., Bradley, J., Yavuz, B., Sparks, E., Venkataraman, S., Liu, D., Freeman, J., Tsai, D. B., Amde, M., Owen, S., Xin, D., Xin, R., Franklin, M. J., Zadeh, R., Zaharia, M., & Talwalkar, A. (2016). MLlib: Machine learning in Apache Spark. *Journal of Machine Learning Research, 17*(34), 1–7.",
    "Our World in Data. (2025). *Electricity mix – Timor-Leste* [Dataset, based on Ember and Energy Institute]. https://ourworldindata.org/electricity-mix",
    "Pedregosa, F., Varoquaux, G., Gramfort, A., Michel, V., Thirion, B., Grisel, O., Blondel, M., Prettenhofer, P., Weiss, R., Dubourg, V., Vanderplas, J., Passos, A., Cournapeau, D., Brucher, M., Perrot, M., & Duchesnay, É. (2011). Scikit-learn: Machine learning in Python. *Journal of Machine Learning Research, 12*, 2825–2830.",
    "Quinlan, J. R. (1986). Induction of decision trees. *Machine Learning, 1*(1), 81–106. https://doi.org/10.1007/BF00116251",
    "Saito, T., & Rehmsmeier, M. (2015). The precision-recall plot is more informative than the ROC plot when evaluating binary classifiers on imbalanced datasets. *PLoS ONE, 10*(3), e0118432. https://doi.org/10.1371/journal.pone.0118432",
    "Trindade, A. (2015). *ElectricityLoadDiagrams20112014* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C58C86",
    "Wang, Y., Chen, Q., Hong, T., & Kang, C. (2019). Review of smart meter data analytics: Applications, methodologies, and challenges. *IEEE Transactions on Smart Grid, 10*(3), 3125–3148. https://doi.org/10.1109/TSG.2018.2818167",
    "World Bank. (2025). *World development indicators: Access to electricity (% of population) – Timor-Leste* [Dataset]. https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS?locations=TL",
    "Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664",
    "Zippenfenig, P. (2023). *Open-Meteo.com weather API* [Computer software]. Zenodo. https://doi.org/10.5281/zenodo.7970649",
  ];
  refs.forEach((r) => out.push(new d.Paragraph({ children: L.runs(r), alignment: d.AlignmentType.LEFT, indent: { left: 567, hanging: 567 }, spacing: { line: 276, after: 120 } })));

  // ---------------------------------------------------------------- ANEXU
  out.push(h1plain("ANEXU"));
  out.push(h2("Anexu A. Estrutura Kódigu Sistema"));
  out.push(...table(["Folder / file", "Funsaun"], [
    ["config.py, .env, .env.example", "Konfigurasaun (MySQL, Kafka, Spark, data lake iha D:, konta website dahuluk)"],
    ["common/domain.py", "Tipu anomalia, load shape, baseline, feature engineering, evidénsia"],
    ["common/db.py, common/spark.py", `Schema table ${F.tables.length} (SQLAlchemy); SparkSession, feature Spark, Parquet writer`],
    ["data_generator/, data/seed/uci_real/", "Download temperature Open-Meteo; seed UCI; jeradór dataset 5 GB / 10 GB / 20 GB ho anomalia"],
    ["bot/bot_live.py, bot/bot_replay.py", "Data source real-time (smart meter 281) no replay dataset"],
    ["api/main.py, api/schemas.py", "FastAPI gateway: ingest, health, stats, detect"],
    ["pipeline/spark_streaming.py", "Kafka → Spark Streaming → Random Forest → MySQL"],
    ["pipeline/spark_batch_etl.py", "Spark ETL: Data Quality, cleaning, feature, analytics, training/test split"],
    ["pipeline/spark_ml_train.py, pipeline/benchmark.py", "Spark MLlib Random Forest; benchmark Pandas vs Spark (ho memory guard)"],
    ["ml/train.py, ml/predict.py", "Training (class weight) no serving Random Forest (scikit-learn)"],
    ["dashboard/app.py, auth.py, ui.py", "Website: navigation, login no sesaun (PBKDF2, cookie), Refresh, Night mode, ikon"],
    ["dashboard/views/ (file 10)", "Pájina login + pájina 9 (inklui users.py ba Jestaun Uzuáriu)"],
    ["setup/, setup_once.bat, start_all.bat", "Kria database, hala'o Kafka (check_running.ps1 hodi la hahú dala rua), instalasaun no hahú sistema"],
    ["tests/test_anomaly.py, data/samples/", "Automated test no sample CSV 100 records"],
    ["docs/report_build/", "Script relatóriu: collect_facts.py, shoot.py (screenshot + teste website), build_report.js"],
  ], [42, 58], { caption: "Estrutura kódigu sistema", size: 18 }));
  out.push(h2("Anexu B. Oinsá Hala'o Sistema"));
  out.push(...numbers([
    "Loke XAMPP Control Panel no hahú **MySQL**.",
    "Kopia `.env.example` ba `.env`; hakerek `PP_DATA_DIR` (data lake) no username / password ba konta Admin no User dahuluk (`PP_ADMIN_*`, `PP_USER_*`, password karakter 8 ka liu).",
    "Dala uluk de'it: hala'o `setup_once.bat` (instala pakote, download temperature, jera dataset 5 GB / 10 GB / 20 GB, kria database, Spark ETL no training Random Forest).",
    "Hala'o `start_all.bat`: hahú Kafka, FastAPI, Spark Streaming, bot no website. Script ne'e seguru atu hala'o dala rua — komponente ne'ebé ativu ona la hahú fali.",
    "Loke browser iha `http://localhost:8505`, login iha tab Admin ka User, no `http://127.0.0.1:8000/docs` (API).",
    "Demonstrasaun live (Admin): loke pájina Simulasaun, klik \"Voltajen Tun iha municipality\" ka \"Hahú na'ok eletrisidade\", depois haree Dashboard, Monitor Real-Time no Anomaly Events.",
    "Se janela Kafka hatudu `CorruptRecordException`: taka janela Kafka, hala'o `setup\\start_kafka.bat reset` (hamoos dadus Kafka de'it), depois hala'o fali `start_all.bat`.",
  ]));
  out.push(h2("Anexu C. Vídeo Demonstrasaun (YouTube)"));
  out.push(pn("Link vídeo demonstrasaun YouTube: ____________________________________________"));
  out.push(p("Vídeo prepara tuir manual Capstone Project seksaun 17 no 18:"));
  out.push(...table(["Komponente", "Rekezitu manual", "PowerPredict TL"], [
    ["Plataforma / Visibility", "YouTube, Unlisted ka Public", "YouTube (Unlisted ka tuir instrusaun dosente)"],
    ["Durasaun / Formatu", "10–20 minutu, MP4, 1080p rekomendadu", "Maizumenus minutu 15, MP4 1080p"],
    ["Screen Recording", "Obrigatóriu", "Grava ekran tomak (website, janela start_all, phpMyAdmin)"],
    ["Live System / Dashboard Demo", "Obrigatóriu", "start_all.bat → login Admin → Simulasaun → Dashboard / Monitor atualiza"],
    ["Source Code Demo", "Rekomendadu", "Folder bot / api / pipeline / dashboard iha VS Code"],
    ["YouTube Link", "Entrega ho relatóriu", "Hakerek iha leten"],
  ], [24, 34, 42], { caption: "Rekezitu vídeo demonstrasaun", firstColBold: true, size: 17 }));
  out.push(...table(["Parte", "Durasaun", "Konteúdu PowerPredict TL"], [
    ["1 – Introdusaun", "1–2 min", "Naran no NIM membru grupu na'in rua, títulu projetu, unidade kurikuler Big Data, problema: anomalia rede no na'ok eletrisidade"],
    ["2 – Problem & Objectives", "1–2 min", "Problem statement (seksaun 1.2), objetivu (1.4), benefísiu ba EDTL (1.5)"],
    ["3 – Big Data Architecture", "2–3 min", "Bot / Open-Meteo / UCI → FastAPI → Kafka → Spark → Data Lake + MySQL → Random Forest → Dashboard (Imajen 4.1, 4.3)"],
    ["4 – Implementasaun", "3–5 min", "Dataset 5 / 10 / 20 GB, Kafka topic, Spark Streaming, Spark ETL, Data Lake, Random Forest (scikit-learn + MLlib)"],
    ["5 – Live Demonstration", "3–5 min", "Simulasaun Voltajen Tun ka Na'ok Eletrisidade → dadus tama → Spark prosesa → Dashboard no Monitor atualiza; Pandas vs Spark"],
    ["6 – Conclusion", "1–2 min", "Rezultadu prinsipál (F1, Spark vs Pandas), limitasaun no future improvement (Kapitulu VI)"],
  ], [26, 14, 60], { caption: "Estrutura vídeo demonstrasaun", firstColBold: true, size: 17 }));

  out.push(h2("Anexu D. Mapa Relatóriu ba Avaliasaun no Learning Outcomes"));
  out.push(p("Tabela tuir mai hatudu iha ne'ebé mak relatóriu ida ne'e hatán kada komponente avaliasaun (manual seksaun 15) no kada Learning Outcome (manual seksaun 4):"));
  out.push(...table(["Komponente avaliasaun", "Peso", "Seksaun relatóriu"], [
    ["Problem & Project Proposal", "10%", "1.1–1.6"],
    ["Literature Review", "5%", "Kapitulu II"],
    ["Data Collection", "10%", "3.4, 3.5, 5.1"],
    ["Data Engineering", "15%", "3.7, 3.8, 4.3, 4.4, 5.1.2"],
    ["Kafka & Streaming", "15%", "3.6, 4.2, 4.3.2, 5.4, 5.6.2"],
    ["Spark Processing", "15%", "3.8, 4.3.1, 5.6, 5.8"],
    ["Analytics / Machine Learning", "10%", "3.9, 4.5, 5.2, 5.3"],
    ["Dashboard", "10%", "3.10, 4.6, 4.7"],
    ["Final Report", "5%", "Relatóriu ida ne'e"],
    ["Presentation & Live Demo", "5%", "Anexu B, Anexu C"],
  ], [44, 12, 44], { caption: "Komponente avaliasaun Capstone Project no seksaun relatóriu", align: [L.AlignmentType.LEFT, L.AlignmentType.CENTER, L.AlignmentType.LEFT], size: 17 }));
  out.push(...table(["LO", "Learning Outcome (manual)", "Evidénsia iha relatóriu"], [
    ["LO1", "Esplika konseitu no karakteristika Big Data", "2.1.3 (5V), 2.1.5–2.1.7"],
    ["LO2", "Identifika fonte no karakteristika dadus", "3.4, 3.5.2"],
    ["LO3", "Dezenvolve prosesu data ingestion", "3.6, 4.3.2, 4.8"],
    ["LO4", "Implementa distributed data processing", "3.8, 4.3.1, 5.6.3"],
    ["LO5", "Prosesu batch no streaming data", "4.3.1, 4.3.2, 5.4"],
    ["LO6", "Halo Big Data analytics", "3.9, 5.2, 5.3"],
    ["LO7", "Dezenvolve visualization no dashboard", "3.10, 4.6, 4.7"],
    ["LO8", "Integra teknolojia Big Data", "4.1, 4.2, 4.3.3"],
    ["LO9", "Avalia performance sistema", "5.5–5.8"],
    ["LO10", "Komunika rezultadu projetu ho profisionál", "Relatóriu, aprezentasaun no vídeo (Anexu C)"],
  ], [10, 50, 40], { caption: "Learning Outcomes no evidénsia", firstColBold: true, size: 17 }));
  return out;
};
