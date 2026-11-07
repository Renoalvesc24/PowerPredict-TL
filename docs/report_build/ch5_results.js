// Kapitulu V - Rezultadu no Diskusaun (data collection + quality, analytics, Random Forest, real-time, testing per
// manual section 14, compliance with manual section 12, discussion of the research questions)
const L = require("./lib");
const { p, pn, h1, h2, h3, bullets, numbers, table, figure, fmt, pct, AlignmentType, tt, TYPES, fl } = L;
const R = AlignmentType.RIGHT, C = AlignmentType.CENTER, LEFT = AlignmentType.LEFT;

module.exports = function (F) {
  const rf = F.models.find((m) => m.engine === "scikit-learn" && m.is_active);
  const ml = F.models.filter((m) => m.engine === "spark-mllib").slice(-1)[0];
  const CM = F.compare.models;
  const DT = CM["Decision Tree"], LR = CM["Logistic Regression"], IF = CM["Isolation Forest"];
  const rfOld = CM["Random Forest #11 (20GB)"];
  const SIZES = F.sizes, MAIN = F.main, MAINT = MAIN.replace("GB", " GB");
  const DS = Object.fromEntries(F.datasets.map((x) => [x.label, x]));
  const ETL = Object.fromEntries(F.runs.filter((r) => r.job_name === "spark_batch_etl").map((r) => [r.dataset, r]));
  const MLRUN = F.runs.filter((r) => r.job_name === "spark_mllib_train").slice(-1)[0];
  const B = (vol, eng) => F.benchmarks.find((b) => b.vol === vol && b.engine === eng);
  const S = F.stream, LV = F.live, CR = rf.class_report, PC = CR.per_class;
  const ANOM = TYPES.slice(1);
  const P = rf.params;
  const V = F.vote_example;
  const kafkaMsgs = F.kafka && F.kafka.total_messages ? fmt(F.kafka.total_messages) : null;
  const totalRows = F.datasets.reduce((a, x) => a + x.rows, 0);
  const totalGB = F.datasets.reduce((a, x) => a + x.size_mb, 0) / 1024;
  const out = [];

  out.push(h1("KAPITULU V", "REZULTADU NO DISKUSAUN"));

  // ================================================================ 5.1 data collection + quality
  out.push(h2("5.1. Rezultadu Data Collection no Data Quality"));
  out.push(h3("5.1.1. Dataset ne'ebé Rekolla"));
  out.push(p(`Jeradór produz dataset tolu ho records totál ${fmt(totalRows)} (${fmt(totalGB, 1)} GB CSV) — liu rekezitu mínimu records 10,000 iha manual. Proporsaun anomalia hanesan iha dataset tolu (maizumenus ${(100 - DS[MAIN].anomaly_pct.Normal).toFixed(1)}%), entaun rezultadu sira bele kompara.`));
  out.push(...table(["Dataset", "Tamañu", "Records", "Meters", "Part-file", "% anomalia", "Tempu jera"], SIZES.map((k) => {
    const x = DS[k];
    return [k, `${fmt(x.size_mb / 1024, 2)} GB`, fmt(x.rows), fmt(x.meters), x.parts, `${(100 - x.anomaly_pct.Normal).toFixed(2)}%`, `${x.seconds} s`];
  }), [12, 14, 18, 12, 12, 14, 18], { caption: "Dataset big data ne'ebé jera", align: [LEFT, R, R, R, R, R, R], size: 18 }));

  out.push(h3("5.1.2. Data Quality no Data Cleaning"));
  const Q = F.quality[MAIN];
  const sumQ = (k) => Q.filter((q) => q.check_name === k).reduce((a, q) => a + q.n_rows, 0);
  const r1 = ETL[MAIN];
  out.push(p(`Spark verifika dataset ${MAINT} (records ${fmt(r1.input_rows)}) no hetan problema Data Quality tuir mai:`));
  out.push(...table(["Check", "Column", "Records", "%", "Asaun"], Q.map((q) => [q.check_name, q.column_name, fmt(q.n_rows), q.pct.toFixed(3) + "%", "Hamoos"]),
    [17, 33, 15, 12, 23], { caption: `Rezultadu verifikasaun Data Quality (dataset ${MAINT})`, align: [LEFT, LEFT, R, R, LEFT], size: 18 }));
  out.push(p(`Missing value ${fmt(sumQ("missing"))}, duplicate ${fmt(sumQ("duplicate"))} no invalid value ${fmt(sumQ("invalid"))} (readings ida bele iha problema liu ida). Rezultadu cleaning ba dataset tolu:`));
  out.push(...table(["Dataset", "Records input", "Records moos", "Hamoos", "%"], SIZES.map((k) => {
    const r = ETL[k];
    return [k, fmt(r.input_rows), fmt(r.output_rows), fmt(r.input_rows - r.output_rows), ((r.input_rows - r.output_rows) / r.input_rows * 100).toFixed(2) + "%"];
  }), [16, 22, 22, 22, 18], { caption: "Rezultadu Data Cleaning ba dataset tolu", align: [LEFT, R, R, R, R], size: 18 }));

  const O = F.outliers;
  if (O) {
    out.push(h3("5.1.3. Outliers"));
    out.push(p(`Manual seksaun 14.C husu verifika outliers. Regra IQR (valór < Q1 − 1.5·IQR ka > Q3 + 1.5·IQR) aplika ba sample test natural (readings ${fmt(O.rows)}, dataset ${MAINT}):`));
    out.push(...table(["Feature", "Limite ki'ik", "Limite aas", "Outliers", "% readings", "% ne'ebé anomalia loos"], O.features.map((f) => [
      fl(f.feature), f.low.toFixed(2), f.high.toFixed(2), fmt(f.n), f.pct.toFixed(2) + "%", f.n ? f.anom_share.toFixed(1) + "%" : "–",
    ]), [24, 14, 14, 14, 14, 20], { caption: "Outliers tuir regra IQR (test sample natural)", align: [LEFT, R, R, R, R, R], size: 17 }));
    const caught = ANOM.map((a) => `${tt(a)} ${O.per_type_caught[a].toFixed(0)}%`).join(", ");
    out.push(p(`Readings ${O.any_pct.toFixed(1)}% iha outlier iha feature ida ka liu, maibé husi outliers sira ne'e ${O.any_anom_share.toFixed(1)}% de'it mak anomalia loos — barak liu mak readings normál husi kliente boot (komersiál, indústria) ka oras pico, tanba distribuisaun konsumu la simétriku. Tan ne'e outliers **la hamoos**: sira la'ós erru dadus (erru loos — valór imposível — hamoos ona nu'udar invalid). Regra IQR nu'udar detetór sei fó alarme falsu barak (precision ~${(O.any_anom_share / 100).toFixed(2)} kompara ho Random Forest ${rf.anomaly_precision.toFixed(2)}) no hetan ${O.anom_caught.toFixed(1)}% husi anomalia hotu (${caught}): Na'ok Eletrisidade no Kontador Aat la mosu nu'udar outlier tanba sira-nia valór medidu parese normál. Ida ne'e hatudu tanba sa presiza model ne'ebé haree feature barak hamutuk (Random Forest), la'ós threshold ba feature ida.`));
  }

  // ================================================================ 5.2 analytics
  out.push(h2(`5.2. Rezultadu Data Analytics (Dataset ${MAINT})`));
  out.push(h3("5.2.1. Analítika Deskritiva"));
  const anomTypes = ANOM;
  const topType = (m) => anomTypes.reduce((best, a) => ((m[a] || 0) > (m[best] || 0) ? a : best), anomTypes[0]);
  const allTot = F.muni.reduce((a, m) => a + m.total, 0), allNorm = F.muni.reduce((a, m) => a + m.Normal, 0);
  out.push(p(`Husi readings moos ${fmt(allTot)}, ${((1 - allNorm / allTot) * 100).toFixed(2)}% mak anomalia. Tabela tuir mai hatudu readings no Anomaly Rate kada municipality (orden tuir Anomaly Rate):`));
  out.push(...table(["Municipality", "Readings", "Anomalia", "Anomaly Rate", "Tipu barak liu"], F.muni.map((m) => [
    m.municipality, fmt(m.total), fmt(m.total - m.Normal), m.pct_anom.toFixed(2) + "%", tt(topType(m)),
  ]), [22, 20, 18, 16, 24], { caption: "Analítika deskritiva kada municipality", align: [LEFT, R, R, R, LEFT], size: 17 }));
  const mHi = F.muni[0], mLo = F.muni[F.muni.length - 1];
  out.push(p(`${mHi.municipality} iha Anomaly Rate aas liu (${mHi.pct_anom.toFixed(2)}%) no ${mLo.municipality} ki'ik liu (${mLo.pct_anom.toFixed(2)}%). Dili iha readings barak liu tanba meters barak, maibé nia Anomaly Rate ki'ik tanba liña distribuisaun badak no voltage drop ki'ik; municipality ho rede naruk ka sistema rasik (Atauro, Oecusse) iha Voltajen Tun no Frekuénsia La Estável barak liu.`));
  out.push(h3("5.2.2. Analítika Tendénsia"));
  out.push(...table(["Tipu anomalia", "Oras pico", "Esplikasaun"], [
    [tt("Voltage Sag"), `${F.hour_peak["Voltage Sag"]}:00`, "Karga aas iha kalan (oras pico) → voltage drop iha feeder naruk"],
    [tt("Over-Voltage"), `${F.hour_peak["Over-Voltage"]}:00`, "Madrugada, karga ki'ik → voltage sa'e"],
    [tt("Frequency Deviation"), `${F.hour_peak["Frequency Deviation"]}:00`, "Events generator trip (la depende maka'as ba oras)"],
    [tt("Overload"), `${F.hour_peak.Overload}:00`, "Kalan, uma-kain uza ekipamentu barak"],
    [tt("Energy Theft"), `${F.hour_peak["Energy Theft"]}:00`, "Episódiu naruk (loron to'o semana) → mosu iha oras hotu"],
    [tt("Meter Fault"), `${F.hour_peak["Meter Fault"]}:00`, "Episódiu loron 1–7 → mosu iha oras hotu"],
  ], [26, 14, 60], { caption: "Tendénsia: oras ho anomalia barak liu kada tipu", firstColBold: true, size: 18 }));
  out.push(...figure("ui_bigdata_batch.png", `Anomalia tuir oras, fulan no municipality, feature profile no top meters (Spark, ${MAINT})`, { maxHeight: 720 }));
  out.push(h3("5.2.3. Analítika Estatístika"));
  const prof = (a, f) => { const r = F.profile.find((x) => x.anomaly_type === a && x.feature === f); return r ? r.p50 : null; };
  const PF = ["baseline_ratio", "load_ratio", "voltage_dev_pct", "freq_dev_hz", "consistency_ratio"];
  out.push(p("Mediana (p50, Spark `percentile_approx`) feature prinsipál kada tipu anomalia hatudu tanba sa feature engineering fó sinál:"));
  out.push(...table(["Tipu", ...PF.map(fl)], TYPES.map((a) => [tt(a), ...PF.map((f) => { const v = prof(a, f); return v === null ? "–" : v.toFixed(3); })]),
    [24, 15, 15, 16, 15, 15], { caption: "Mediana feature kada tipu anomalia", align: [LEFT, R, R, R, R, R], size: 17 }));
  out.push(p(`Na'ok Eletrisidade iha baseline ratio ${prof("Energy Theft", "baseline_ratio").toFixed(2)} (meter rejista ~${(prof("Energy Theft", "baseline_ratio") * 100).toFixed(0)}% husi konsumu normál), Karga Liu iha load ratio ${prof("Overload", "load_ratio").toFixed(2)} (> 1.0), Kontador Aat iha consistency ratio ${prof("Meter Fault", "consistency_ratio").toFixed(2)} (current la tuir P/(V·PF)), no Frekuénsia La Estável iha frequency deviation ${prof("Frequency Deviation", "freq_dev_hz").toFixed(2)} Hz. Ba Normal, feature hotu besik 1.0 ka 0.`));
  out.push(h3("5.2.4. Meters Suspeitu (Top Meters)"));
  out.push(...table(["Meter", "Municipality", "Customer type", "Anomalia", "Oras anómala", "kWh lakon (estimativa)"], F.top_meters.map((m) => [
    m.meter_id, m.municipality, m.customer_type, tt(m.anomaly_type), fmt(m.n_hours), m.lost_kwh_est ? fmt(m.lost_kwh_est, 1) : "–",
  ]), [18, 15, 15, 20, 14, 18], { caption: "Meters ho oras anómala barak liu (dataset 20 GB)", align: [LEFT, LEFT, LEFT, LEFT, R, R], size: 17 }));
  out.push(p("Lista ne'e mak output prátiku ba ekipa inspesaun EDTL: meters ho Na'ok Eletrisidade durante oras rihun ba rihun no estimativa kWh ne'ebé lakon."));

  // ================================================================ 5.3 Random Forest results
  out.push(h2("5.3. Rezultadu Model Random Forest"));
  out.push(h3("5.3.1. Komparasaun ho Métodu Seluk"));
  out.push(p(`Métodu haat treina iha training set hanesan no test iha test set hanesan (readings ${fmt(F.compare.n_test)} husi meters separadu, dataset ${MAINT}):`));
  const row = (name, m, kind, dec) => [name, kind, m.precision.toFixed(3), m.recall.toFixed(3), m.f1.toFixed(3), m.pr_auc.toFixed(3), dec];
  out.push(...table(["Métodu", "Tipu", "Precision", "Recall", "F1", "PR-AUC", "Desizaun"], [
    ["**Random Forest**", "Supervised (ensemble)", `**${rf.anomaly_precision.toFixed(3)}**`, `**${rf.anomaly_recall.toFixed(3)}**`, `**${rf.anomaly_f1.toFixed(3)}**`, `**${rf.pr_auc.toFixed(3)}**`, "**Hili – uza iha sistema**"],
    row("Decision Tree", DT, "Supervised", "La uza"),
    row("Logistic Regression", LR, "Supervised (lineár)", "La uza"),
    row("Isolation Forest (Liu et al., 2008)", IF, "Unsupervised", "La uza"),
  ], [25, 18, 10, 10, 9, 10, 18], { caption: `Komparasaun métodu machine learning iha test set hanesan (dataset ${MAINT})`, size: 17, align: [LEFT, LEFT, R, R, R, R, LEFT] }));
  out.push(p(`Decision Tree ho Logistic Regression hetan peza klase hanesan ho Random Forest (Normal = ${P.normal_weight}). Decision Tree iha recall aas maibé PR-AUC ki'ik (${DT.pr_auc.toFixed(3)}) tanba tree ida fó probability 0 ka 1 de'it; Logistic Regression la kaptura relasaun non-linear; Isolation Forest (la uza label) la bele fahe anomalia husi variasaun normál konsumu.`));

  out.push(h3("5.3.2. Rezultadu Test Random Forest"));
  out.push(p(`Random Forest treina ho training set ${fmt(rf.n_train)} readings husi dataset ${MAINT} no test iha ${fmt(rf.n_test)} readings husi meters separadu (distribuisaun natural, ${(CR.natural_anomaly_rate * 100).toFixed(2)}% anomalia). Spark MLlib Random Forest treina distribuídu iha ${fmt(ml.n_train)} readings no test iha readings hotu husi meters test (${fmt(ml.n_test)}).`));
  const mrow = (name, m) => [name, fmt(m.n_train), fmt(m.n_test), m.anomaly_precision.toFixed(4), m.anomaly_recall.toFixed(4), m.anomaly_f1.toFixed(4), m.pr_auc ? m.pr_auc.toFixed(4) : "–", m.f1_macro.toFixed(4), `${m.train_seconds.toFixed(1)} s`];
  out.push(...table(["Model Random Forest", "Training", "Test", "Precision", "Recall", "F1", "PR-AUC", "Macro F1", "Tempu"], [
    mrow(`scikit-learn ${MAINT} (#${rf.id}, ativu)`, rf), mrow(`Spark MLlib ${MAINT} (#${ml.id})`, ml),
  ], [24, 11, 12, 9, 9, 8, 9, 9, 9], { caption: "Rezultadu test Random Forest (anomalia vs normal)", size: 16, align: [LEFT, R, R, R, R, R, R, R, R] }));
  out.push(p(`Random Forest scikit-learn atinje Precision ${rf.anomaly_precision.toFixed(3)}, Recall ${rf.anomaly_recall.toFixed(3)}, F1 ${rf.anomaly_f1.toFixed(3)}, PR-AUC ${rf.pr_auc.toFixed(3)} no ROC-AUC ${rf.roc_auc.toFixed(4)}, ho False Positive Rate ${pct(rf.false_positive_rate, 2)} de'it. Cross-validation 3-fold (group = meter) fó F1 anomalia ${CR.cv.anomaly_f1_mean.toFixed(3)} ± ${CR.cv.anomaly_f1_std.toFixed(3)}, katak model estável. Spark MLlib Random Forest atinje F1 ${ml.anomaly_f1.toFixed(3)} (Precision ${ml.anomaly_precision.toFixed(3)}, Recall ${ml.anomaly_recall.toFixed(3)}) iha readings ${fmt(ml.n_test / 1e6, 1)} millaun: recall hanesan, maibé precision ki'ik liu tanba MLlib uza split ho bin ${ml.params.maxBins} ba feature kontínuu, tree menus no la uza class weight (teste ho weight Normal = 2 fó F1 0.836, entaun model MLlib la ho weight mak rai).`));
  if (rfOld) {
    const fpRatio = (pr) => (1 - pr) / pr;
    const cut = (1 - fpRatio(rf.anomaly_precision) * rf.anomaly_recall / (fpRatio(rfOld.precision) * rfOld.recall)) * 100;
    out.push(p(`**Tuning class weight.** Random Forest ne'ebé treina la ho class weight (#11, dataset hanesan) atinje Precision ${rfOld.precision.toFixed(3)}, Recall ${rfOld.recall.toFixed(3)} no F1 ${rfOld.f1.toFixed(3)} iha test set hanesan: training set undersample Normal, entaun model fó alarme falsu barak. Ho class_weight Normal = ${P.normal_weight} (#${rf.id}), Precision sa'e ba ${rf.anomaly_precision.toFixed(3)} — alarme falsu menus maizumenus ${cut.toFixed(0)}% — enkuantu Recall tun uitoan de'it (${rf.anomaly_recall.toFixed(3)}), no F1 sa'e ba ${rf.anomaly_f1.toFixed(3)}.`));
  }

  out.push(h3("5.3.3. Feature Importance"));
  const fi = Object.entries(rf.feature_importance);
  out.push(...table(["No", "Feature", "Data type", "Feature Importance (%)"], fi.map(([k, v], i) => [i + 1, fl(k),
    ["customer_type", "municipality"].includes(k) ? "Categorical" : "Numeric", v.toFixed(2) + "%"]),
    [8, 42, 20, 30], { caption: "Feature Importance Random Forest (feature 16)", align: [C, LEFT, LEFT, R], size: 17 }));
  out.push(...figure("feature_importance.png", "Feature Importance Random Forest (Gini)", { maxHeight: 420 }));
  out.push(p(`Feature **${fl(fi[0][0])}** (${fi[0][1].toFixed(2)}%) no **${fl(fi[1][0])}** (${fi[1][1].toFixed(2)}%) mak importante liu, tuir husi ${fl(fi[2][0])} (${fi[2][1].toFixed(2)}%) no ${fl(fi[3][0])} (${fi[3][1].toFixed(2)}%). Feature engineering ho koñesimentu domíniu (load ratio, consistency ratio, baseline ratio) iha importánsia aas liu valór medidu barak, konsistente ho analítika estatístika iha seksaun 5.2.3.`));

  out.push(h3("5.3.4. Métrika Kada Tipu Anomalia"));
  out.push(...table(["Tipu", "Precision", "Recall", "F1-Score", "Readings (test)"], TYPES.map((a) => [tt(a), PC[a].precision.toFixed(4), PC[a].recall.toFixed(4), PC[a].f1.toFixed(4), fmt(PC[a].support)]),
    [32, 17, 17, 17, 17], { caption: "Métrika kada tipu anomalia (Random Forest, test set)", align: [LEFT, R, R, R, R], size: 18 }));
  out.push(...figure("confusion_matrix.png", "Confusion Matrix Random Forest iha test set", { maxHeight: 460 }));
  const easy = ANOM.filter((a) => PC[a].f1 > 0.99).map(tt);
  out.push(p(`${easy.join(", ")} deteta kuaze perfeitu (F1 > 0.99). Na'ok Eletrisidade mak susar liu (F1 ${PC["Energy Theft"].f1.toFixed(2)}): uma-kain ne'ebé uza eletrisidade uitoan (ezemplu fulan ho konsumu ki'ik) parese ho na'ok. Tan ne'e sistema hatudu **suspeita** de'it bainhira meter ida repete Na'ok Eletrisidade iha ≥50% readings ikus.`));

  out.push(h3("5.3.5. Threshold Anomaly Score"));
  out.push(...table(["Threshold Anomaly Score", "Precision", "Recall", "F1-Score", "Readings flag"], CR.thresholds.map((t) => [t.threshold.toFixed(1), t.precision.toFixed(4), t.recall.toFixed(4), t.f1.toFixed(4), fmt(t.flagged)]),
    [28, 18, 18, 18, 18], { caption: "Precision no Recall tuir threshold Anomaly Score (test set)", align: [C, R, R, R, R], size: 18 }));
  const t6 = CR.thresholds.find((t) => Math.abs(t.threshold - 0.6) < 1e-9);
  out.push(p(`Threshold aas hamenus false positive maibé lakon anomalia balun. Sistema uza threshold 0.6 ba events (Precision ${t6.precision.toFixed(3)}, Recall ${t6.recall.toFixed(3)}); Admin bele muda iha pájina Simulasaun.`));
  out.push(...figure("anomaly_pca_2d.png", "Feature space (PCA 2D) kór tuir tipu anomalia", { maxHeight: 420 }));

  // ================================================================ 5.4 real-time
  out.push(h2("5.4. Rezultadu Real-Time (Stream)"));
  out.push(p(`Iha stream, readings ${fmt(LV.n)} husi meters ${LV.meters} kompara ho ground truth bot. Random Forest atinje Accuracy ${pct(LV.accuracy)}, Precision ${LV.rf.precision.toFixed(4)}, Recall ${LV.rf.recall.toFixed(4)}, F1 ${LV.rf.f1.toFixed(4)}, ROC-AUC ${LV.roc_auc.toFixed(4)} no PR-AUC ${LV.pr_auc.toFixed(4)}. Bot la iha variasaun fulan-fulan UCI, entaun readings normál estável liu; rezultadu oficiál mak husi test set (seksaun 5.3).`));
  out.push(...table(["Tipu anomalia", "Readings (live)", "Recall Random Forest"], ANOM.filter((a) => LV.per_type_recall[a]).map((a) => [tt(a), fmt(LV.per_type_recall[a].n), pct(LV.per_type_recall[a].rf, 2)]),
    [40, 30, 30], { caption: "Recall kada tipu anomalia iha stream real-time", align: [LEFT, R, R], size: 18 }));

  // ================================================================ 5.5 functional
  out.push(h2("5.5. Functional Testing"));
  const evN = F.events.reduce((a, e) => a + Number(e.n), 0), evC = F.events.reduce((a, e) => a + Number(e.correct), 0);
  const ftRows = [
    ["Dadus tama?", "Bot → FastAPI → Kafka", kafkaMsgs ? `Loos – ${kafkaMsgs} mensajen iha topic` : "Loos – FastAPI publish ba Kafka"],
    ["Dadus prosesa ho loos?", "Spark parse, validation, feature, Random Forest", `Loos – ${fmt(Number(S.msgs))} mensajen, ${fmt(Number(S.invalid))} invalid rejeita`],
    ["Dadus rai ho loos?", "MySQL no data lake", `Loos – ${fmt(LV.n)} readings iha tbl_readings_live`],
    ["Dashboard funsiona?", "Pájina 9 loke, KPI no gráfiku atualiza", "Loos – screenshot pájina hotu (seksaun 4.7)"],
    ["Anomaly events", "Events ho tipu loos", `Loos – ${fmt(evN)} events, ${pct(evC / evN, 1)} tipu loos`],
    ["Simulasaun Voltajen Tun", "Meters Oecusse sai Voltajen Tun", "Loos – readings 202 hotu deteta"],
    ["Simulasaun Na'ok Eletrisidade", "Meters 8 sai suspeita", "Loos – 105 husi 117 readings deteta"],
    ["Deteksaun Manual / API /api/detect", "Tipu + votu tree", `Loos – ${tt(V.predicted_type)} ${V.votes[V.predicted_type]}/${V.n_trees}`],
    ["Automated test (pytest)", "Domain, feature, model, API", "Loos – test 10 passa"],
    ...(F.web_checks || []).map((c) => [`Website: ${c.test}`, c.expected, (c.ok ? "Loos" : "Sala") + (c.detail ? ` – ${c.detail}` : "")]),
  ];
  out.push(...table(["No", "Test", "Rezultadu esperadu", "Rezultadu"], ftRows.map((r, i) => [String(i + 1), ...r]), [6, 30, 32, 32], { caption: "Rezultadu functional testing", size: 17 }));
  out.push(p(`Pergunta haat iha manual (dadus tama, prosesa, rai, dashboard funsiona) hotu hetan resposta "loos". Teste website (No 10 to'o ${ftRows.length}) hala'o automatikamente ho Playwright iha Microsoft Edge (\`docs/report_build/shoot.py\`), ho konta temporáriu Admin no User ne'ebé kria ho password aleatóriu no hamoos depois teste: ${(F.web_checks || []).filter((c) => c.ok).length} husi ${(F.web_checks || []).length} passa.`));

  // ================================================================ 5.6 performance
  out.push(h2("5.6. Performance Testing"));
  out.push(p("Manual seksaun 14.B husu sukat Processing Time, Throughput, Latency no CPU/Memory Usage."));
  out.push(h3("5.6.1. Processing Time no Throughput (Batch)"));
  const stg = ["1_read_csv", "2_quality_profile", "3_clean_features_join", "4_anomaly_analytics", "5_train_test_samples", "6_write_curated_parquet"];
  const lab = { "1_read_csv": "Read CSV", "2_quality_profile": "Data Quality profile", "3_clean_features_join": "Cleaning + feature + join", "4_anomaly_analytics": "Anomaly analytics", "5_train_test_samples": "Training / test sample", "6_write_curated_parquet": "Write curated Parquet" };
  out.push(...table(["Etapa Spark ETL", ...SIZES.map((k) => k.replace("GB", " GB"))], [
    ["Core Spark", ...SIZES.map((k) => `local[${ETL[k].details.cores}]`)],
    ...stg.map((s) => [lab[s], ...SIZES.map((k) => ((ETL[k].details.timings_s || {})[s] || 0).toFixed(1) + " s")]),
    ["**Totál**", ...SIZES.map((k) => `**${ETL[k].seconds.toFixed(1)} s**`)],
    ["Records input → output", ...SIZES.map((k) => `${fmt(ETL[k].input_rows / 1e6, 1)} M → ${fmt(ETL[k].output_rows / 1e6, 1)} M`)],
    ["Throughput", ...SIZES.map((k) => `${fmt(ETL[k].input_rows / ETL[k].seconds / 1000, 0)} k records/s`)],
  ], [34, 22, 22, 22], { caption: "Tempu prosesamentu Spark batch ETL", align: [LEFT, R, R, R], size: 18 }));
  const e5 = ETL["5GB"], e20 = ETL["20GB"];
  out.push(p(`Dadus dala ${(e20.input_rows / e5.input_rows).toFixed(0)} boot liu (5 GB → 20 GB) presiza tempu dala ${(e20.seconds / e5.seconds).toFixed(1)}. Ba 20 GB, Spark uza core 16 (la'ós 32) no shuffle partitions = 2 × partition input hodi Java heap la to'o limite (iha teste dahuluk, ho konfigurasaun default, job 20 GB para ho *Java heap space*). Spark MLlib Random Forest presiza ${MLRUN.seconds.toFixed(0)} segundu (inklui lee Parquet no test iha readings ${fmt(ml.n_test / 1e6, 1)} millaun).`));
  out.push(...figure("ui_bigdata_etl.png", "Spark ETL: istória job, tempu kada etapa no Data Quality", { maxHeight: 700 }));
  out.push(h3("5.6.2. Latency no Throughput (Stream)"));
  out.push(p(`Ba speed layer, husi micro-batch ${fmt(S.batches)}: média mensajen ${fmt(Number(S.per_batch), 0)} kada batch (throughput ~${fmt(Number(S.per_batch) / 2, 0)} readings/s husi bot), prosesamentu ${fmt(S.proc_ms, 0)} ms kada batch, no latency média bot → MySQL ${(S.lat_ms / 1000).toFixed(2)} s (latency inklui intervalu trigger 3 s). Random Forest klasifika readings 1,000 iha ${CR.inference_ms_per_1000.toFixed(1)} ms.`));
  out.push(...figure("ui_bigdata_stream.png", "Estatístika streaming: latency end-to-end kada micro-batch", { maxHeight: 420 }));
  if (F.cpu && F.cpu.length) {
    out.push(h3("5.6.3. CPU no Memory Usage"));
    out.push(p(`CPU no memória sukat ho psutil kada segundu 0.5 bainhira servisu benchmark hanesan hala'o (\`docs/report_build/measure_cpu.py\`, komputadór ho thread ${F.cpu[0].logical_cpus}):`));
    out.push(...table(["Engine", "Volume", "Tempu", "CPU média", "CPU pico", "Memory pico"], F.cpu.map((c) => [
      c.engine === "spark" ? "Apache Spark" : "Pandas", c.volume, `${c.seconds.toFixed(1)} s`, `${c.cpu_avg_pct.toFixed(1)}%`, `${c.cpu_peak_pct.toFixed(0)}%`, `${c.mem_peak_gb.toFixed(2)} GB`,
    ]), [20, 14, 16, 16, 16, 18], { caption: "CPU no memory usage (Pandas vs Spark)", align: [LEFT, LEFT, R, R, R, R], size: 18 }));
    const pc = F.cpu.find((c) => c.engine === "pandas"), s20c = F.cpu.find((c) => c.engine === "spark" && c.volume === "20GB");
    out.push(p(`Pandas uza CPU ${pc.cpu_avg_pct.toFixed(0)}% de'it (core ida husi ${pc.logical_cpus}) — lalais ba dadus ki'ik maibé la uza komputadór tomak. Spark uza core hotu (CPU média ${s20c.cpu_avg_pct.toFixed(0)}%, pico 100% iha 20 GB) no memória hela ~13 GB maske dadus aumenta dala haat, tanba Spark prosesa partition-partition.`));
  }

  // ================================================================ 5.7 data quality testing
  out.push(h2("5.7. Data Quality Testing"));
  out.push(p(`Manual seksaun 14.C husu verifika Missing Values, Duplicate Records, Invalid Records no Outliers. Batch: Spark verifika kada dataset (seksaun 5.1.2) no outliers analiza ho regra IQR (seksaun 5.1.3). Stream: kada mensajen valida (schema, limite valór, duplicate) antes tama ba model — mensajen invalid ${fmt(Number(S.invalid))} husi ${fmt(Number(S.msgs))} (${(Number(S.invalid) / Number(S.msgs) * 100).toFixed(2)}%) rejeita, hanesan taxa 0.5% ne'ebé bot hatama.`));

  // ================================================================ 5.8 scalability
  out.push(h2("5.8. Scalability Testing"));
  out.push(p("Tuir manual seksaun 14.D (volume 10,000; 50,000; 100,000; 500,000; 1,000,000) no aumenta ho dataset 5 GB, 10 GB no 20 GB, scalability testing sukat servisu hanesan (lee CSV, hamoos, totál kWh kada municipality, top-10 meters, distribuisaun tipu anomalia, média voltage kada customer type) ho Pandas no Spark, run 3 kada engine, process foun kada run. Volume ki'ik mak subset husi dataset 5 GB. Engine rua fó rezultadu hanesan (records no totál kWh hanesan). Benchmark para Pandas bainhira memória komputadór to'o 85% (ka livre < 0.5 GB), hodi Windows la hahú swap, no rai rezultadu \"FAILED: out of memory\"."));
  const vols = ["10k", "50k", "100k", "500k", "1M", ...SIZES].filter((v) => B(v, "spark"));
  const peak = (b) => { const m = /peak ([\d.]+) GB/.exec(b.result_check || ""); return m ? `${m[1]} GB` : `${fmt(b.mem_mb / 1024, 1)} GB`; };
  out.push(...table(["Volume", "Records", "Pandas (s)", "Pandas RAM", "Spark (s)", "Spark RAM", "Manán"], vols.map((v) => {
    const a = B(v, "pandas"), b = B(v, "spark");
    if (!a || a.failed) {
      return [v, fmt(b.n_rows), "✕ out of memory", a ? `≥ ${peak(a)}` : "–", b.avg_s.toFixed(2), `${fmt(b.mem_mb / 1024, 2)} GB`, "**Spark de'it**"];
    }
    const win = a.avg_s < b.avg_s ? `Pandas ${(b.avg_s / a.avg_s).toFixed(1)}×` : `Spark ${(a.avg_s / b.avg_s).toFixed(2)}×`;
    return [v, fmt(a.n_rows), a.avg_s.toFixed(2), `${fmt(a.mem_mb / 1024, 2)} GB`, b.avg_s.toFixed(2), `${fmt(b.mem_mb / 1024, 2)} GB`, win];
  }), [11, 15, 16, 14, 12, 14, 18], { caption: "Rezultadu scalability testing Pandas vs Spark (média run 3)", align: [LEFT, R, R, R, R, R, LEFT], size: 17 }));
  const p1 = B("1M", "pandas"), s1 = B("1M", "spark"), s5 = B("5GB", "spark"), s10 = B("10GB", "spark"), s20 = B("20GB", "spark");
  const kS = Math.log(s20.avg_s / s5.avg_s) / Math.log(s20.n_rows / s5.n_rows);
  out.push(...figure("ui_bigdata_bench.png", "Dataset size vs execution time no memory: Pandas vs Spark", { maxHeight: 560 }));
  out.push(p(`Ba dadus ki'ik (10k to'o 1M records) Pandas lalais liu tanba Spark iha kustu fixu ~11 segundu (JVM no planu job): iha 1M, Pandas ${p1.avg_s.toFixed(1)} s vs Spark ${s1.avg_s.toFixed(1)} s. Maibé Pandas tenke karrega dataset tomak iha RAM (maizumenus dala 4 tamañu CSV), entaun iha dataset 5 GB, 10 GB no 20 GB **Pandas la konsege remata** — memória to'o ${peak(B("5GB", "pandas"))}–${peak(B("20GB", "pandas"))} no benchmark para. Spark prosesa partition-partition no bele *spill* ba disk: 5 GB iha ${s5.avg_s.toFixed(1)} s, 10 GB iha ${s10.avg_s.toFixed(1)} s no 20 GB iha ${s20.avg_s.toFixed(1)} s, ho memory maizumenus ${fmt(s20.mem_mb / 1024, 0)} GB de'it ba volume hotu. Husi 5 GB ba 20 GB, eksponente eskala Spark k = ${kS.toFixed(2)} (kuaze lineár). **Konkluzaun: ba dataset 5 GB ba leten, Apache Spark de'it mak bele uza** iha komputadór ne'e.`));

  // ================================================================ 5.9 compliance
  out.push(h2("5.9. Kumprimentu ho Rekezitu Mínimu Manual"));
  out.push(p("Tabela tuir mai kompara rekezitu tékniku mínimu iha manual Capstone Project (seksaun 12 no 13) ho saida mak PowerPredict TL implementa:"));
  out.push(...table(["Rekezitu", "Mínimu (manual)", "PowerPredict TL", "Estadu"], [
    ["Data Source", "Fonte online 1", "UCI Open Data, Open-Meteo REST API (arkivu + live), bot simulated real-time", "Kumpre"],
    ["Data Records", "≥ 10,000", `${fmt(totalRows / 1e6, 1)} M records batch + ${fmt(LV.n / 1e6, 2)} M readings live`, "Kumpre"],
    ["Data Type", "≥ tipu 2", "Numeric, categorical, datetime; CSV, JSON, Parquet, MySQL", "Kumpre"],
    ["Streaming / Real-Time", "Obrigatóriu", "Bot → FastAPI → Kafka → Spark Streaming kada 3 s; dashboard refresh 5–10 s", "Kumpre"],
    ["Apache Kafka", "Obrigatóriu", "Kafka 3.9.1 KRaft, topic smart-meter-readings, partition 3", "Kumpre"],
    ["Apache Spark", "Obrigatóriu", "Batch ETL 5 / 10 / 20 GB, Spark Streaming, Spark MLlib", "Kumpre"],
    ["Data Storage", "Obrigatóriu", `Data Lake (CSV + Parquet) + MySQL (table ${F.tables.length})`, "Kumpre"],
    ["Data Analytics", "Obrigatóriu", "Descriptive, trend, statistical + Random Forest (anomaly detection)", "Kumpre"],
    ["Visualization", "Obrigatóriu", "KPI, gráfiku Altair, mapa, confusion matrix, tendénsia", "Kumpre"],
    ["Dashboard", "Obrigatóriu", "Streamlit: login Admin / User, pájina 9, Refresh, Night mode", "Kumpre"],
    ["Documentation", "Obrigatóriu", "Relatóriu ida ne'e + README + kódigu ho komentáriu", "Kumpre"],
    ["Live Demonstration", "Obrigatóriu", "start_all.bat + pájina Simulasaun (Admin); vídeo YouTube (Anexu C)", "Prontu"],
  ], [18, 16, 52, 14], { caption: "Kumprimentu ho rekezitu tékniku mínimu manual", firstColBold: true, size: 17 }));

  // ================================================================ 5.10 discussion
  out.push(h2("5.10. Diskusaun"));
  out.push(p("Seksaun ida ne'e hatán pergunta peskiza (seksaun 1.3) bazeia ba rezultadu:"));
  out.push(...numbers([
    `**Dezempeñu Random Forest.** Iha meters ne'ebé model nunka haree, Random Forest atinje F1 ${rf.anomaly_f1.toFixed(3)}, PR-AUC ${rf.pr_auc.toFixed(3)} no ROC-AUC ${rf.roc_auc.toFixed(3)}; iha stream real-time F1 ${LV.rf.f1.toFixed(3)}. Tipu kualidade enerjia (Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável) no Kontador Aat kuaze perfeitu; Na'ok Eletrisidade mak dezafiu (F1 ${PC["Energy Theft"].f1.toFixed(2)}).`,
    `**Tanba sa Random Forest fit.** Dadus iha label, klase la balansu no anomalia depende ba kombinasaun feature. Random Forest manán métodu tolu seluk iha test set hanesan, estável iha cross-validation (± ${CR.cv.anomaly_f1_std.toFixed(3)}), no class weight bele kontrola balansu Precision–Recall. Ida ne'e konsistente ho Fernández-Delgado et al. (2014). ${F.outliers ? `Regra outlier (IQR) hetan ${F.outliers.anom_caught.toFixed(0)}% husi anomalia maibé ho precision ${(F.outliers.any_anom_share / 100).toFixed(2)} de'it no Na'ok Eletrisidade ${F.outliers.per_type_caught["Energy Theft"].toFixed(0)}% de'it, entaun threshold simples la to'o.` : ""}`,
    `**Big Data no Pandas vs Spark.** Pipeline FastAPI–Kafka–Spark–MySQL prosesa dataset to'o 20 GB no stream kontínuu ho latency ~${(S.lat_ms / 1000).toFixed(1)} s. Konfirma aprezentasaun individuál (Costa, 2026a): Pandas manán ba dadus ki'ik, maibé iha komputadór RAM 32 GB Pandas la konsege ona iha 5 GB — Spark mak solusaun ba dataset 5 GB ba leten.`,
    `**Feature importante.** ${fl(fi[0][0])}, ${fl(fi[1][0])}, ${fl(fi[2][0])} no ${fl(fi[3][0])} mak importante liu. Hanesan Jokar et al. (2016), konsumu kompara ho padraun normál (baseline ratio) mak sinál ba na'ok; consistency ratio mak sinál ba Kontador Aat.`,
    "**Website.** Website Streamlit ho login no role fasil uza no seguru: teste automátiku hotu passa, User la bele loke pájina Admin, no funsaun Refresh no Night mode servisu.",
  ]));
  out.push(p(`**Limitasaun rezultadu.** (1) Dadus sintétiku: maske seed mai husi dadus reál, padraun anomalia mai husi jeradór, entaun rezultadu iha dadus EDTL reál bele ki'ik liu. (2) Rezultadu stream aas liu test set tanba bot la iha variasaun fulan-fulan. (3) Sistema hala'o iha komputadór ida ho Kafka broker ida; iha produsaun presiza cluster no replication. (4) Na'ok Eletrisidade ho konsumu ki'ik loos sei bele sai alarme falsu — tan ne'e sistema uza "suspeita" no inspesaun manuál.`));
  return out;
};
