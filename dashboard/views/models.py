"""
models.py - Model Random Forest: rezultadu test, tanba sa Random Forest, evaluasaun detalla, retrain, ativa model
"""
import subprocess
import sys

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import update

import charts as C
import config
import auth
import data
import ui
from common import db
from common import domain as D

ui.page_header("Model Random Forest", "model", "Sistema uza métodu machine learning ida de'it: Random Forest. Test iha meters ne'ebé model NUNKA haree "
           "(1 husi 5 meters separadu husi Spark), ho distribuisaun natural (~3.9 % anomalia).")

ms = data.models()
ms = ms[ms.algorithm == "random_forest"] if not ms.empty else ms
if ms.empty:
    st.warning("Seidauk iha model - hala'o `python -m ml.train --dataset 20GB`.")
    st.stop()

ms["Model"] = ms.apply(lambda r: f"#{int(r.id)} Random Forest ({'Spark MLlib' if r.engine == 'spark-mllib' else 'scikit-learn'}, {r.dataset})", axis=1)
st.subheader("Model Random Forest (test iha meters separadu)")
tbl = ms[["Model", "n_train", "n_test", "anomaly_precision", "anomaly_recall", "anomaly_f1", "pr_auc", "roc_auc",
          "false_positive_rate", "f1_macro", "train_seconds", "is_active"]].rename(columns={
    "n_train": "Training", "n_test": "Test", "anomaly_precision": "Precision", "anomaly_recall": "Recall",
    "anomaly_f1": "F1 anomalia", "pr_auc": "PR-AUC", "roc_auc": "ROC-AUC", "false_positive_rate": "False Positive Rate",
    "f1_macro": "Macro F1 (tipu)", "train_seconds": "Training (s)", "is_active": "Active"})
st.dataframe(tbl.style.format({c: "{:.1%}" for c in ["Precision", "Recall", "F1 anomalia", "Macro F1 (tipu)", "False Positive Rate"]}
                              | {"PR-AUC": "{:.3f}", "ROC-AUC": "{:.3f}", "Training (s)": "{:.1f}", "Training": "{:,}",
                                 "Test": "{:,}"}, na_rep="-"), width="stretch", hide_index=True)
ml_rows = ms[ms.engine == "spark-mllib"]
if not ml_rows.empty:
    mr = ml_rows.iloc[0]
    st.caption(f"Spark MLlib Random Forest = métodu hanesan, treina distribuídu iha {int(mr.n_train):,} readings no "
               f"test iha readings hotu husi meters test ({int(mr.n_test):,} readings, dataset {mr.dataset}).")

rm = data.active_model("supervised")
rf = ms[ms.id == rm["id"]].iloc[0] if rm else ms.sort_values("anomaly_f1").iloc[-1]
rep = data.jloads(rf["class_report"], {})
cv = rep.get("cv", {})

st.subheader("Tanba sa Random Forest?")
st.markdown(f"""
1. **Accurate liu.** Iha test hanesan (dataset 20 GB, readings 1 millaun husi meters separadu), Random Forest iha
   F1 anomalia **0.954** (PR-AUC 0.990), aas liu Decision Tree (0.925), Logistic Regression (0.896) no
   Isolation Forest (0.144). Tan ne'e sistema uza **Random Forest de'it**.
2. **Dadus iha label (ground truth).** Jeradór rejista anomaly events ba kada reading, entaun ita bele uza
   supervised learning ne'ebé fó mós **tipu** anomalia (la'ós de'it "anomalia ka lae").
3. **Anomalia depende ba feature barak hamutuk.** Na'ok Eletrisidade = power ki'ik *kompara ho baseline meter iha oras
   ne'e*; Kontador Aat = current la tuir P/(V·PF). Tree sira aprende decision boundary non-linear no interasaun.
4. **Votu tree 150 estável.** Probability husi votu sai **Anomaly Score** (1 − P(Normal)) ne'ebé bele hili threshold.
5. **Feature numériku + kategóriku, la presiza normaliza, robustu ba noise sensor.**
6. **Model estável:** cross-validation 3-fold grupu tuir meter: F1 anomalia
   **{cv.get('anomaly_f1_mean', float('nan')):.3f} ± {cv.get('anomaly_f1_std', float('nan')):.3f}**.
7. **Eskala ba Big Data:** Spark MLlib Random Forest treina distribuídu iha readings millaun.
""")

st.subheader(f"Random Forest #{int(rf.id)} (ativu) - detalla")
d1, d2 = st.columns(2)
with d1:
    st.write("**Precision / Recall tuir tipu anomalia (test)**")
    per = pd.DataFrame(rep.get("per_class", {})).T.reset_index().rename(columns={"index": "Tipu"})
    if not per.empty:
        per["Tipu"] = per.Tipu.map(D.ANOMALY_TET)
        st.dataframe(per[["Tipu", "precision", "recall", "f1", "support"]].rename(columns={
            "precision": "Precision", "recall": "Recall", "f1": "F1-Score", "support": "Readings"}).style.format(
            {"Precision": "{:.1%}", "Recall": "{:.1%}", "F1-Score": "{:.1%}", "Readings": "{:,.0f}"}),
            width="stretch", hide_index=True)
    st.caption("Na'ok Eletrisidade mak susar liu: uma-kain ida ne'ebé uza eletrisidade uitoan parese ho na'ok. "
               "Tan ne'e dashboard hatudu 'suspeita' de'it bainhira meter ida repete Na'ok Eletrisidade.")
with d2:
    st.write("**Confusion Matrix (test)**")
    cm = data.jloads(rf["confusion_matrix"], {})
    if cm.get("matrix"):
        C.show(C.heatmap(cm["matrix"], cm["labels"]))
d3, d4 = st.columns(2)
with d3:
    st.write("**Threshold Anomaly Score: Precision vs Recall (test)**")
    th = pd.DataFrame(rep.get("thresholds", []))
    if not th.empty:
        long = th.melt(id_vars="threshold", value_vars=["precision", "recall", "f1"], var_name="metric", value_name="v")
        C.show(alt.Chart(long).mark_line(point=alt.OverlayMarkDef(size=60), strokeWidth=2).encode(
            x=alt.X("threshold:Q", title="Score threshold (1 − P(Normal))"),
            y=alt.Y("v:Q", title=None, scale=alt.Scale(domain=[0.6, 1])),
            color=alt.Color("metric:N", scale=alt.Scale(domain=["precision", "recall", "f1"],
                                                        range=[C.color_of("Voltage Sag"), C.color_of("Over-Voltage"),
                                                               C.color_of("Frequency Deviation")]),
                            legend=alt.Legend(orient="bottom", title=None)),
            tooltip=["threshold", "metric", alt.Tooltip("v:Q", format=".3f")]).properties(height=240))
        st.caption(f"Threshold alerta agora: {data.setting('alert_min_score', '0.6')} (muda iha Simulasaun)")
with d4:
    st.write("**Feature Importance (Gini)**")
    fi = data.jloads(rf["feature_importance"], {})
    dfi = pd.DataFrame({"Feature": [D.FEATURE_LABELS.get(k, k) for k in fi], "Importance (%)": list(fi.values())})
    C.show(C.hbar(dfi, "Feature", "Importance (%)", title_x="Importance (%)", fmt=".1f"))
p = data.jloads(rf["params"], {})
st.caption(f"Parameter: {p} · model size {rep.get('model_size_mb', '-')} MB · inference "
           f"{rep.get('inference_ms_per_1000', '-')} ms / 1,000 readings")

if not auth.is_admin():
    st.info("Retrain Random Forest no hili model ativu: Admin de'it.", icon=ui.ICON["lock"])
    st.stop()

st.subheader("Retrain Random Forest (training foun)")
with st.form("retrain"):
    c1, c2, c3 = st.columns(3)
    trees = c1.slider("n_estimators (tree)", 20, 300, 150, 10)
    depth = c2.slider("max_depth", 6, 40, 24, 1)
    ds = c3.selectbox("Dataset", ["20GB", "10GB", "5GB"], help="Presiza Spark ETL ba dataset ne'e uluk")
    ok = st.form_submit_button("Training Random Forest", icon=ui.ICON["train"], type="primary")
log = config.LOG_DIR / "train_web.log"
if ok:
    if not (config.CURATED_DIR / f"train_sample_{ds}").exists():
        st.error(f"Seidauk iha training sample ba {ds} - hala'o `python -m pipeline.spark_batch_etl --dataset {ds}`.")
    else:
        with open(log, "w", encoding="utf-8") as fh:
            subprocess.Popen([sys.executable, "-W", "ignore", "-u", "-m", "ml.train", "--dataset", ds,
                              "--trees", str(trees), "--depth", str(depth), "--no-cv"],
                             cwd=str(config.ROOT), stdout=fh, stderr=subprocess.STDOUT)
        st.success("Training hahú iha background. Model foun sai ativu automátiku; Spark Streaming uza nia iha 10 s.")
if log.exists():
    with st.expander("Training log ikus"):
        st.code(log.read_text(encoding="utf-8", errors="ignore")[-3000:] or "(mamuk)")

st.subheader("Hili model ativu")
sup = ms[ms.engine == "scikit-learn"].sort_values("id", ascending=False)
choice = st.selectbox("Model Random Forest (Spark Streaming uza ida ne'e)", sup.id.tolist(),
                      format_func=lambda i: (lambda r: f"{r.Model} · F1 {r.anomaly_f1:.3f} · {r.created_at:%d/%m %H:%M}"
                                             + (" (ATIVU)" if r.is_active else ""))(sup[sup.id == i].iloc[0]))
if st.button("Ativa model ne'e"):
    with db.begin() as conn:
        conn.execute(update(db.models).where(db.models.c.task == "supervised").values(is_active=False))
        conn.execute(update(db.models).where(db.models.c.id == int(choice)).values(is_active=True))
    st.cache_data.clear()
    st.success(f"Model #{choice} ativu ona.")
    st.rerun()
