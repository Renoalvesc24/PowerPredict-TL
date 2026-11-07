"""
about.py - Metodolojia: data source, arkitetura, tipu anomalia, oinsá Random Forest servisu
"""
import pandas as pd
import streamlit as st

import data
import ui
from common import domain as D

ui.page_header("Metodolojia", "about", "Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)")

st.header("1. Dadus mai husi ne'ebé? (Data Source)")
st.markdown("""
Dadus hotu **sintétiku maibé hahú husi dadus reál (seed)** - la'ós dadus kliente EDTL loos:

| Parte | Programa | Seed reál |
|---|---|---|
| **Batch / Big Data** (dataset CSV 5 GB, 10 GB, 20 GB) | `data_generator/generate_dataset.py` | **UCI ElectricityLoadDiagrams20112014** - konsumu reál kliente 264 (padraun fulan-fulan) · **Open-Meteo** - temperature reál kada oras 2025 iha kapitál municipality 14 |
| **Real-time** (smart meter 281, reading kada 2 s) | `bot/bot_live.py` | Modelu rede hanesan + temperature **live** husi Open-Meteo API |

Métodu hanesan **aprezentasaun individuál / Exercicio 1**: hahú husi dadus reál, depois aumenta meters to'o file CSV
to'o 5 GB, 10 GB no 20 GB (part file ~115 MB hanesan block HDFS). Jeradór hatama mós problema Data Quality
(missing value 0.3 %, duplicate 0.2 %, invalid value 0.1 %) atu Spark iha servisu Data Cleaning.

**Label (ground truth):** jeradór hatama **anomaly events** no rejista events ne'ebé ativu iha kada reading
(`anomaly_type`). Label mai husi events, **la'ós husi threshold** iha valór - entaun model tenke aprende padraun.
""")

st.header("2. Arkitetura (Lambda Architecture)")
st.graphviz_chart("""
digraph G {
  rankdir=LR; bgcolor="transparent"; node [shape=box, style="rounded,filled", fillcolor="#eef4fc", color="#2a78d6",
  fontname="Helvetica", fontsize=11]; edge [color="#8a8a85", fontname="Helvetica", fontsize=9];
  subgraph cluster_speed { label="Speed layer (real-time)"; color="#cfcfca"; fontname="Helvetica";
    bot [label="bot/bot_live.py\\nsmart meter 281 + Open-Meteo"]; api [label="api/main.py\\nFastAPI gateway"];
    kafka [label="Apache Kafka\\ntopic smart-meter-readings\\npartition 3"];
    stream [label="pipeline/spark_streaming.py\\nvalidation · feature · window 1 min\\nwatermark 2 min"];
    rf [label="Random Forest\\n(tipu anomalia + Anomaly Score)", fillcolor="#fdeee6", color="#eb6834"]; }
  subgraph cluster_batch { label="Batch layer"; color="#cfcfca"; fontname="Helvetica";
    gen [label="data_generator\\ndataset 5GB / 10GB / 20GB"]; etl [label="pipeline/spark_batch_etl.py\\nData Quality · feature · sample"];
    lake [label="Data Lake\\nParquet curated"]; train [label="ml/train.py (scikit-learn)\\npipeline/spark_ml_train.py (MLlib)\\nRandom Forest", fillcolor="#fdeee6", color="#eb6834"]; }
  mysql [label="MySQL (XAMPP)\\nDatabase powerpredict_db", fillcolor="#e8f6ef", color="#1baf7a"];
  web [label="dashboard/app.py\\nStreamlit (pájina 8)", fillcolor="#e8f6ef", color="#1baf7a"];
  bot -> api [label="POST batch"]; api -> kafka [label="key = meter_id"]; kafka -> stream; stream -> rf; rf -> mysql;
  gen -> etl -> lake -> train; etl -> mysql; train -> rf [label="model ativu"]; mysql -> web;
}
""")

st.header("3. Tipu anomalia ne'ebé sistema deteta")
st.dataframe(pd.DataFrame([{"Tipu": D.ANOMALY_TET[a], "Severity": D.ANOMALY_SEVERITY[a],
                            "Saida mak akontese": D.ANOMALY_INFO[a][0], "Se mak tenke halo": D.ANOMALY_INFO[a][1]}
                           for a in D.ANOMALIES]), width="stretch", hide_index=True)

st.header("4. Métodu Machine Learning: Random Forest")
st.markdown("""
**Tipu tarefa:** *supervised multi-class classification* - kada reading simu klase ida husi 7
(Normal + tipu anomalia 6). Husi probability, ita kalkula **Anomaly Score = 1 − P(Normal)**, entaun model ida
mós sai **anomaly detector binária** (anomalia ka normal) ho threshold ne'ebé ita bele muda.

**Tanba sa Random Forest de'it?** Iha test hanesan (dataset 20 GB, readings 1 millaun husi meters separadu),
Random Forest mak accurate liu (F1 anomalia 0.954, PR-AUC 0.990) kompara ho Decision Tree (0.925),
Logistic Regression (0.896) no Isolation Forest (0.144). Tan ne'e sistema uza métodu machine learning ida de'it:
**Random Forest** (Breiman, 2001).

1. *Bootstrap (bagging)*: kada tree (150) treina ho random sample (ho replacement) husi training set.
2. *Random feature selection*: iha kada split, tree haree de'it √16 ≈ 4 feature aleatóriu.
3. *Gini impurity*: hili split ne'ebé hamenus impurity liu:
""")
st.latex(r"Gini(t) = 1 - \sum_{k=1}^{K} p_k^2 \qquad \Delta = Gini(t) - \frac{n_L}{n}Gini(t_L) - \frac{n_R}{n}Gini(t_R)")
st.markdown("4. *Majority voting*: kada tree vota; probability klase = % tree ne'ebé vota ba klase ne'e.")
st.latex(r"\hat{y} = \arg\max_k \frac{1}{T}\sum_{t=1}^{T} \mathbb{1}[h_t(x)=k] \qquad Anomaly\;Score = 1 - P(\text{Normal}\mid x)")

st.markdown("**Feature 16** (Spark kalkula hanesan iha batch no stream):")
st.dataframe(pd.DataFrame([{"Feature": f, "Deskrisaun": D.FEATURE_LABELS[f]} for f in D.FEATURES]),
             width="stretch", hide_index=True, height=300)
st.markdown("""
Feature importante liu mak **feature engineering ho koñesimentu domíniu**:
- `baseline_ratio` = power ÷ konsumu normal meter ne'e iha oras ne'e (contract × utilisation × daily shape ×
  weekend × temperature × power factor) → Na'ok Eletrisidade (rasiu ki'ik) no Karga Liu (rasiu boot)
- `consistency_ratio` = P ÷ (V·I·PF) → Kontador Aat (current sensor sala)
- `voltage_dev_pct`, `freq_dev_hz`, `load_ratio` → Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu

**Evaluation honestu:**
- Training / test fahe **tuir meter** (1 husi 5 meters separadu) - episódiu na'ok naruk la bele iha parte rua.
- Training set (20 GB): Normal undersampled (600k) + anomalia hotu (to'o 80k / tipu) + **class weight Normal = 4** (korrije undersampling, entaun model la dehan "anomalia" fácil liu). Test set: **distribuisaun natural** (~3.9 % anomalia).
- Cross-validation 3-fold *StratifiedGroupKFold* (group = meter).
- Live: dashboard kompara prediction stream ho ground truth jeradór (`true_type`, la'ós input model).
""")
rm = data.active_model("supervised")
if rm:
    st.success(f"Model ativu: Random Forest #{int(rm['id'])} - test iha {int(rm['n_test']):,} readings husi meters "
               f"separadu: Precision {rm['anomaly_precision']:.1%}, Recall {rm['anomaly_recall']:.1%}, "
               f"F1-Score {rm['anomaly_f1']:.1%}, PR-AUC {rm['pr_auc']:.3f}")

st.header("5. Teknolojia")
st.markdown("""
Python 3.11 · **Apache Kafka 3.9** (KRaft) · **Apache Spark 3.5** (PySpark, Spark SQL, MLlib) · **FastAPI** ·
**scikit-learn** (Random Forest) · **MySQL / MariaDB** (XAMPP) · **Streamlit** + Altair · Parquet (Apache Arrow) · Open-Meteo API
""")
