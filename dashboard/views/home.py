"""
home.py - Dashboard prinsipál (estrutura hanesan projetu referénsia, tópiku: deteksaun anomalia)

Header -> 1. Alerta/Notifikasaun -> 2. Análitika Deskritiva -> 3. Análize Tendénsia
-> 4. Análize Estatístika -> 5. Monitorizasaun + Deteksaun Anomalia (Anomaly Score, Random Forest)
-> 6. Monitorizasaun + Klasifikasaun Anomalia (Random Forest) -> 7. Monitorizasaun Real-Time
Auto refresh kada 10 s (st.fragment, la blokeia navegasaun).
"""
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

import charts as C
import data
import ui
from common import domain as D
from dash_config import CONFIG
from ml_anomaly import run_live_rf, run_live_score, sample_predictions, theft_suspects

ui.page_header("Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)",
               "app", key="refresh_home")


@st.fragment(run_every=f"{CONFIG['refresh_seconds']}s")
def dashboard():
    st.caption(f"Pipeline Big Data Real-Time | Bot → FastAPI → Kafka → Spark Streaming → Random Forest → MySQL | "
               f"Refresh: {CONFIG['refresh_seconds']}s | Atualiza: {datetime.now().strftime('%H:%M:%S')}")
    if not data.ping():
        st.error("MySQL la konektadu - hahú XAMPP MySQL", icon=ui.ICON["error"])
        st.stop()
    hb = data.heartbeats().get("stream_processor")
    if data.is_alive("stream_processor"):
        st.success(f"MySQL konektadu · Spark Streaming ativu (batch {hb['info'].get('batch')}, "
                   f"lag {hb['info'].get('lag')}, watermark {int(hb['info'].get('watermark_s', 120))} s)", icon=ui.ICON["ok"])
    else:
        st.warning("MySQL konektadu, maibé Spark Streaming la ativu - dadus la atualiza (hala'o start_all.bat)",
                   icon=ui.ICON["warning"])

    win = data.live_window(CONFIG["window_readings"])
    if win.empty:
        st.info("Hein readings husi stream... (hahú bot, API no Spark Streaming)", icon=ui.ICON["wait"])
        return
    now_ts = win["ts"].max()
    latest = win.sort_values("id").groupby("meter_id").tail(1)
    rf = run_live_rf(win)

    # ============================================================ 1. ALERTA
    st.header(":material/notifications_active: Alerta / Notifikasaun", anchor=False)
    c1, c2 = st.columns(2)
    with c1:
        now_anom = latest[latest.is_anomaly]
        if len(now_anom):
            parts = ", ".join(f"{n} {D.ANOMALY_TET[t]}" for t, n in now_anom.predicted_type.value_counts().items())
            st.warning(f"{len(now_anom)} meters iha anomalia agora: {parts} — Random Forest", icon=ui.ICON["warning"])
        else:
            st.success("La iha meters ho anomalia agora — Random Forest", icon=ui.ICON["ok"])
    with c2:
        sus = theft_suspects(win)
        ev = data.events(500)
        open_high = ev[(ev.acknowledged == 0) & (ev.severity == "High")] if not ev.empty else ev
        if len(sus):
            st.error(f"{len(sus)} meters suspeita na'ok eletrisidade (≥50 % readings ikus = Na'ok Eletrisidade): "
                     + ", ".join(sus.meter_id.head(4)) + (" ..." if len(sus) > 4 else ""), icon=ui.ICON["theft"])
        elif len(open_high):
            st.warning(f"{len(open_high)} events severidade aas seidauk konfirma — haree pájina Anomaly Events",
                       icon=ui.ICON["warning"])
        else:
            st.success("La iha suspeita na'ok eletrisidade", icon=ui.ICON["ok"])

    # ============================================================ 2. DESKRITIVA
    st.header(":material/insights: Análitika Deskritiva", anchor=False)
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric(":material/data_usage: Readings", f"{data.table_count('tbl_readings_live'):,}")
    m2.metric(":material/speed: Meters", f"{win.meter_id.nunique():,}")
    m3.metric(":material/map: Municipalities", f"{win.municipality.nunique():,}")
    m4.metric(":material/percent: Anomaly Rate", f"{win.is_anomaly.mean() * 100:.2f}%",
              help=f"Anomalous readings iha {len(win):,} readings ikus (Random Forest)")
    m5.metric(":material/notifications_active: Anomaly Events", f"{data.table_count('tbl_anomaly_events'):,}")

    # ============================================================ 3. TENDÉNSIA
    st.header(":material/trending_up: Análize Tendénsia", anchor=False)
    tr = data.trend(CONFIG["trend_minutes"])
    if tr.empty:
        st.info("Hein agregasaun Spark...", icon=ui.ICON["wait"])
    else:
        step = st.radio("Janela", ["1 minutu", "5 minutu"], horizontal=True, key="trend_step", label_visibility="collapsed")
        if step == "5 minutu":
            tr = tr.set_index("window_start").resample("5min").sum(numeric_only=True).reset_index()
        st.caption(f"Anomalia kada {step} (Random Forest, janela tumbling Spark, {CONFIG['trend_minutes']} minutu ikus)")
        long = C.type_long(tr, "window_start")
        if long.empty:
            st.info("La iha anomalia iha janela ne'e")
        else:
            C.show(C.stacked_time(long, "window_start", "n", "tipu", title_y="Anomalous readings"))
        rate = tr.assign(rate=lambda d: d.n_anomalies / d.n_readings.where(d.n_readings > 0) * 100)
        st.caption("Rasiu anomalia (%) no readings kada janela")
        r1, r2 = st.columns(2)
        with r1:
            C.show(alt.Chart(rate).mark_line(strokeWidth=2, color=C.color_of("Voltage Sag")).encode(
                x=alt.X("window_start:T", title=None, axis=C.TIME_AXIS),
                y=alt.Y("rate:Q", title="% anomalia"),
                tooltip=[alt.Tooltip("window_start:T", format="%H:%M", title="Minutu"),
                         alt.Tooltip("rate:Q", format=".2f", title="% anomalia")]).properties(height=180))
        with r2:
            C.show(alt.Chart(rate).mark_area(opacity=0.25, color=C.color_of("Normal")).encode(
                x=alt.X("window_start:T", title=None, axis=C.TIME_AXIS),
                y=alt.Y("n_readings:Q", title="Readings"),
                tooltip=[alt.Tooltip("window_start:T", format="%H:%M", title="Minutu"),
                         alt.Tooltip("n_readings:Q", format=",", title="Readings")]).properties(height=180))

    # ============================================================ 4. ESTATÍSTIKA
    st.header(":material/bar_chart: Análize Estatístika", anchor=False)
    st.caption(f"Distribuisaun anomalia iha {len(win):,} readings ikus (tuir tipu no tuir municipality)")
    an = win[win.is_anomaly]
    s1, s2 = st.columns([2, 3])
    with s1:
        by_type = C.tet(an.predicted_type).value_counts().rename_axis("Tipu").reset_index(name="Readings")
        if by_type.empty:
            st.info("La iha anomalia")
        else:
            C.show(C.hbar(by_type, "Tipu", "Readings", color="Tipu", title_x="Anomalous readings"))
    with s2:
        bm = an.assign(predicted_type=C.tet(an.predicted_type)).groupby(["municipality", "predicted_type"]).size().reset_index(name="n")
        if not bm.empty:
            C.show(C.stacked_hbar(bm, "municipality", "n", "predicted_type", title_x="Anomalous readings"))

    # ============================================================ 5. ANOMALY SCORE (RANDOM FOREST)
    st.header("Monitorizasaun + Deteksaun Anomalia")
    th = float(data.setting("alert_min_score", "0.6"))
    sc = run_live_score(win, th)
    u1, u2, u3, u4 = st.columns(4)
    u1.metric(":material/data_usage: Readings", f"{len(win):,}")
    u2.metric(":material/speed: Meters", f"{win.meter_id.nunique():,}")
    u3.metric(":material/my_location: Anomalia atuál", f"{(win.true_type.fillna('Normal') != 'Normal').sum():,}",
              help="Ground truth husi jeradór (bot) - uza de'it ba evaluasaun")
    u4.metric(":material/flag: Flag (Score ≥ threshold)", f"{sc.get('flagged', 0):,}")
    st.divider()
    st.subheader(":material/radar: Deteksaun Anomalia: Anomaly Score (Random Forest)", anchor=False)
    st.caption(f"Model: Random Forest | Anomaly Score = 1 − P(Normal) | Threshold events agora: {th:.2f} "
               "(muda iha pájina Simulasaun)")
    if "error" in sc:
        st.info(sc["error"])
    else:
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Precision", f"{sc['precision']:.2%}")
        i2.metric("Recall", f"{sc['recall']:.2%}")
        i3.metric("F1-Score", f"{sc['f1']:.2%}")
        i4.metric("PR-AUC", f"{sc['pr_auc']:.3f}" if sc["pr_auc"] else "-")
        st.divider()
        h1, h2 = st.columns(2)
        with h1:
            st.write("**Distribuisaun Anomaly Score**")
            C.show(alt.Chart(sc["scores"]).mark_bar(opacity=0.85, binSpacing=1).encode(
                x=alt.X("Anomaly Score:Q", bin=alt.Bin(maxbins=40), title="Anomaly Score (0 = normal, 1 = anomalia)"),
                y=alt.Y("count():Q", stack=None, title="Readings", scale=alt.Scale(type="symlog")),
                color=alt.Color("Ground truth:N", scale=alt.Scale(domain=["Normal", "Anomalia"],
                                                                  range=[C.color_of("Normal"), C.color_of("Energy Theft")]),
                                legend=alt.Legend(orient="bottom", title=None)),
                tooltip=[alt.Tooltip("Ground truth:N"), alt.Tooltip("count():Q", title="Readings")]).properties(height=240))
        with h2:
            st.write("**Precision / Recall tuir Threshold (live)**")
            st.dataframe(sc["table"].style.format({"Threshold": "{:.1f}", "Precision": "{:.1%}", "Recall": "{:.1%}",
                                                   "F1-Score": "{:.1%}", "Flag": "{:,}"}),
                         width="stretch", hide_index=True)
            st.caption("Threshold aas = false positive menus, maibé anomalia balun lakon. "
                       "Random Forest fó score besik 0 ba readings normál no besik 1 ba anomalia.")

    # ============================================================ 6. RANDOM FOREST
    st.header("Monitorizasaun + Klasifikasaun Anomalia")
    rm = data.active_model("supervised")
    o1, o2, o3, o4 = st.columns(4)
    o1.metric(":material/data_usage: Live Readings", f"{data.table_count('tbl_readings_live'):,}")
    o2.metric(":material/notifications_active: Events", f"{data.table_count('tbl_anomaly_events'):,}")
    o3.metric(":material/view_module: Batch Spark", f"{data.table_count('tbl_stream_batches'):,}")
    o4.metric(":material/account_tree: Model", f"{data.table_count('tbl_models'):,}")
    st.divider()
    st.subheader("Distribuisaun Tipu Anomalia (Prediction Random Forest)")
    dist = win.predicted_type.value_counts().reindex(D.ANOMALY_TYPES, fill_value=0).rename_axis("Tipu").reset_index(name="Readings")
    dist["Tipu"] = C.tet(dist["Tipu"])
    C.show(alt.Chart(dist).mark_bar(cornerRadiusEnd=4).encode(
        x=alt.X("Tipu:N", sort=C.ALL, title=None, axis=alt.Axis(labelAngle=0, labelLimit=160)),
        y=alt.Y("Readings:Q", scale=alt.Scale(type="symlog"), title="Readings (log scale)"),
        color=alt.Color("Tipu:N", scale=C.scale(include_normal=True), legend=None),
        tooltip=["Tipu", alt.Tooltip("Readings:Q", format=",")]).properties(height=220))
    st.divider()
    st.subheader(":material/category: Klasifikasaun Anomalia (Random Forest)", anchor=False)
    st.caption("Model: Random Forest Classifier | Target: anomaly_type (Normal + 6 tipu anomalia) | "
               "Anomaly Score = 1 − P(Normal)")
    if "error" in rf:
        st.info(rf["error"])
    else:
        st.info("Klase: " + ", ".join(C.ALL), icon=ui.ICON["info"])
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Accuracy (7 klase)", f"{rf['accuracy']:.2%}")
        k2.metric("Precision anomalia", f"{rf['anomaly_precision']:.2%}")
        k3.metric("Recall anomalia", f"{rf['anomaly_recall']:.2%}")
        k4.metric("F1-Score anomalia", f"{rf['anomaly_f1']:.2%}")
        if rm:
            st.caption(f"Validasaun live iha {rf['n']:,} readings stream ikus vs ground truth jeradór. Model #{int(rm['id'])} "
                       f"training ho dataset {rm['dataset']} ({int(rm['n_train']):,} readings); test iha meters separadu: "
                       f"F1 anomalia {rm['anomaly_f1']:.2%}, PR-AUC {rm['pr_auc']:.3f}.")
        st.divider()
        f1c, f2c = st.columns(2)
        with f1c:
            st.write("**Feature Importance**")
            fi = data.jloads(rm.get("feature_importance") if rm else None, {})
            if fi:
                dfi = pd.DataFrame({"Feature": [D.FEATURE_LABELS.get(k, k) for k in fi], "Importance (%)": list(fi.values())})
                C.show(C.hbar(dfi, "Feature", "Importance (%)", title_x="Importance (%)", fmt=".1f"))
        with f2c:
            st.write("**Confusion Matrix (live)**")
            C.show(C.heatmap(rf["confusion"], D.ANOMALY_TYPES))
        st.write("**Precision / Recall tuir tipu anomalia (live)**")
        st.dataframe(rf["per_type"].style.format({"Precision": "{:.1%}", "Recall": "{:.1%}", "F1-Score": "{:.1%}"}),
                     width="stretch", hide_index=True)
        st.write("**Ezemplu Prediction (anomalia ikus)**")
        st.dataframe(sample_predictions(win, 10), width="stretch", hide_index=True)

    # ============================================================ 7. REAL-TIME
    st.header(":material/sensors: Monitorizasaun Real-Time", anchor=False)
    st.caption(f"Readings ne'ebé tama ona ba MySQL · ikus: {now_ts:%H:%M:%S}")
    cols = ["ts", "meter_id", "municipality", "customer_type", "voltage_v", "power_kw", "power_factor",
            "frequency_hz", "baseline_ratio", "predicted_type", "anomaly_score"]
    rec = win[cols].head(20).assign(predicted_type=lambda d: C.tet(d.predicted_type)).rename(columns={
        "ts": "Tempu", "meter_id": "Meter", "municipality": "Municipality", "customer_type": "Tipu Kliente",
        "voltage_v": "Voltage (V)", "power_kw": "Power (kW)", "power_factor": "Power Factor",
        "frequency_hz": "Frequency (Hz)", "baseline_ratio": "Baseline Ratio", "predicted_type": "Prediction",
        "anomaly_score": "Anomaly Score"})
    st.write("**Readings foun (20 ikus)**")
    st.dataframe(rec, width="stretch", hide_index=True, height=(len(rec) + 1) * 35 + 3,
                 column_config={"Anomaly Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")})
    st.write("**Anomalia Foun (20 ikus)**")
    an20 = win[win.is_anomaly][cols].head(20).assign(predicted_type=lambda d: C.tet(d.predicted_type)).rename(columns=dict(zip(cols, rec.columns)))
    if an20.empty:
        st.info("La iha anomalia iha readings ikus")
    else:
        st.dataframe(an20, width="stretch", hide_index=True,
                     column_config={"Anomaly Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")})


dashboard()
