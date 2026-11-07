"""
monitor.py - Monitor Real-Time: mapa meters, estadu agora kada municipality, feed anomalia (refresh 5 s)
"""
import altair as alt
import pandas as pd
import streamlit as st

import charts as C
import data
import ui
from common import domain as D
from dash_config import CONFIG

ui.page_header("Monitor Real-Time", "monitor", "Estadu ikus husi kada meter ne'ebé streaming (281 meters, reading kada 2 s). Refresh automátiku kada 5 s.")


@st.fragment(run_every=f"{CONFIG['monitor_refresh_seconds']}s")
def monitor():
    win = data.live_window(6000)
    if win.empty:
        st.info("Hein readings husi stream...", icon=ui.ICON["wait"])
        return
    latest = win.sort_values("id").groupby("meter_id").tail(1).copy()
    meters = data.live_meters()[["meter_id", "latitude", "longitude"]]
    latest = latest.merge(meters, on="meter_id", how="left")
    n_anom = int(latest.is_anomaly.sum())

    a, b, c, d = st.columns(4)
    a.metric("Active meters", f"{latest.meter_id.nunique():,}")
    b.metric("Anomalia agora", f"{n_anom:,}", help="Meter ne'ebé nia readings ikus Random Forest dehan anómala")
    c.metric("Rasiu agora", f"{n_anom / max(len(latest), 1) * 100:.1f}%")
    lat = win.latency_ms.dropna()
    d.metric("Latency stream (median)", f"{lat.median() / 1000:.1f} s" if len(lat) else "-",
             help="Husi bot hamosu readings to'o rezultadu iha MySQL")

    left, right = st.columns([3, 2])
    with left:
        st.subheader(":material/map: Mapa meters", anchor=False)
        pal = C.colors(True)
        latest["color"] = C.tet(latest.predicted_type).map(pal)
        latest["size"] = latest.is_anomaly.map({True: 2600, False: 700})
        latest = latest.sort_values("is_anomaly")      # anomalia iha leten
        st.map(latest.dropna(subset=["latitude", "longitude"]), latitude="latitude", longitude="longitude",
               color="color", size="size", zoom=7.6, height=430)
        legend = " ".join(f"<span style='color:{pal[t]};font-size:1.2em'>●</span> {t}" for t in C.ALL)
        st.markdown(f"<div style='font-size:.85em'>{legend}</div>", unsafe_allow_html=True)
    with right:
        st.subheader("Municipality agora")
        g = latest.groupby("municipality").agg(meters=("meter_id", "count"), anomalia=("is_anomaly", "sum"),
                                               voltajen=("voltage_v", "mean")).reset_index()
        g["%"] = (g.anomalia / g.meters * 100).round(1)
        st.dataframe(g.sort_values("anomalia", ascending=False).rename(columns={
            "municipality": "Municipality", "meters": "Meters", "anomalia": "Anomalia", "voltajen": "Average Voltage (V)"}),
            width="stretch", hide_index=True, height=430,
            column_config={"Average Voltage (V)": st.column_config.NumberColumn(format="%.1f"),
                           "%": st.column_config.ProgressColumn("% anomalia", min_value=0, max_value=100, format="%.0f%%")})

    st.subheader(":material/rss_feed: Feed anomalia (anomalous readings ikus)", anchor=False)
    feed = win[win.is_anomaly].head(40).copy()
    if feed.empty:
        st.success("La iha anomalia iha readings ikus")
    else:
        feed["Evidénsia"] = feed.apply(lambda r: "; ".join(D.explain_anomaly(r)), axis=1)
        feed["Tipu"] = feed.predicted_type.map(D.ANOMALY_TET)
        st.dataframe(feed[["ts", "meter_id", "municipality", "Tipu", "anomaly_score", "voltage_v", "power_kw",
                           "frequency_hz", "Evidénsia"]].rename(columns={
            "ts": "Tempu", "meter_id": "Meter", "municipality": "Municipality", "anomaly_score": "Score",
            "voltage_v": "V", "power_kw": "kW", "frequency_hz": "Hz"}), width="stretch", hide_index=True,
            column_config={"Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")})

    st.subheader(":material/ssid_chart: Voltage no Frequency (minutu ikus)", anchor=False)
    recent = win[win.ts >= win.ts.max() - pd.Timedelta(minutes=3)]
    v = recent.groupby(["ts", "municipality"]).voltage_v.mean().reset_index()
    f = recent.groupby("ts").frequency_hz.mean().reset_index()
    v1, v2 = st.columns(2)
    with v1:
        C.show(alt.Chart(v).mark_line(strokeWidth=1.5).encode(
            x=alt.X("ts:T", title=None, axis=alt.Axis(format="%H:%M:%S")),
            y=alt.Y("voltage_v:Q", title="Average Voltage (V)", scale=alt.Scale(zero=False)),
            detail="municipality:N", color=alt.value(C.color_of("Voltage Sag")), opacity=alt.value(0.55),
            tooltip=["municipality:N", alt.Tooltip("ts:T", format="%H:%M:%S"), alt.Tooltip("voltage_v:Q", format=".1f")])
            .properties(height=220, title="Voltage kada municipality (198-242 V = ±10 %)"))
    with v2:
        C.show(alt.Chart(f).mark_line(strokeWidth=2, color=C.color_of("Frequency Deviation")).encode(
            x=alt.X("ts:T", title=None, axis=alt.Axis(format="%H:%M:%S")),
            y=alt.Y("frequency_hz:Q", title="Average Frequency (Hz)", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("ts:T", format="%H:%M:%S"), alt.Tooltip("frequency_hz:Q", format=".2f")])
            .properties(height=220, title="Grid Frequency (normal 49.5-50.5 Hz)"))


monitor()
