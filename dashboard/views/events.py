"""
events.py - Anomaly events (alerta): filtru, konfirma, download CSV, suspeita na'ok
"""
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import update

import charts as C
import auth
import data
import ui
from common import db
from common import domain as D
from ml_anomaly import theft_suspects

ui.page_header("Anomaly Events", "events", "Spark Streaming hamosu event ida bainhira Random Forest deteta anomalia ho Anomaly Score ≥ "
           f"{data.setting('alert_min_score', '0.6')} (maksimu 1 event / meter / tipu kada 5 minutu).")

ev = data.events(5000)
if ev.empty:
    st.info("Seidauk iha anomaly events.")
    st.stop()

f1, f2, f3, f4 = st.columns(4)
types = f1.multiselect("Tipu", D.ANOMALIES, default=[], format_func=lambda a: D.ANOMALY_TET[a])
munis = f2.multiselect("Municipality", D.MUNICIPALITY_NAMES, default=[])
sev = f3.multiselect("Severidade", ["High", "Medium"], default=[])
state = f4.selectbox("Estadu", ["Hotu", "Seidauk konfirma", "Konfirma ona"])
view = ev.copy()
if types:
    view = view[view.anomaly_type.isin(types)]
if munis:
    view = view[view.municipality.isin(munis)]
if sev:
    view = view[view.severity.isin(sev)]
if state != "Hotu":
    view = view[view.acknowledged == (1 if state == "Konfirma ona" else 0)]

k1, k2, k3, k4 = st.columns(4)
k1.metric("Events (filter)", f"{len(view):,}")
k2.metric("Seidauk konfirma", f"{int((view.acknowledged == 0).sum()):,}")
k3.metric("Severidade aas", f"{int((view.severity == 'High').sum()):,}")
withtruth = view.dropna(subset=["true_type"])
k4.metric("Loos (vs ground truth)", f"{(withtruth.true_type == withtruth.anomaly_type).mean():.1%}" if len(withtruth) else "-",
          help="Events ne'ebé tipu prediction hanesan ho anomalia ne'ebé jeradór hatama")

c1, c2 = st.columns([3, 2])
with c1:
    recent = view.assign(m=pd.to_datetime(view.created_at).dt.floor("min"))
    recent = recent[recent.m >= recent.m.max() - pd.Timedelta(minutes=120)]       # 2 oras ikus (tempu events sai)
    per_min = recent.groupby(["m", "anomaly_type"]).size().reset_index(name="n")
    per_min["anomaly_type_order"] = per_min.anomaly_type.map({a: i for i, a in enumerate(D.ANOMALIES)})
    per_min["anomaly_type"] = C.tet(per_min.anomaly_type)
    if not per_min.empty:
        C.show(C.stacked_time(per_min, "m", "n", "anomaly_type", height=220, title_y="Events"))
with c2:
    bt = C.tet(view.anomaly_type).value_counts().rename_axis("Tipu").reset_index(name="Events")
    if not bt.empty:
        C.show(C.hbar(bt, "Tipu", "Events", color="Tipu", title_x="Events"))

st.subheader("Lista events")
show = view[["id", "ts", "meter_id", "municipality", "customer_type", "anomaly_type", "severity", "anomaly_score",
             "true_type", "reason", "acknowledged"]].head(1000).assign(
    anomaly_type=lambda d: C.tet(d.anomaly_type), true_type=lambda d: C.tet(d.true_type))
sel = st.dataframe(show.rename(columns={
    "id": "ID", "ts": "Tempu", "meter_id": "Meter", "municipality": "Municipality", "customer_type": "Tipu kliente",
    "anomaly_type": "Anomalia", "severity": "Severidade", "anomaly_score": "Score", "true_type": "Ground truth",
    "reason": "Evidénsia", "acknowledged": "Konfirma"}),
    width="stretch", hide_index=True, height=420, on_select="rerun", selection_mode="multi-row",
    column_config={"Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                   "Konfirma": st.column_config.CheckboxColumn()})
rows = sel.selection.rows if sel and sel.selection else []
b1, b2, _ = st.columns([1, 1, 3])
admin = auth.is_admin()
if b1.button(f"Konfirma {len(rows)} events", icon=ui.ICON["confirm"], disabled=not rows or not admin, type="primary",
             help=None if admin else "Admin de'it mak bele konfirma events"):
    ids = [int(show.iloc[i]["id"]) for i in rows]
    with db.begin() as conn:
        conn.execute(update(db.anomaly_events).where(db.anomaly_events.c.id.in_(ids))
                     .values(acknowledged=True, acknowledged_at=datetime.now()))
    st.cache_data.clear()
    st.toast(f"{len(ids)} events konfirma ona")
    st.rerun()
b2.download_button("Download CSV", view.to_csv(index=False).encode("utf-8"),
                   file_name=f"anomaly_events_{datetime.now():%Y%m%d_%H%M}.csv", mime="text/csv", icon=ui.ICON["download"])

st.subheader(":material/electrical_services: Suspeita na'ok eletrisidade (meters ho konsumu ki'ik la normál)",
             anchor=False)
st.caption("Na'ok la'ós reading ida de'it: meter ida sai suspeita bainhira Random Forest klasifika ≥ 50 % husi nia "
           "readings ikus hanesan Na'ok Eletrisidade. Ida-ne'e mak lista ba ekipa inspesaun.")
win = data.live_window(30000)
sus = theft_suspects(win) if not win.empty else pd.DataFrame()
if sus.empty:
    st.success("La iha suspeita agora", icon=ui.ICON["ok"])
else:
    st.dataframe(sus.rename(columns={"meter_id": "Meter", "municipality": "Municipality", "customer_type": "Tipu kliente",
                                     "n": "Readings", "theft": "% Na'ok Eletrisidade", "baseline": "Konsumu vs baseline (%)",
                                     "kw": "kW (median)"}), width="stretch", hide_index=True)
