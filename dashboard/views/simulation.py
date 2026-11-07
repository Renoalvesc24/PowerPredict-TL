"""
simulation.py - Simulasaun: hamosu anomalia ba demo (bot lee setting husi API kada tick)
"""
import time

import streamlit as st

import auth
import data
import ui
from common import domain as D

auth.require_admin()
ui.page_header("Simulasaun Anomalia", "simulation", "Meter bot hamosu anomalia rasik (aleatóriu). Iha ne'e ita bele obriga anomalia ida ba demonstrasaun - "
           "depois haree Dashboard / Monitor Real-Time / Anomaly Events.")

cur = data.setting("scenario", "normal")
bot = data.heartbeats().get("bot")
st.info(f"Senáriu agora: **{cur}** · bot {':green[ativu]' if data.is_alive('bot') else ':red[la ativu]'}"
        + (f" ({bot['info'].get('messages', 0):,} readings haruka)" if bot else ""), icon=ui.ICON["live"])

st.subheader("Senáriu kontínuu (to'o ita hili Normal)")
c1, c2, c3 = st.columns(3)
with c1:
    muni = st.selectbox("Municipality", D.MUNICIPALITY_NAMES, index=D.MUNICIPALITY_NAMES.index("Oecusse"))
    if st.button("Voltajen Tun iha municipality", icon=ui.ICON["down"], width="stretch"):
        data.set_setting("scenario", f"voltage_sag:{muni}")
        st.rerun()
    if st.button("Voltajen Sa'e Liu iha municipality", icon=ui.ICON["up"], width="stretch"):
        data.set_setting("scenario", f"over_voltage:{muni}")
        st.rerun()
with c2:
    st.write("")
    st.write("")
    if st.button("Frekuénsia La Estável (generator trip, rede prinsipál)", icon=ui.ICON["factory"], width="stretch"):
        data.set_setting("scenario", "generator_trip")
        st.rerun()
    if st.button("Karga Liu (2 % meters / tick)", icon=ui.ICON["overload"], width="stretch"):
        data.set_setting("scenario", "overload_wave")
        st.rerun()
with c3:
    st.write("")
    st.write("")
    if st.button("Normal (para senáriu)", icon=ui.ICON["stop"], type="primary", width="stretch"):
        data.set_setting("scenario", "normal")
        st.rerun()

st.subheader("Events dala ida (meters aleatóriu)")
d1, d2 = st.columns(2)
with d1:
    n = st.slider("Meters", 1, 30, 8)
    if st.button(f"Hahú na'ok eletrisidade iha {n} meters (5-30 min)", icon=ui.ICON["theft"], width="stretch"):
        data.set_setting("trigger", f"theft:{n}:{int(time.time())}")
        st.success("Bot sei hahú na'ok iha tick tuir mai")
with d2:
    n2 = st.slider("Meters ", 1, 30, 5)
    if st.button(f"Kontador Aat iha {n2} meters (2-10 min)", icon=ui.ICON["fault"], width="stretch"):
        data.set_setting("trigger", f"meter_fault:{n2}:{int(time.time())}")
        st.success("Bot sei hamosu Kontador Aat iha tick tuir mai")

st.subheader("Konfigurasaun")
e1, e2 = st.columns(2)
with e1:
    iv = st.select_slider("Intervalu bot (s)", [0.5, 1.0, 2.0, 3.0, 5.0],
                          value=float(data.setting("producer_interval_s", "2")))
    if st.button("Rai intervalu", icon=ui.ICON["save"]):
        data.set_setting("producer_interval_s", str(iv))
        st.success("Bot uza intervalu foun iha tick tuir mai")
with e2:
    th = st.slider("Threshold Anomaly Score ba events", 0.3, 0.95, float(data.setting("alert_min_score", "0.6")), 0.05,
                   help="Score = 1 − P(Normal). Aas liu = false positive menus, maibé lakon anomalia barak liu")
    if st.button("Rai threshold", icon=ui.ICON["save"]):
        data.set_setting("alert_min_score", f"{th:.2f}")
        st.success("Spark Streaming uza threshold foun iha batch tuir mai")
