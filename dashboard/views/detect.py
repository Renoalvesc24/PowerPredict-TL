"""
detect.py - Deteksaun Manual: hatama reading ida, Random Forest dehan tipu anomalia no tanba sa
"""
import json

import altair as alt
import pandas as pd
import streamlit as st

import charts as C
import data
import ui
from common import db
from common import domain as D
from ml.predict import predict_one

ui.page_header("Deteksaun Manual", "detect", "Hili meter ida, hatama valór reading, no Random Forest (150 tree) hatán: normal ka anomalia saida, "
           "ho votu kada tree no evidénsia.")

meters = data.live_meters()
if meters.empty:
    st.error("La iha meters iha database - hala'o `python -m setup.init_db`.")
    st.stop()

PRESETS = {
    "Normal": dict(voltage_v=221.0, power_kw=None, power_factor=0.94, frequency_hz=50.0, hour=20),
    "Voltage Sag": dict(voltage_v=191.0, power_kw=None, power_factor=0.94, frequency_hz=50.0, hour=19),
    "Over-Voltage": dict(voltage_v=249.0, power_kw=0.25, power_factor=0.95, frequency_hz=50.0, hour=3),
    "Frequency Deviation": dict(voltage_v=219.0, power_kw=None, power_factor=0.94, frequency_hz=48.8, hour=20),
    "Overload": dict(voltage_v=214.0, power_kw="overload", power_factor=0.93, frequency_hz=50.0, hour=20),
    "Energy Theft": dict(voltage_v=220.0, power_kw="theft", power_factor=0.94, frequency_hz=50.0, hour=20),
    "Meter Fault": dict(voltage_v=220.0, power_kw=None, power_factor=0.94, frequency_hz=50.0, hour=20, fault=True),
}

left, right = st.columns([2, 3])
with left:
    st.subheader("1. Reading")
    labels = meters.apply(lambda r: f"{r.meter_id} · {r.municipality} · {r.customer_type} · {r.contracted_kva} kVA", axis=1)
    idx = st.selectbox("Meter", range(len(meters)), format_func=lambda i: labels.iloc[i])
    m = meters.iloc[idx]
    st.write("Ezemplu lalais:")
    pc = st.columns(4)
    for i, name in enumerate(PRESETS):
        if pc[i % 4].button(D.ANOMALY_TET[name], key=f"p_{name}", width="stretch"):
            st.session_state["preset"] = name
    preset = PRESETS[st.session_state.get("preset", "Normal")]
    hour = st.slider("Oras", 0, 23, preset["hour"])
    temp = st.number_input("Temperature (°C)", 15.0, 42.0, 28.0, 0.5)
    base = float(D.baseline_kw(m.customer_type, m.contracted_kva, m.peak_util, m.pf_base, hour, 0, temp))
    pk = preset["power_kw"]
    p_default = (round(base, 3) if pk is None else round(base * 0.15, 3) if pk == "theft"
                 else round(m.contracted_kva * 1.3 * 0.93, 3) if pk == "overload" else pk)
    v = st.number_input("Voltage (V)", 150.0, 270.0, float(preset["voltage_v"]), 0.5)
    p = st.number_input("Power (kW)", 0.0, 120.0, float(p_default), 0.01,
                        help=f"Baseline meter ne'e iha oras {hour}:00 ≈ {base:.3f} kW")
    pf = st.number_input("Power Factor", 0.3, 1.0, float(preset["power_factor"]), 0.01)
    f = st.number_input("Frequency (Hz)", 45.0, 55.0, float(preset["frequency_hz"]), 0.05)
    div = 3.0 if m.phase == "three" else 1.0
    i_ok = p * 1000 / (v * max(pf, 0.3)) / div
    i_def = round(i_ok * (2.4 if preset.get("fault") else 1.0), 2)
    cur = st.number_input("Current (A)", 0.0, 400.0, float(i_def), 0.1,
                          help=f"Current ne'ebé tuir P/(V·PF) = {i_ok:.2f} A")
    go = st.button("Deteta anomalia", icon=ui.ICON["search"], type="primary", width="stretch")

with right:
    st.subheader("2. Rezultadu")
    if go or "last_detect" in st.session_state:
        if go:
            inp = {"meter_id": m.meter_id, "voltage_v": v, "power_kw": p, "power_factor": pf, "frequency_hz": f,
                   "temperature_c": temp, "current_a": cur,
                   "timestamp": pd.Timestamp.now().normalize().replace(hour=hour).strftime("%Y-%m-%d %H:%M:%S")}
            try:
                res = predict_one(inp)
            except RuntimeError as exc:
                st.error(str(exc))
                st.stop()
            st.session_state["last_detect"] = res
            db.bulk_insert(db.manual_checks, [{
                "model_id": res["model"]["id"], "meter_id": m.meter_id, "inputs": db.to_json(res["inputs"]),
                "predicted_type": res["predicted_type"], "anomaly_score": res["anomaly_score"],
                "confidence": res["confidence"], "probabilities": db.to_json(res["probabilities"]),
                "votes": db.to_json(res["votes"])}])
        res = st.session_state["last_detect"]
        t = res["predicted_type"]
        box = st.success if t == D.NORMAL else st.error
        box(f"### {D.ANOMALY_TET[t]}\nAnomaly Score **{res['anomaly_score']:.2f}** · Confidence "
            f"**{res['confidence']:.0%}** · {res['votes'][t] if res['votes'] else '-'} husi {res['n_trees']} tree vota ida-ne'e")
        pr = pd.DataFrame({"Tipu": C.tet(list(res["probabilities"])), "Probability": list(res["probabilities"].values()),
                           "Votu": [res["votes"][k] for k in res["probabilities"]] if res["votes"] else None})
        C.show(alt.Chart(pr).mark_bar(cornerRadiusEnd=4).encode(
            y=alt.Y("Tipu:N", sort=C.ALL, title=None, axis=alt.Axis(labelLimit=220)),
            x=alt.X("Probability:Q", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format="%")),
            color=alt.Color("Tipu:N", scale=C.scale(include_normal=True), legend=None),
            tooltip=["Tipu", alt.Tooltip("Probability:Q", format=".1%"), "Votu"]).properties(height=230,
            title="Probability Random Forest (= % tree ne'ebé vota)"))
        st.write("**Evidénsia:**")
        ev = res["evidence"]
        st.markdown("\n".join(f"- {e}" for e in ev) if ev else "- Valór hotu iha laran limit normal")
        feats = res["features"]
        st.write("**Feature ne'ebé model haree:**")
        st.dataframe(pd.DataFrame({"Feature": [D.FEATURE_LABELS.get(k, k) for k in feats],
                                   "Value": [round(v, 4) if isinstance(v, float) else v for v in feats.values()]}),
                     width="stretch", hide_index=True, height=300)
    else:
        st.info("Hili ezemplu ka hatama valór, depois klik **Deteta anomalia**.")

st.subheader("Istória deteksaun manual")
try:
    hist = data.q("SELECT created_at, meter_id, predicted_type, anomaly_score, confidence FROM tbl_manual_checks "
                  "ORDER BY id DESC LIMIT 20")
    st.dataframe(hist.assign(predicted_type=lambda d: C.tet(d.predicted_type)).rename(columns={
        "created_at": "Tempu", "meter_id": "Meter", "predicted_type": "Rezultadu",
        "anomaly_score": "Anomaly Score", "confidence": "Confidence"}), width="stretch", hide_index=True)
except Exception:
    pass
