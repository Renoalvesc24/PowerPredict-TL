"""
app.py - Website PowerPredict TL: Deteksaun Anomalia Rede Eletrisidade (Streamlit)

Hala'o (husi folder projetu):
    streamlit run dashboard/app.py --server.port 8505

Login (auth.py): User = monitorizasaun no análize; Admin = asesu tomak (simulasaun, retrain model,
konfirma events, jestaun uzuáriu). Pájina sira (dashboard/views/):
    Monitorizasaun : Dashboard, Monitor Real-Time, Anomaly Events
    Análize        : Deteksaun Manual, Model Random Forest, Big Data Pipeline (dataset 5GB/10GB/20GB)
    Admin          : Simulasaun, Jestaun Uzuáriu
    Informasaun    : Metodolojia
"""
import sys
from pathlib import Path

import streamlit as st

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))

import auth  # noqa: E402
import data  # noqa: E402
import ui  # noqa: E402

st.set_page_config(page_title="PowerPredict TL - Deteksaun Anomalia Rede Eletrisidade (Random Forest)",
                   page_icon=ui.ICON["app"], layout="wide")
ui.css()
auth.cookie_script()
ui.theme_script()

user = auth.current_user()
if not user:
    st.navigation([st.Page("views/login.py", title="Login", icon=ui.ICON["login"], url_path="login")],
                  position="hidden").run()
    st.stop()

I = ui.ICON
pages = {
    "Monitorizasaun": [
        st.Page("views/home.py", title="Dashboard", icon=I["dashboard"], default=True),
        st.Page("views/monitor.py", title="Monitor Real-Time", icon=I["monitor"]),
        st.Page("views/events.py", title="Anomaly Events", icon=I["events"]),
    ],
    "Análize": [
        st.Page("views/detect.py", title="Deteksaun Manual", icon=I["detect"]),
        st.Page("views/models.py", title="Model Random Forest", icon=I["model"]),
        st.Page("views/bigdata.py", title="Big Data Pipeline", icon=I["bigdata"]),
    ],
}
if auth.is_admin(user):
    pages["Admin"] = [
        st.Page("views/simulation.py", title="Simulasaun", icon=I["simulation"]),
        st.Page("views/users.py", title="Jestaun Uzuáriu", icon=I["users"]),
    ]
pages["Informasaun"] = [st.Page("views/about.py", title="Metodolojia", icon=I["about"])]
nav = st.navigation(pages)

with st.sidebar:
    st.markdown(f"#### {I['app']} PowerPredict TL")
    st.caption("Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest "
               "(Capstone Project) · Kafka + Spark")
    ui.user_card(user)
    # one button per row: "Night mode" does not fit in half the sidebar width
    ui.theme_button("theme_sidebar")
    if st.button("Logout", icon=I["logout"], width="stretch"):
        auth.logout()
        st.rerun()

    st.caption("Estadu sistema")

    @st.fragment(run_every="10s")
    def status():
        hb = data.heartbeats()
        ui.status_line("MySQL", data.ping())
        for comp, label in (("api", "FastAPI"), ("bot", "Meter bot"), ("stream_processor", "Spark Streaming")):
            h = hb.get(comp)
            extra = ""
            if h and comp == "stream_processor":
                extra = f"batch {h['info'].get('batch', '-')}, lag {h['info'].get('lag', '-')}"
            elif h and comp == "api":
                extra = f"{h['info'].get('rate_mps', 0)} msg/s"
            elif h and comp == "bot":
                extra = f"{h['info'].get('scenario', '')}"
            ui.status_line(label, data.is_alive(comp), extra)

    status()

nav.run()
