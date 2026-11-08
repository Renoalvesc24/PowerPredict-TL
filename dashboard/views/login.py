"""
login.py - pájina login: tab User no tab Admin (role mai husi konta iha tbl_users)
"""
import streamlit as st

import auth
import config
import ui

st.html("<style>[data-testid='stSidebar'], [data-testid='stSidebarCollapsedControl'], "
        "[data-testid='stExpandSidebarButton'] { display: none; }</style>")

_, mid, _ = st.columns([1, 1.25, 1])
with mid:
    st.space("large")
    st.title("PowerPredict TL", icon=ui.ICON["app"], anchor=False, text_alignment="center")
    st.caption("Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)",
               text_alignment="center")

    if not auth.has_accounts():
        st.warning("Seidauk iha konta. Hakerek PP_ADMIN_USERNAME / PP_ADMIN_PASSWORD (no PP_USER_*) iha `.env` "
                   "(password karakter 8 ka liu), depois restart website.", icon=ui.ICON["warning"])

    if config.PUBLIC_GUEST:
        if st.button("Tama hanesan konvidadu (haree de'it)", icon=":material/visibility:", type="primary", width="stretch"):
            auth.guest_login()
            st.rerun()
        st.caption("Haree dashboard live, monitor real-time, anomaly events, model no Big Data. La presiza konta.",
                   text_alignment="center")

    if config.PUBLIC_NO_LOGIN:          # public copy: guest view only, no account login
        st.stop()

    with st.container(border=True):
        t_user, t_admin = st.tabs([f"{ui.ICON['user']} User", f"{ui.ICON['admin']} Admin"])
        for tab, portal in ((t_user, "user"), (t_admin, "admin")):
            with tab:
                with st.form(f"login_{portal}", border=False):
                    st.caption("Monitorizasaun, alerta, deteksaun manual no relatóriu." if portal == "user" else
                               "Asesu tomak: simulasaun, retrain model, konfirma events no jestaun uzuáriu.")
                    username = st.text_input("Username", key=f"u_{portal}", autocomplete="username")
                    password = st.text_input("Password", type="password", key=f"p_{portal}",
                                             autocomplete="current-password")
                    ok = st.form_submit_button("Login", icon=ui.ICON["login"], type="primary", width="stretch")
                if ok:
                    user, err = auth.login(username, password, portal)
                    if user:
                        st.rerun()
                    st.error(err, icon=ui.ICON["error"])

    c1, c2 = st.columns(2, vertical_alignment="center")
    c1.caption("Sesaun ativu oras 8.")
    with c2:
        ui.theme_button("theme_login")
