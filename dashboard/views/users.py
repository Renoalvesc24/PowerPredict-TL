"""
users.py - Jestaun Uzuáriu (Admin de'it): lista konta, kria konta, muda role, reset password, ativa / dezativa
"""
import streamlit as st

import auth
import ui

auth.require_admin()
me = st.session_state["user"]
ui.page_header("Jestaun Uzuáriu", "users", "Konta website: Admin (asesu tomak) no User (monitorizasaun). "
               "Password rai iha Database hanesan hash PBKDF2-SHA256 de'it.")

users = auth.list_users()
n_admin_active = int(((users.role == "admin") & (users.is_active.astype(bool))).sum()) if not users.empty else 0
c1, c2, c3 = st.columns(3)
c1.metric("Konta", f"{len(users):,}", icon=ui.ICON["users"], border=True)
c2.metric("Admin", f"{int((users.role == 'admin').sum()):,}", icon=ui.ICON["admin"], border=True)
c3.metric("User", f"{int((users.role == 'user').sum()):,}", icon=ui.ICON["user"], border=True)

show = users.assign(role=users.role.map(auth.ROLES), is_active=users.is_active.astype(bool))
st.dataframe(show.drop(columns=["id"]).rename(columns={
    "username": "Username", "full_name": "Naran", "role": "Role", "is_active": "Ativu", "created_at": "Kria iha",
    "last_login": "Login ikus"}), width="stretch", hide_index=True,
    column_config={"Ativu": st.column_config.CheckboxColumn(), "Kria iha": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
                   "Login ikus": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")})

left, right = st.columns(2, gap="large")
with left:
    st.subheader("Kria konta foun", anchor=False)
    with st.form("new_user", clear_on_submit=True):
        u = st.text_input("Username")
        n = st.text_input("Naran kompletu")
        r = st.segmented_control("Role", list(auth.ROLES), default="user", format_func=auth.ROLES.get, required=True)
        p1 = st.text_input("Password", type="password", help=f"Karakter {auth.MIN_PASSWORD} ka liu")
        p2 = st.text_input("Konfirma password", type="password")
        if st.form_submit_button("Kria konta", icon=ui.ICON["save"], type="primary"):
            err = "Password la hanesan." if p1 != p2 else auth.create_user(u, n, r, p1)
            if err:
                st.error(err, icon=ui.ICON["error"])
            else:
                st.toast(f"Konta '{u}' kria ona", icon=ui.ICON["ok"])
                st.rerun()

with right:
    st.subheader("Jere konta", anchor=False)
    if users.empty:
        st.info("Seidauk iha konta.")
    else:
        pick = st.selectbox("Konta", users.id.tolist(),
                            format_func=lambda i: (lambda r: f"{r.username} · {auth.ROLES[r.role]}"
                                                   + ("" if r.is_active else " · la ativu"))(users[users.id == i].iloc[0]))
        row = users[users.id == pick].iloc[0]
        is_me = int(row.id) == me["id"]
        last_admin = row.role == "admin" and bool(row.is_active) and n_admin_active <= 1
        if is_me:
            st.caption("Ida ne'e mak ita-nia konta - la bele muda role, dezativa ka hamoos.")
        elif last_admin:
            st.caption("Admin ativu ikus - la bele muda role, dezativa ka hamoos (website presiza admin ida).")
        locked = is_me or last_admin

        new_role = st.segmented_control("Role", list(auth.ROLES), default=row.role, format_func=auth.ROLES.get,
                                        required=True, key=f"role_{pick}", disabled=locked)
        active = st.toggle("Konta ativu", value=bool(row.is_active), key=f"active_{pick}", disabled=locked)
        if st.button("Rai mudansa", icon=ui.ICON["save"], disabled=locked, key=f"save_{pick}"):
            auth.update_user(int(row.id), role=new_role, is_active=active)
            st.toast("Mudansa rai ona", icon=ui.ICON["ok"])
            st.rerun()

        with st.form(f"reset_{pick}", clear_on_submit=True):
            np1 = st.text_input("Password foun", type="password")
            np2 = st.text_input("Konfirma password foun", type="password")
            if st.form_submit_button("Reset password", icon=ui.ICON["lock"]):
                err = "Password la hanesan." if np1 != np2 else auth.update_user(int(row.id), password=np1)
                if err:
                    st.error(err, icon=ui.ICON["error"])
                else:
                    st.toast(f"Password '{row.username}' muda ona", icon=ui.ICON["ok"])

        if st.button("Hamoos konta", icon=":material/delete:", disabled=locked, key=f"del_{pick}"):
            st.session_state["_confirm_delete"] = int(row.id)
        if st.session_state.get("_confirm_delete") == int(row.id) and not locked:
            st.warning(f"Hamoos konta '{row.username}'? La bele fila fali.", icon=ui.ICON["warning"])
            y, n_ = st.columns(2)
            if y.button("Sin, hamoos", type="primary", key=f"yes_{pick}"):
                auth.delete_user(int(row.id))
                st.session_state.pop("_confirm_delete", None)
                st.toast("Konta hamoos ona", icon=ui.ICON["ok"])
                st.rerun()
            if n_.button("Kansela", key=f"no_{pick}"):
                st.session_state.pop("_confirm_delete", None)
                st.rerun()
