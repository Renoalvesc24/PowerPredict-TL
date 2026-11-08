"""
ui.py - elementu UI komún: header pájina ho butaun Refresh, Night mode / Day mode, ikon profisionál
(Material Symbols, la'ós emoji) no CSS ki'ik.

Night mode: Streamlit rai tema iha browser (localStorage `stActiveTheme-<path>-v2` = "Light" / "Dark").
Butaun ne'e hakerek valór ne'e ba pájina hotu no reload pájina; login la lakon tanba cookie sesaun (auth.py).
"""
from __future__ import annotations

import json
from datetime import datetime

import streamlit as st

# Material Symbols (https://fonts.google.com/icons) - ikon ida ba kada konseitu
ICON = {
    "app": ":material/bolt:", "dashboard": ":material/space_dashboard:", "monitor": ":material/monitor_heart:",
    "events": ":material/notifications_active:", "detect": ":material/troubleshoot:",
    "model": ":material/account_tree:", "bigdata": ":material/database:", "simulation": ":material/tune:",
    "about": ":material/menu_book:", "users": ":material/manage_accounts:", "login": ":material/login:",
    "logout": ":material/logout:", "refresh": ":material/refresh:", "dark": ":material/dark_mode:",
    "light": ":material/light_mode:", "ok": ":material/check_circle:", "warning": ":material/warning:",
    "error": ":material/error:", "wait": ":material/hourglass_top:", "info": ":material/info:",
    "theft": ":material/electrical_services:", "readings": ":material/data_usage:", "meters": ":material/speed:",
    "map": ":material/map:", "rate": ":material/percent:", "list": ":material/list_alt:",
    "trend": ":material/trending_up:", "stats": ":material/bar_chart:", "target": ":material/my_location:",
    "flag": ":material/flag:", "batch": ":material/view_module:", "brain": ":material/psychology:",
    "live": ":material/sensors:", "feed": ":material/rss_feed:", "wave": ":material/ssid_chart:",
    "download": ":material/download:", "confirm": ":material/task_alt:", "train": ":material/model_training:",
    "admin": ":material/admin_panel_settings:", "user": ":material/person:", "lock": ":material/lock:",
    "dataset": ":material/inventory_2:", "etl": ":material/settings_suggest:", "search": ":material/search:",
    "speed": ":material/speed:", "stream": ":material/stream:", "down": ":material/south:",
    "up": ":material/north:", "factory": ":material/factory:", "overload": ":material/local_fire_department:",
    "fault": ":material/build:", "stop": ":material/stop_circle:", "save": ":material/save:",
}

THEME_PATHS = ["/", "/monitor", "/events", "/detect", "/models", "/bigdata", "/simulation", "/about", "/users",
               "/login"]

CSS = """
<style>
/* cards for metrics + compact headers; colours come from the active Streamlit theme */
[data-testid="stMetric"] { padding: 10px 14px; border-radius: 10px;
                           border: 1px solid rgba(128,128,128,0.25); background: rgba(128,128,128,0.04); }
[data-testid="stMetricLabel"] p { font-size: 0.86rem; opacity: 0.85; }
[data-testid="stMetricValue"] { font-size: 1.7rem; }
h1 { font-size: 1.85rem !important; }
h2 { font-size: 1.35rem !important; padding-top: 0.8rem !important; }
[data-testid="stSidebarUserContent"] { padding-top: 0.5rem; }
.pp-user { border-radius: 10px; padding: 10px 12px; margin-bottom: 6px;
           border: 1px solid rgba(128,128,128,0.25); background: rgba(128,128,128,0.06); }
.pp-user b { font-size: 0.95rem; } .pp-user span { font-size: 0.78rem; opacity: 0.75; }
.pp-role { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 0.72rem;
           font-weight: 600; letter-spacing: .02em; margin-left: 6px; }
.pp-role.admin { background: rgba(235,104,52,0.15); color: #d95926; }
.pp-role.user { background: rgba(42,120,214,0.15); color: #2a78d6; }
.pp-role.guest { background: rgba(27,175,122,0.15); color: #199e70; }
.pp-status { font-size: 0.86rem; margin: 2px 0; }
.pp-status small { opacity: .65; }
</style>
"""


def css() -> None:
    st.html(CSS)


def is_dark() -> bool:
    try:
        return st.context.theme.type == "dark"
    except Exception:
        return False


# --------------------------------------------------------------------------- header + refresh
def page_header(title: str, icon: str, caption: str | None = None, key: str | None = None) -> None:
    """Title with icon on the left, Refresh button + last update time on the right."""
    left, right = st.columns([0.8, 0.2], vertical_alignment="center")
    left.title(title, icon=ICON.get(icon, icon), anchor=False)
    with right:
        if st.button("Refresh", icon=ICON["refresh"], key=key or f"refresh_{title}", width="stretch",
                     help="Karrega dadus foun husi Database agora (la hein auto refresh)"):
            st.cache_data.clear()
            st.session_state["_refreshed"] = datetime.now().strftime("%H:%M:%S")
            st.rerun()
        st.caption(f"Atualiza: {datetime.now():%H:%M:%S}", text_alignment="right")
    if caption:
        st.caption(caption)
    if st.session_state.get("_refreshed"):
        st.toast(f"Dadus atualiza ona ({st.session_state.pop('_refreshed')})", icon=ICON["ok"])


# --------------------------------------------------------------------------- night mode
def theme_button(key: str = "theme") -> None:
    dark = is_dark()
    label, icon = ("Day mode", ICON["light"]) if dark else ("Night mode", ICON["dark"])
    if st.button(label, icon=icon, key=key, width="stretch", help="Muda tema website (Night / Day)"):
        st.session_state["_set_theme"] = "Light" if dark else "Dark"
        st.rerun()


def theme_script() -> None:
    """Runs once after the button: store the theme for every page path and reload the page."""
    if "_set_theme" not in st.session_state:
        return
    value = json.dumps(st.session_state.pop("_set_theme"))
    paths = json.dumps(THEME_PATHS)
    st.html(f"""<script>
      (function() {{
        const paths = {paths}; const here = window.location.pathname;
        if (!paths.includes(here)) paths.push(here);
        for (const p of paths) {{ window.localStorage.setItem('stActiveTheme-' + p + '-v2', JSON.stringify({value})); }}
        window.location.reload();
      }})();
    </script>""", unsafe_allow_javascript=True, width="content")


# --------------------------------------------------------------------------- sidebar
def user_card(user: dict) -> None:
    role = user["role"]
    st.html(f"<div class='pp-user'><b>{user['full_name']}</b><span class='pp-role {role}'>"
            f"{ {'admin': 'ADMIN', 'guest': 'KONVIDADU'}.get(role, 'USER')}</span><br><span>@{user['username']}</span></div>")


def status_line(label: str, ok: bool, extra: str = "") -> None:
    icon = ":green[:material/check_circle:]" if ok else ":red[:material/cancel:]"
    st.markdown(f"{icon} **{label}** <small style='opacity:.65'>{extra}</small>", unsafe_allow_html=True)
