"""
dash_config.py - Konfigurasaun dashboard Streamlit
(naran la'ós config.py atu la konflitu ho config.py iha raiz projetu)
"""

CONFIG = {
    "refresh_seconds": 10,          # auto refresh pájina prinsipál
    "monitor_refresh_seconds": 5,   # auto refresh Monitor Real-Time
    "window_readings": 50000,       # readings ikus ne'ebé karrega ba análize (~3 min stream)
    "trend_minutes": 60,            # janela timeline (minutu)
    "stream_stale_seconds": 30,     # heartbeat tuan liu ne'e = komponente la ativu
    "api_url": "http://127.0.0.1:8000",
}
