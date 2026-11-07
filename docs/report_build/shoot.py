"""
Capture report screenshots of the running PowerPredict TL website (Streamlit) and of
the HTML diagrams in ./diagrams, using the installed Microsoft Edge.

    python shoot.py pages      # website screenshots (dashboard on :8505, API on :8000 must run)
    python shoot.py diagrams   # diagram PNGs

The website needs a login. `pages` creates two temporary accounts (admin_relatoriu, user_relatoriu)
with random passwords, uses them for the screenshots and the login / role checks, and deletes them
at the end. The check results are written to web_checks.json (read by collect_facts.py).
"""
import json
import secrets
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "dashboard")]
OUT = HERE / "img"
OUT.mkdir(exist_ok=True)
BASE = "http://localhost:8505"
NO_FADE = ('[data-stale="true"]{opacity:1 !important;filter:none !important}'
           '[data-testid="stStatusWidget"],[data-testid="stToolbar"],header{visibility:hidden !important}'
           '[data-testid="stToastContainer"]{display:none !important}')
TEMP = {"admin": ("admin_relatoriu", "Admin Relatóriu"), "user": ("user_relatoriu", "User Relatóriu")}
CHECKS: list[dict] = []


# ----------------------------------------------------------------------------- temporary accounts
def remove_account(username: str) -> None:
    import auth
    from sqlalchemy import select

    from common import db
    with db.begin() as conn:
        row = conn.execute(select(db.users.c.id).where(db.users.c.username == username)).first()
    if row:
        auth.delete_user(int(row.id))


def create_accounts() -> dict:
    import auth
    creds = {}
    for role, (username, name) in TEMP.items():
        remove_account(username)
        password = secrets.token_urlsafe(18)
        err = auth.create_user(username, name, role, password)
        if err:
            raise RuntimeError(err)
        creds[role] = (username, password)
    return creds


def check(test: str, expected: str, ok: bool, detail: str = "") -> None:
    CHECKS.append({"test": test, "expected": expected, "ok": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + test + (f" ({detail})" if detail else ""))


# ----------------------------------------------------------------------------- page helpers
def visible(page, text, timeout=15000) -> bool:
    loc = page.get_by_text(text, exact=False)
    try:
        loc.first.wait_for(state="attached", timeout=timeout)
    except Exception:  # noqa: BLE001
        return False
    for _ in range(timeout // 500):
        if any(loc.nth(i).is_visible() for i in range(loc.count())):
            return True
        page.wait_for_timeout(500)
    return False


def first_visible(loc):
    for i in range(loc.count()):
        if loc.nth(i).is_visible():
            return loc.nth(i)
    raise RuntimeError("no visible element")


def login(page, portal: str, username: str, password: str) -> None:
    page.goto(BASE + "/", wait_until="load")
    page.get_by_label("Username").first.wait_for(timeout=90000)
    page.wait_for_timeout(1500)
    if portal == "admin":
        page.get_by_role("tab", name="Admin").first.click()
        page.wait_for_timeout(800)
    first_visible(page.get_by_label("Username")).fill(username)
    first_visible(page.get_by_label("Password")).fill(password)
    first_visible(page.get_by_role("button", name="Login")).click()
    page.wait_for_timeout(3500)


def logged_in(page) -> bool:
    return visible(page, "Logout", 30000)


def open_page(page, path, wait_text, settle=5000):
    page.goto(BASE + path, wait_until="load")
    page.get_by_text(wait_text).first.wait_for(timeout=90000)
    page.add_style_tag(content=NO_FADE)
    page.wait_for_timeout(settle)


def main_box(page):
    return page.locator('[data-testid="stMainBlockContainer"]').first.bounding_box()


def y_of(page, text):
    loc = page.locator('[data-testid="stMainBlockContainer"]').get_by_text(text, exact=False)
    for _ in range(60):
        for i in range(loc.count()):
            e = loc.nth(i)
            if e.is_visible() and e.bounding_box():
                return e.bounding_box()["y"]
        page.wait_for_timeout(1000)
    raise RuntimeError(f"text not visible: {text}")


def section(page, name, start_text, end_text=None, pad=14, end_y=None):
    mb = main_box(page)
    y0 = y_of(page, start_text) - pad
    y1 = end_y if end_y else (y_of(page, end_text) - pad if end_text else mb["y"] + mb["height"])
    page.screenshot(path=str(OUT / f"{name}.png"), clip={"x": mb["x"], "y": y0, "width": mb["width"], "height": y1 - y0})
    print("saved", name, int(y1 - y0), "px")


def full_main(page, name, max_h=None):
    mb = main_box(page)
    h = mb["height"] if not max_h else min(mb["height"], max_h)
    page.screenshot(path=str(OUT / f"{name}.png"), clip={"x": mb["x"], "y": mb["y"], "width": mb["width"], "height": h})
    print("saved", name, int(h), "px")


def window(page, name, path=None, wait_text=None, height=1000, settle=5000):
    """Screenshot of the whole browser window (sidebar + top of the page)."""
    page.set_viewport_size({"width": 1440, "height": height})
    if path:
        open_page(page, path, wait_text, settle)
    page.screenshot(path=str(OUT / f"{name}.png"))
    print("saved", name, "(window)")


def trim(name: str, pad: int = 40) -> None:
    """Cut the empty white margin around a window screenshot (the login card sits in the middle)."""
    from PIL import Image, ImageChops
    path = OUT / f"{name}.png"
    im = Image.open(path).convert("RGB")
    box = ImageChops.difference(im, Image.new("RGB", im.size, (255, 255, 255))).point(lambda v: 255 if v > 12 else 0).getbbox()
    if box:
        x0, y0, x1, y1 = box
        im.crop((max(0, x0 - pad), max(0, y0 - pad), min(im.width, x1 + pad), min(im.height, y1 + pad))).save(path)


def app_background(page) -> str:
    return page.evaluate("getComputedStyle(document.querySelector('.stApp')).backgroundColor")


def is_dark_rgb(rgb: str) -> bool:
    nums = [int(x) for x in rgb.replace("rgba", "").replace("rgb", "").strip("() ").split(",")[:3]]
    return sum(nums) / 3 < 80


# ----------------------------------------------------------------------------- website screenshots
def pages():
    creds = create_accounts()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="msedge", headless=True)
            shoot_all(browser, creds)
            browser.close()
    finally:
        for username, _ in TEMP.values():
            remove_account(username)
        print("temporary accounts removed")
        (HERE / "web_checks.json").write_text(json.dumps(CHECKS, indent=2, ensure_ascii=False), encoding="utf-8")
        print("checks ->", HERE / "web_checks.json", sum(c["ok"] for c in CHECKS), "/", len(CHECKS), "pass")


def shoot_all(browser, creds):
    au, ap = creds["admin"]
    uu, up = creds["user"]

    # --- login page + login / role checks (light theme)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1.5, color_scheme="light")
    page = ctx.new_page()
    page.goto(BASE + "/", wait_until="load")
    page.get_by_label("Username").first.wait_for(timeout=90000)
    page.add_style_tag(content=NO_FADE)
    page.wait_for_timeout(2500)
    page.screenshot(path=str(OUT / "ui_login.png"))
    trim("ui_login")
    print("saved ui_login (window, trimmed)")
    check("Login page", "Pájina login mosu antes asesu website", visible(page, "Username"), "tab User + tab Admin")

    login(page, "admin", au, "password-sala-123")
    check("Password sala", "Login rejeita", visible(page, "Username ka password la loos."), "mensajen erru mosu")
    login(page, "admin", uu, up)
    check("Konta User iha tab Admin", "Login rejeita (role la hanesan)", visible(page, "Konta ne'e mak"), "role tenke tuir tab")

    login(page, "user", uu, up)
    ok = logged_in(page)
    sidebar = page.locator('[data-testid="stSidebar"]')
    no_admin = ok and sidebar.get_by_text("Jestaun Uzuáriu").count() == 0 and sidebar.get_by_text("Simulasaun").count() == 0
    check("Login User", "Login susesu; menu Admin la mosu", ok and no_admin, "seksaun Monitorizasaun, Análize, Informasaun de'it")
    page.add_style_tag(content=NO_FADE)
    page.wait_for_timeout(4000)
    window(page, "ui_user_view")
    page.goto(BASE + "/simulation", wait_until="load")
    page.wait_for_timeout(6000)
    check("User loke /simulation", "Pájina Admin la loke ba User", not visible(page, "Simulasaun Anomalia", 4000), "URL admin bloka")
    page.goto(BASE + "/models", wait_until="load")
    check("User iha pájina Model", "Retrain no ativa model: Admin de'it", visible(page, "Admin de'it", 60000), "butaun retrain la mosu")
    first_visible(page.get_by_role("button", name="Logout")).click()
    page.wait_for_timeout(4000)
    check("Logout", "Fila ba pájina login", visible(page, "Username", 30000), "sesaun hamoos")
    ctx.close()

    # --- admin session: all pages (big viewport for section clips)
    ctx = browser.new_context(viewport={"width": 1440, "height": 1000}, device_scale_factor=1.5, color_scheme="light")
    page = ctx.new_page()
    login(page, "admin", au, ap)
    ok = logged_in(page)
    check("Login Admin", "Login susesu; menu Admin mosu",
          ok and page.locator('[data-testid="stSidebar"]').get_by_text("Jestaun Uzuáriu").count() > 0, "Simulasaun + Jestaun Uzuáriu")
    page.goto(BASE + "/monitor", wait_until="load")
    check("Sesaun hela", "Loke URL foun la presiza login fali", logged_in(page), "cookie sesaun pp_session")

    window(page, "ui_home_window", "/", "Readings foun (20 ikus)", settle=7000)
    page.set_viewport_size({"width": 1440, "height": 9000})
    open_page(page, "/", "Readings foun (20 ikus)", 7000)
    section(page, "ui_home_top", "Sistema Deteksaun Anomalia", "Análize Tendénsia")
    section(page, "ui_home_trend", "Análize Tendénsia", "Monitorizasaun + Deteksaun Anomalia")
    section(page, "ui_home_score", "Monitorizasaun + Deteksaun Anomalia", "Monitorizasaun + Klasifikasaun Anomalia")
    section(page, "ui_home_rf", "Monitorizasaun + Klasifikasaun Anomalia", "Precision / Recall tuir tipu anomalia (live)")
    section(page, "ui_home_rf_tables", "Precision / Recall tuir tipu anomalia (live)", "Monitorizasaun Real-Time")
    section(page, "ui_home_realtime", "Monitorizasaun Real-Time", "Anomalia Foun (20 ikus)")

    page.set_viewport_size({"width": 1440, "height": 2600})
    page.goto(BASE + "/monitor", wait_until="load")
    page.get_by_text("Feed anomalia").first.wait_for(timeout=90000)
    page.add_style_tag(content=NO_FADE)
    page.wait_for_timeout(8000)
    section(page, "ui_monitor", "Monitor Real-Time", "Voltage no Frequency")

    page.set_viewport_size({"width": 1440, "height": 4000})
    open_page(page, "/events", "Lista events")
    section(page, "ui_events", "Anomaly Events", "Suspeita na'ok")
    section(page, "ui_events_theft", "Suspeita na'ok")
    before = page.locator('[data-testid="stMainBlockContainer"]').get_by_text("Atualiza:").first.inner_text()
    page.wait_for_timeout(1200)
    first_visible(page.get_by_role("button", name="Refresh")).click()
    page.wait_for_timeout(4000)
    after = page.locator('[data-testid="stMainBlockContainer"]').get_by_text("Atualiza:").first.inner_text()
    check("Butaun Refresh", "Dadus foti fali; oras 'Atualiza' muda", before != after, f"{before} -> {after}")

    open_page(page, "/detect", "Deteta anomalia", 2500)
    page.get_by_role("button", name="Na'ok Eletrisidade").first.click()
    page.wait_for_timeout(2500)
    page.get_by_role("button", name="Deteta anomalia").first.click()
    page.get_by_text("Probability Random Forest").first.wait_for(timeout=60000)
    page.wait_for_timeout(3000)
    section(page, "ui_detect", "Deteksaun Manual", "Istória deteksaun manual")

    page.set_viewport_size({"width": 1440, "height": 7000})
    open_page(page, "/models", "Hili model ativu")
    section(page, "ui_models_compare", "Model Random Forest (test iha meters separadu)", "Precision / Recall tuir tipu anomalia (test)")
    section(page, "ui_models_detail", "Precision / Recall tuir tipu anomalia (test)", "Retrain Random Forest")

    open_page(page, "/bigdata", "Big Data Pipeline")
    section(page, "ui_bigdata_datasets", "Big Data Pipeline")
    for tab, name in (("Spark ETL", "ui_bigdata_etl"), ("Anomalia iha dadus batch", "ui_bigdata_batch"),
                      ("Pandas vs Spark", "ui_bigdata_bench"), ("Streaming", "ui_bigdata_stream")):
        page.get_by_role("tab", name=tab).first.click()
        page.wait_for_timeout(5000)
        full_main(page, name, max_h=3000)

    open_page(page, "/simulation", "Konfigurasaun", 2500)
    full_main(page, "ui_simulation")
    open_page(page, "/users", "Kria konta foun", 3000)
    full_main(page, "ui_users", max_h=1800)
    open_page(page, "/about", "Teknolojia", 4000)
    section(page, "ui_about_arch", "Metodolojia", "3. Tipu anomalia")

    # --- Night mode: the sidebar button stores the theme and reloads; the login must survive
    page.set_viewport_size({"width": 1440, "height": 1000})
    open_page(page, "/", "Readings foun (20 ikus)", 4000)
    light_bg = app_background(page)
    first_visible(page.get_by_role("button", name="Night mode")).click()
    page.wait_for_timeout(9000)
    page.get_by_text("Readings foun (20 ikus)").first.wait_for(timeout=90000)
    page.add_style_tag(content=NO_FADE)
    page.wait_for_timeout(5000)
    dark_bg = app_background(page)
    check("Night mode", "Tema muda ba dark; login la lakon",
          is_dark_rgb(dark_bg) and not is_dark_rgb(light_bg) and logged_in(page), f"{light_bg} -> {dark_bg}")
    page.screenshot(path=str(OUT / "ui_home_dark.png"))
    print("saved ui_home_dark (window)")
    page.set_viewport_size({"width": 1440, "height": 4000})
    open_page(page, "/bigdata", "Big Data Pipeline")
    page.get_by_role("tab", name="Pandas vs Spark").first.click()
    page.wait_for_timeout(5000)
    full_main(page, "ui_bigdata_bench_dark", max_h=1700)
    page.set_viewport_size({"width": 1440, "height": 1000})
    open_page(page, "/", "Readings foun (20 ikus)", 3000)
    first_visible(page.get_by_role("button", name="Day mode")).click()
    page.wait_for_timeout(8000)
    ctx.close()

    # --- API docs + phpMyAdmin (no login)
    ctx = browser.new_context(viewport={"width": 1440, "height": 1600}, device_scale_factor=1.5)
    page = ctx.new_page()
    page.goto("http://127.0.0.1:8000/docs", wait_until="load")
    page.wait_for_timeout(3500)
    page.screenshot(path=str(OUT / "ui_api_docs.png"))
    print("saved ui_api_docs")
    try:
        page.set_viewport_size({"width": 1440, "height": 1100})
        page.goto("http://localhost/phpmyadmin/index.php?route=/database/structure&db=powerpredict_db", wait_until="load")
        page.wait_for_timeout(3000)
        page.screenshot(path=str(OUT / "ui_phpmyadmin.png"))
        print("saved ui_phpmyadmin")
    except Exception as exc:  # noqa: BLE001
        print("phpMyAdmin not captured:", exc)
    ctx.close()


def diagrams():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 800}, device_scale_factor=2)
        for f in sorted((HERE / "diagrams").glob("*.html")):
            page.goto(f.as_uri())
            page.wait_for_timeout(300)
            page.locator(".canvas").first.screenshot(path=str(OUT / f"{f.stem}.png"))
            print("saved", f.stem)
        browser.close()


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "pages"
    {"pages": pages, "diagrams": diagrams}[what]()
