"""
Create the PowerPredict TL database (MySQL on XAMPP), all tables and the
reference data (municipalities + meter master + default settings).

    python -m setup.init_db                 # uses the 5GB meter master
    python -m setup.init_db --reset         # DROP the database first (fresh start)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, delete, func, insert, select, text
from sqlalchemy.engine import make_url

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import db  # noqa: E402
from common.domain import MUNICIPALITIES  # noqa: E402

DEFAULT_SETTINGS = {
    "scenario": "normal",                 # live demo scenario (Simulasaun page)
    "trigger": "",                        # one-shot event trigger (theft wave, meter faults)
    "producer_interval_s": str(config.PRODUCER_INTERVAL_SECONDS),
    "alert_min_score": "0.60",            # anomaly score needed to raise an event
}


def drop_database() -> None:
    url = make_url(config.DB_URL)
    server = create_engine(url._replace(database=None), future=True)
    with server.begin() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{url.database}`"))
    server.dispose()
    print(f"  dropped database {url.database}")


def seed_municipalities() -> None:
    with db.begin() as conn:
        conn.execute(delete(db.municipalities))
        conn.execute(insert(db.municipalities), [
            {"name": n, "capital": c, "latitude": la, "longitude": lo, "population_weight": w,
             "grid": g, "voltage_drop": d}
            for n, c, la, lo, w, g, d in MUNICIPALITIES
        ])
    print(f"  {len(MUNICIPALITIES)} municipalities")


def seed_meters(label: str, fleet: int) -> None:
    path = config.REFERENCE_DIR / f"meters_{label}.csv"
    if not path.exists():
        print(f"  [skip] {path.name} not found - run the data generator first")
        return
    m = pd.read_csv(path)
    # live fleet: a stratified sample (every municipality represented) streams in real time
    m["is_live"] = False
    per_muni = max(4, fleet // m.municipality.nunique())
    live_idx = m.groupby("municipality", group_keys=False).apply(lambda g: g.head(per_muni)).index
    m.loc[live_idx[:fleet], "is_live"] = True
    cols = ["meter_id", "customer_name", "customer_type", "municipality", "contracted_kva", "phase",
            "latitude", "longitude", "seed_profile", "peak_util", "pf_base", "distance_k", "is_live"]
    with db.begin() as conn:
        conn.execute(delete(db.meters))
    db.bulk_insert(db.meters, m[cols].to_dict("records"))
    print(f"  {len(m):,} meters from {path.name} ({int(m.is_live.sum())} in the live fleet)")


def seed_settings() -> None:
    for k, v in DEFAULT_SETTINGS.items():
        if db.get_setting(k) is None:
            db.set_setting(k, v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meters", default="5GB", help="which meters_<label>.csv to load")
    ap.add_argument("--fleet", type=int, default=config.LIVE_FLEET_SIZE)
    ap.add_argument("--reset", action="store_true", help="drop and recreate the database")
    args = ap.parse_args()
    print(f"Database: {config.DB_URL.split('@')[-1]}")
    if args.reset:
        drop_database()
    db.create_all()
    print(f"  {len(db.metadata.tables)} tables ready")
    seed_municipalities()
    seed_meters(args.meters, args.fleet)
    seed_settings()
    with db.begin() as conn:
        n = conn.execute(select(func.count()).select_from(db.meters)).scalar()
    print(f"Done. tbl_meters has {n:,} rows.")


if __name__ == "__main__":
    main()
