"""
Download REAL hourly weather for 2025 at the 14 municipality capitals
from the Open-Meteo Historical Weather API (REST API, open data, CC BY 4.0).

    python -m data_generator.fetch_weather

Output: data/raw/reference/weather_hourly_2025.csv
        (municipality, timestamp, temperature_c, humidity_pct)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common.domain import MUNICIPALITIES  # noqa: E402

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OUT_FILE = config.REFERENCE_DIR / "weather_hourly_2025.csv"


def fetch(start: str, end: str, retries: int = 4) -> pd.DataFrame:
    params = {
        "latitude": ",".join(str(m[2]) for m in MUNICIPALITIES),
        "longitude": ",".join(str(m[3]) for m in MUNICIPALITIES),
        "start_date": start,
        "end_date": end,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": config.TIMEZONE,
    }
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(ARCHIVE_URL, params=params, timeout=90)
            resp.raise_for_status()
            payload = resp.json()
            break
        except Exception as exc:  # network errors, 429, 5xx
            last_error = exc
            print(f"  attempt {attempt} failed: {exc}")
            time.sleep(5 * attempt)
    else:
        raise RuntimeError(f"Open-Meteo archive request failed: {last_error}")

    if isinstance(payload, dict):
        payload = [payload]
    frames = []
    for (name, *_), item in zip(MUNICIPALITIES, payload):
        hourly = item["hourly"]
        frames.append(pd.DataFrame({
            "municipality": name,
            "timestamp": pd.to_datetime(hourly["time"]),
            "temperature_c": hourly["temperature_2m"],
            "humidity_pct": hourly["relative_humidity_2m"],
        }))
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default="2025-12-31")
    args = ap.parse_args()

    print(f"Fetching Open-Meteo archive {args.start} .. {args.end} for {len(MUNICIPALITIES)} capitals")
    df = fetch(args.start, args.end)
    # Fill the odd missing hour by interpolation inside each municipality
    df = df.sort_values(["municipality", "timestamp"])
    df["temperature_c"] = df.groupby("municipality")["temperature_c"].transform(
        lambda s: s.interpolate(limit_direction="both"))
    df["humidity_pct"] = df.groupby("municipality")["humidity_pct"].transform(
        lambda s: s.interpolate(limit_direction="both"))
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")
    meta = {
        "source": "Open-Meteo Historical Weather API (archive-api.open-meteo.com)",
        "licence": "CC BY 4.0",
        "rows": int(len(df)),
        "period": [args.start, args.end],
        "fetched_at": pd.Timestamp.now(tz=config.TIMEZONE).isoformat(),
    }
    (config.REFERENCE_DIR / "weather_hourly_2025.meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Saved {len(df):,} rows -> {OUT_FILE}")
    print(df.groupby("municipality")["temperature_c"].agg(["min", "mean", "max"]).round(1))


if __name__ == "__main__":
    main()
