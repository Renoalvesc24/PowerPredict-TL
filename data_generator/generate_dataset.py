"""
Big Data generator - smart-meter readings for Timor-Leste WITH LABELLED ANOMALIES,
scaled to a target size (100 MB, 1 GB, 5 GB ...), following the method of the
individual presentation / Exercicio 1: start from REAL seed data and scale it up.

Real seed data
  1. UCI ElectricityLoadDiagrams20112014 (Trindade, 2015, CC BY 4.0):
     real monthly consumption of 264 customers -> each synthetic meter copies
     the month-to-month pattern and consumption level of one real customer.
  2. Open-Meteo Historical Weather API: real hourly temperature for 2025 at
     the 14 municipality capitals -> drives air-conditioning load.

Scaling
  The number of meters grows until the CSV reaches the target size. Every
  meter produces one reading per hour for the whole of 2025 (8,760 rows),
  so 100 MB ~ 100 meters, 1 GB ~ 1,000 meters and 5 GB ~ 5,000 meters.
  Output is split into ~115 MB part files, like HDFS blocks.

Anomalies (ground truth)
  Six kinds of anomaly EVENT are injected and every reading is labelled with
  the event that was active (column anomaly_type):
    Voltage Sag, Over-Voltage, Frequency Deviation  - grid events (feeder / grid)
    Overload                                        - customer above contracted kVA
    Energy Theft                                    - meter bypass, registers 5-55 %
    Meter Fault                                     - current channel reads wrong
  The label comes from the injected event, NOT from a threshold on the stored
  values. The CSV stores MEASURED values (true value + sensor noise) and normal
  behaviour overlaps with some anomalies (a quiet household vs. a thief), so a
  detector has to learn the pattern from several features together.

Veracity
  0.3 % missing values, 0.2 % duplicate rows and 0.1 % invalid values are
  injected on purpose so the Spark cleaning step has real work to do.

Usage
    python -m data_generator.generate_dataset --size 5GB --workers 12
    python -m data_generator.generate_dataset --size 10GB --workers 16
    python -m data_generator.generate_dataset --size 20GB --workers 16
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import domain as D  # noqa: E402

SIZES = {"5GB": 5 * 1024**3, "10GB": 10 * 1024**3, "20GB": 20 * 1024**3, "10MB": 10 * 1024**2}  # 10MB = quick test
YEAR_START = "2025-01-01 00:00:00"
HOURS = pd.date_range(YEAR_START, periods=8760, freq="h")
N_HOURS = len(HOURS)
METERS_PER_PART = 100           # ~115 MB per part file
BYTES_PER_ROW_ESTIMATE = 118.0
COLUMNS = ["reading_id", "timestamp", "meter_id", "municipality", "customer_type",
           "contracted_kva", "phase", "voltage_v", "current_a", "power_kw", "energy_kwh",
           "power_factor", "frequency_hz", "temperature_c", "anomaly_type"]
INJECTED = {
    "Voltage Sag": "12-48 feeder events / municipality / year, -14..-35 V, 1-6 h (70 % at the evening peak)",
    "Over-Voltage": "~10 night-time events / municipality / year, +22..+34 V, 1-3 h",
    "Frequency Deviation": "38-68 generator-trip events / grid / year, 0.6-1.6 Hz away from 50 Hz, 1-3 h",
    "Overload": "~6 events / meter / year at 1.05-1.6 x contract, plus natural peaks above 100 %",
    "Energy Theft": "6 % of meters, 1-3 episodes of 3-30 days, the meter registers only 5-55 %",
    "Meter Fault": "8 % of meters, one 1-7 day current-channel fault (x0.15-0.5 or x1.8-3.0)",
}


# ---------------------------------------------------------------------------
# Real seed data
# ---------------------------------------------------------------------------
def load_weather() -> np.ndarray:
    """(14, 8760) array of real hourly temperature, municipality order of D.MUNICIPALITIES."""
    path = config.REFERENCE_DIR / "weather_hourly_2025.csv"
    if not path.exists():
        raise SystemExit("Run `python -m data_generator.fetch_weather` first (real weather seed).")
    w = pd.read_csv(path, parse_dates=["timestamp"])
    out = np.zeros((len(D.MUNICIPALITIES), N_HOURS))
    for i, name in enumerate(D.MUNICIPALITY_NAMES):
        s = w[w.municipality == name].set_index("timestamp")["temperature_c"]
        out[i] = s.reindex(HOURS).interpolate(limit_direction="both").to_numpy()
    return out


def load_uci_profiles() -> tuple[np.ndarray, np.ndarray]:
    """Real UCI customers -> (264, 12) month factors and (264,) relative level."""
    seed = config.UCI_SEED_DIR / "readings_UCI_REAL_CONSUMPTION.csv"
    if not seed.exists():
        print(f"  [warn] UCI seed not found at {seed}; using flat month factors")
        return np.ones((1, 12)), np.ones(1)
    r = pd.read_csv(seed)
    r["kwh"] = r["current_reading"] - r["previous_reading"]
    r["month"] = pd.to_datetime(r["reading_date"]).dt.month
    factors, levels = [], []
    for _, g in r.groupby("meter_serial"):
        g = g.head(12)
        if len(g) < 12 or g.kwh.mean() <= 0:
            continue
        f = (g.kwh / g.kwh.mean()).to_numpy()
        order = np.argsort(g.month.to_numpy())
        factors.append(np.clip(f[order], 0.6, 1.4))
        levels.append(g.kwh.mean())
    levels = np.array(levels)
    rel = levels / np.median(levels)
    return np.array(factors), rel


# ---------------------------------------------------------------------------
# Meter master
# ---------------------------------------------------------------------------
def build_meters(n_meters: int, rng: np.random.Generator, n_profiles: int, levels: np.ndarray) -> pd.DataFrame:
    weights = np.array([m[4] for m in D.MUNICIPALITIES], dtype=float)
    weights /= weights.sum()
    counts = np.maximum(1, np.floor(weights * n_meters).astype(int))
    while counts.sum() < n_meters:
        counts[np.argmax(weights * n_meters - counts)] += 1
    while counts.sum() > n_meters:
        counts[np.argmax(counts)] -= 1

    rows = []
    for mi, (name, capital, lat, lon, _w, grid, drop) in enumerate(D.MUNICIPALITIES):
        code = D.MUNICIPALITY_CODES[name]
        for k in range(counts[mi]):
            ctype = rng.choice(D.CUSTOMER_TYPES, p=D.CUSTOMER_TYPE_SHARE)
            kva_opts, kva_p = D.CONTRACT_KVA[ctype]
            kva = float(rng.choice(kva_opts, p=kva_p))
            phase = "three" if (ctype != "residential" or kva > 5.5) else "single"
            profile = int(rng.integers(0, n_profiles))
            level = float(np.clip(levels[profile] ** 0.25, 0.75, 1.30)) if len(levels) > 1 else 1.0
            pf_mu, pf_sd = D.POWER_FACTOR_BASE[ctype]
            pf_base = rng.normal(pf_mu, pf_sd)
            if rng.random() < 0.08:            # 8 % of meters have poor power factor
                pf_base -= rng.uniform(0.08, 0.16)
            rows.append({
                "meter_id": f"TL-{code}-{len(rows) + 1:06d}",
                "customer_name": f"{D.CUSTOMER_NAME_PREFIX[ctype]} {code}-{k + 1:05d}",
                "customer_type": ctype,
                "municipality": name,
                "municipality_idx": mi,
                "grid": grid,
                "contracted_kva": kva,
                "phase": phase,
                "latitude": round(lat + rng.normal(0, 0.03), 5),
                "longitude": round(lon + rng.normal(0, 0.03), 5),
                "seed_profile": profile,
                "peak_util": float(np.clip(D.PEAK_UTILISATION[ctype] * level * rng.lognormal(0, 0.18), 0.25, 1.05)),
                "pf_base": float(np.clip(pf_base, 0.62, 0.995)),
                "distance_k": float(rng.uniform(0.5, 1.8)),   # electrical distance from the substation
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Grid-wide state + grid events (shared by every meter of a municipality / grid)
# ---------------------------------------------------------------------------
def _ar1(rng, n, phi, sigma):
    x = np.zeros(n)
    e = rng.normal(0, sigma, n)
    for i in range(1, n):
        x[i] = phi * x[i - 1] + e[i]
    return x


def build_grid_state(seed: int, weather: np.ndarray) -> dict:
    """Feeder voltage, voltage events (sag / over-voltage) and grid-frequency events.
    Every injected event is also kept as a boolean mask = the ground-truth label."""
    rng = np.random.default_rng(seed + 7)
    n_m = len(D.MUNICIPALITIES)
    hod = HOURS.hour.to_numpy()
    res_shape = np.array(D.LOAD_SHAPES["residential"])[hod]

    feeder = np.zeros((n_m, N_HOURS))
    v_offset = np.zeros((n_m, N_HOURS))
    v_event = np.zeros((n_m, N_HOURS))
    sag = np.zeros((n_m, N_HOURS), dtype=bool)
    swell = np.zeros((n_m, N_HOURS), dtype=bool)
    for i, (name, *_rest) in enumerate(D.MUNICIPALITIES):
        temp_f = np.clip(1 + 0.012 * (weather[i] - 27), 0.85, 1.25)
        f = res_shape * temp_f
        feeder[i] = f / f.max()
        v_offset[i] = _ar1(rng, N_HOURS, 0.95, 0.6)
        n_sags = int(rng.poisson(12 if name == "Dili" else 40)) + (8 if name in ("Atauro", "Oecusse") else 0)
        for _ in range(n_sags):                        # voltage sag, mostly at the evening peak
            day = int(rng.integers(0, 365))
            start = day * 24 + (int(rng.integers(17, 22)) if rng.random() < 0.7 else int(rng.integers(0, 24)))
            dur = int(rng.integers(1, 7))
            v_event[i, start:start + dur] -= rng.uniform(14, 35)
            sag[i, start:start + dur] = True
        for _ in range(int(rng.poisson(10))):          # night-time over-voltage (light load)
            day = int(rng.integers(0, 365))
            start = day * 24 + int(rng.integers(0, 5))
            dur = int(rng.integers(1, 4))
            v_event[i, start:start + dur] += rng.uniform(22, 34)
            swell[i, start:start + dur] = True

    freq = np.zeros((len(D.GRIDS), N_HOURS))
    freq_ev = np.zeros((len(D.GRIDS), N_HOURS), dtype=bool)
    spec = {"main": (0.07, 30, 8), "oecusse": (0.13, 40, 12), "atauro": (0.18, 50, 18)}
    for g, name in enumerate(D.GRIDS):
        sigma, n_mod, n_sev = spec[name]
        freq[g] = D.NOMINAL_FREQUENCY + np.clip(_ar1(rng, N_HOURS, 0.6, sigma), -0.42, 0.42)
        for _ in range(n_mod):                         # generator trip / load rejection
            t = int(rng.integers(0, N_HOURS))
            d = int(rng.integers(1, 4))
            freq[g, t:t + d] = D.NOMINAL_FREQUENCY + rng.choice([-1, 1], p=[0.8, 0.2]) * rng.uniform(0.6, 0.95)
            freq_ev[g, t:t + d] = True
        for _ in range(n_sev):                         # severe under-frequency event
            t = int(rng.integers(0, N_HOURS))
            freq[g, t:t + 1] = D.NOMINAL_FREQUENCY - rng.uniform(1.05, 1.6)
            freq_ev[g, t:t + 1] = True
    return {"feeder": feeder, "v_offset": v_offset, "v_event": v_event, "sag": sag, "swell": swell,
            "freq": freq, "freq_ev": freq_ev}


def meter_episodes(rng, m: int, share: float, n_range: tuple[int, int], days: tuple[float, float]) -> list:
    """(meter, start_hour, end_hour) episodes for `share` of the meters."""
    out = []
    for i in np.flatnonzero(rng.random(m) < share):
        for _ in range(int(rng.integers(n_range[0], n_range[1] + 1))):
            length = max(1, int(rng.uniform(*days) * 24))
            start = int(rng.integers(0, max(1, N_HOURS - length)))
            out.append((int(i), start, start + length))
    return out


# ---------------------------------------------------------------------------
# Worker: one part file (~100 meters x 8,760 hours)
# ---------------------------------------------------------------------------
_W: dict = {}


def _init_worker(seed: int):
    weather = load_weather()
    month_f, levels = load_uci_profiles()
    _W.update(weather=weather, month_f=month_f, levels=levels, grid=build_grid_state(seed, weather), seed=seed)


def generate_part(part_no: int, meters: pd.DataFrame, out_dir: str, row_offset: int) -> dict:
    t0 = time.time()
    rng = np.random.default_rng(_W["seed"] * 1000 + part_no)
    weather, grid, month_f = _W["weather"], _W["grid"], _W["month_f"]
    m = len(meters)
    hod = HOURS.hour.to_numpy()
    dow = HOURS.dayofweek.to_numpy()
    month = HOURS.month.to_numpy()
    weekend = dow >= 5

    mi = meters.municipality_idx.to_numpy()
    gi = meters.grid.map({g: i for i, g in enumerate(D.GRIDS)}).to_numpy()
    ctype = meters.customer_type.to_numpy()
    kva = meters.contracted_kva.to_numpy()[:, None]

    # ---- normal consumption (real UCI month pattern + real Open-Meteo temperature) ----
    shape = np.stack([np.array(D.LOAD_SHAPES[t])[hod] for t in ctype])
    wkf = np.stack([np.where(weekend, D.WEEKEND_FACTOR[t], 1.0) for t in ctype])
    beta = np.array([D.TEMPERATURE_SENSITIVITY[t] for t in ctype])[:, None]
    temp = weather[mi]
    temp_f = np.clip(1 + beta * (temp - 27), 0.85, 1.45)
    mf = month_f[meters.seed_profile.to_numpy() % len(month_f)][:, month - 1]
    noise = rng.lognormal(0, 0.15, (m, N_HOURS))
    spikes = np.where(rng.random((m, N_HOURS)) < 0.010, rng.uniform(1.25, 1.9, (m, N_HOURS)), 1.0)
    s_kva = kva * meters.peak_util.to_numpy()[:, None] * shape * wkf * temp_f * mf * noise * spikes

    # ---- anomaly: overload events (customer draws more than the contract) ----
    for i in range(m):
        for _ in range(int(rng.poisson(6))):
            t = int(rng.integers(0, N_HOURS))
            d = int(rng.integers(1, 6))
            s_kva[i, t:t + d] = np.maximum(s_kva[i, t:t + d], kva[i, 0] * rng.uniform(1.05, 1.6))
    load_ratio = s_kva / kva
    overload = load_ratio > 1.0

    pf_true = np.clip(meters.pf_base.to_numpy()[:, None] + rng.normal(0, 0.015, (m, N_HOURS)), 0.55, 1.0)
    p_true = s_kva * pf_true

    # ---- anomalies: voltage sag, over-voltage, frequency deviation (grid events) ----
    drop = np.array([D.MUNICIPALITIES[i][6] for i in mi])[:, None] * D.FEEDER_DROP_SCALE
    v_true = (224.0 + grid["v_offset"][mi]
              - D.NOMINAL_VOLTAGE * drop * grid["feeder"][mi] * meters.distance_k.to_numpy()[:, None]
              - 3.0 * np.clip(load_ratio, 0, 1.6) + grid["v_event"][mi] + rng.normal(0, 0.8, (m, N_HOURS)))
    f_true = grid["freq"][gi] + rng.normal(0, 0.01, (m, N_HOURS))
    div = D.phase_divisor(meters.phase.to_numpy())[:, None]
    i_true = p_true * 1000 / (v_true * pf_true) / div

    # ---- anomaly: energy theft (meter bypass registers only 5-40 % for days / weeks) ----
    theft_k = np.ones((m, N_HOURS))
    for i, a, b in meter_episodes(rng, m, 0.06, (1, 3), (3, 30)):
        theft_k[i, a:b] = rng.uniform(0.05, 0.55)
    # ---- anomaly: meter fault (current channel reads wrong for 1-7 days) ----
    fault_k = np.ones((m, N_HOURS))
    for i, a, b in meter_episodes(rng, m, 0.08, (1, 1), (1, 7)):
        fault_k[i, a:b] = rng.uniform(0.15, 0.5) if rng.random() < 0.5 else rng.uniform(1.8, 3.0)

    label = D.assign_label({
        "Meter Fault": fault_k != 1.0, "Energy Theft": theft_k < 1.0,
        "Frequency Deviation": grid["freq_ev"][gi], "Voltage Sag": grid["sag"][mi],
        "Over-Voltage": grid["swell"][mi], "Overload": overload,
    }, (m, N_HOURS))

    # ---- what the meter reports = true value x tampering / fault + sensor noise ----
    v_meas = np.round(v_true + rng.normal(0, 1.2, (m, N_HOURS)), 2)
    i_meas = np.round(i_true * theft_k * fault_k * rng.normal(1, 0.01, (m, N_HOURS)), 2)
    p_meas = np.round(p_true * theft_k * rng.normal(1, 0.01, (m, N_HOURS)), 3)
    pf_meas = np.round(np.clip(pf_true + rng.normal(0, 0.008, (m, N_HOURS)), 0.3, 1.0), 3)
    f_meas = np.round(f_true + rng.normal(0, 0.02, (m, N_HOURS)), 2)
    t_meas = np.round(temp + rng.normal(0, 0.3, (m, N_HOURS)), 1)

    def flat(a):                     # time-major order inside the part (like a log of arrivals)
        return np.ascontiguousarray(a.T).reshape(-1)

    n = m * N_HOURS
    meter_rep = np.tile(np.arange(m), N_HOURS)
    hour_rep = np.repeat(np.arange(N_HOURS), m)
    df = pd.DataFrame({
        "reading_id": (row_offset + meter_rep * N_HOURS + hour_rep + 1).astype(np.int64),
        "timestamp": HOURS.values[hour_rep],
        "meter_id": meters.meter_id.to_numpy()[meter_rep],
        "municipality": meters.municipality.to_numpy()[meter_rep],
        "customer_type": ctype[meter_rep],
        "contracted_kva": meters.contracted_kva.to_numpy()[meter_rep],
        "phase": meters.phase.to_numpy()[meter_rep],
        "voltage_v": flat(v_meas),
        "current_a": flat(i_meas),
        "power_kw": flat(p_meas),
        "energy_kwh": flat(p_meas),          # hourly reading: kWh in the hour = average kW
        "power_factor": flat(pf_meas),
        "frequency_hz": flat(f_meas),
        "temperature_c": flat(t_meas),
        "anomaly_type": np.array(D.ANOMALY_TYPES, dtype=object)[flat(label)],
    })

    # ---- inject data-quality problems (veracity) ----
    idx = rng.choice(n, int(n * 0.003), replace=False)
    for col, part in zip(["voltage_v", "current_a", "power_factor", "temperature_c"], np.array_split(idx, 4)):
        df.loc[part, col] = np.nan
    idx = rng.choice(n, int(n * 0.001), replace=False)
    a, b, c, d = np.array_split(idx, 4)
    df.loc[a, "voltage_v"] = 0.0
    df.loc[b, "voltage_v"] = 999.9
    df.loc[c, "power_factor"] = 1.35
    df.loc[d, "power_kw"] = -df.loc[d, "power_kw"]
    dup_idx = rng.choice(n, int(n * 0.002), replace=False)
    df = pd.concat([df, df.iloc[dup_idx]], ignore_index=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.set_column(table.schema.get_field_index("timestamp"), "timestamp",
                             table.column("timestamp").cast(pa.timestamp("s")))
    out = Path(out_dir) / f"part-{part_no:05d}.csv"
    pacsv.write_csv(table, out, pacsv.WriteOptions(include_header=True, quoting_style="none"))
    counts = pd.Series(df["anomaly_type"]).value_counts().to_dict()
    return {"part": part_no, "rows": int(len(df)), "bytes": out.stat().st_size,
            "meters": m, "seconds": round(time.time() - t0, 2), "anomaly_counts": counts}


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Generate the PowerPredict TL big dataset (with anomalies)")
    ap.add_argument("--size", default="5GB", help="target size label: " + ", ".join(SIZES))
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--meters", type=int, default=0, help="override the number of meters")
    args = ap.parse_args()

    label = args.size.upper()
    target = SIZES.get(label)
    if target is None:
        raise SystemExit(f"Unknown size {args.size}; choose one of {list(SIZES)}")

    t0 = time.time()
    rng = np.random.default_rng(args.seed)
    month_f, levels = load_uci_profiles()
    n_meters = args.meters or max(14, math.ceil(target / (N_HOURS * BYTES_PER_ROW_ESTIMATE)))
    meters = build_meters(n_meters, rng, len(month_f), levels)

    out_dir = config.RAW_DIR / f"readings_{label}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    config.REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    meters.drop(columns=["municipality_idx"]).to_csv(config.REFERENCE_DIR / f"meters_{label}.csv",
                                                      index=False, encoding="utf-8")

    parts = [meters.iloc[i:i + METERS_PER_PART].reset_index(drop=True)
             for i in range(0, len(meters), METERS_PER_PART)]
    print(f"[{label}] {n_meters:,} meters x {N_HOURS:,} hours -> {len(parts)} part files, {args.workers} workers")
    results = []
    with ProcessPoolExecutor(max_workers=min(args.workers, len(parts)),
                             initializer=_init_worker, initargs=(args.seed,)) as ex:
        futs = [ex.submit(generate_part, i, p, str(out_dir), i * METERS_PER_PART * N_HOURS) for i, p in enumerate(parts)]
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            done = sum(x["bytes"] for x in results)
            print(f"  part {r['part']:05d}: {r['rows']:,} rows, {r['bytes']/1e6:.1f} MB, {r['seconds']}s "
                  f"(total {done/1024**2:,.0f} MB)")

    results.sort(key=lambda r: r["part"])
    total_rows = sum(r["rows"] for r in results)
    total_bytes = sum(r["bytes"] for r in results)
    counts: dict[str, int] = {}
    for r in results:
        for k, v in r["anomaly_counts"].items():
            counts[k] = counts.get(k, 0) + int(v)
    manifest = {
        "label": label, "target_bytes": target, "bytes": total_bytes,
        "size_mb": round(total_bytes / 1024**2, 1), "rows": total_rows, "meters": n_meters,
        "hours": N_HOURS, "period": [str(HOURS[0]), str(HOURS[-1])], "parts": len(results),
        "columns": COLUMNS, "anomaly_counts": counts,
        "anomaly_pct": {k: round(v / total_rows * 100, 3) for k, v in sorted(counts.items(), key=lambda kv: -kv[1])},
        "seed_data": ["UCI ElectricityLoadDiagrams20112014 (real monthly consumption, 264 customers)",
                      "Open-Meteo Historical Weather API (real hourly temperature 2025, 14 capitals)"],
        "injected_anomalies": INJECTED,
        "injected_quality_issues": {"missing": "0.3%", "invalid": "0.1%", "duplicates": "0.2%"},
        "generated_at": pd.Timestamp.now(tz=config.TIMEZONE).isoformat(),
        "seconds": round(time.time() - t0, 1),
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("label", "size_mb", "rows", "meters", "parts", "anomaly_pct", "seconds")},
                     indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
