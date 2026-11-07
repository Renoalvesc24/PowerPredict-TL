"""
bot_live.py - real-time data source: a fleet of simulated smart meters -> FastAPI

Every tick (default 2 s) each live meter (tbl_meters.is_live, fetched from the
API) produces one reading. The bot POSTs the whole tick as one batch to
POST /api/readings/batch; the API publishes it to Kafka.

* Load, voltage and frequency follow the same grid model as the big dataset
  (common.domain + data_generator.generate_dataset).
* Temperature comes LIVE from the Open-Meteo API (14 capitals, cached 15 min,
  falls back to the 2025 archive when offline).
* Anomalies happen on their own (state machines per meter / municipality / grid):
  Voltage Sag, Over-Voltage, Frequency Deviation, Overload, Energy Theft, Meter Fault.
  Every reading carries `true_type` = ground truth, used ONLY to evaluate the model.
* The Simulasaun page of the dashboard can force scenarios (voltage_sag:<muni>,
  over_voltage:<muni>, generator_trip, overload_wave) or one-shot triggers
  (theft:<n>, meter_fault:<n>).
* 0.5 % of readings are deliberately broken so Spark's validation has work to do.

    python -m bot.bot_live
    python -m bot.bot_live --api http://localhost:8000 --interval 1
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import domain as D  # noqa: E402

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
RUNNING = True


def _stop(*_):
    global RUNNING
    RUNNING = False


class Weather:
    """Live temperature per municipality from Open-Meteo, cached 15 minutes."""

    def __init__(self):
        self.temps: dict[str, float] = {}
        self.fetched = 0.0
        self.source = "none"
        path = config.REFERENCE_DIR / "weather_hourly_2025.csv"
        self.archive = pd.read_csv(path, parse_dates=["timestamp"]) if path.exists() else None

    def get(self) -> dict[str, float]:
        if time.time() - self.fetched < 900 and self.temps:
            return self.temps
        try:
            r = requests.get(OPEN_METEO, timeout=10, params={
                "latitude": ",".join(str(m[2]) for m in D.MUNICIPALITIES),
                "longitude": ",".join(str(m[3]) for m in D.MUNICIPALITIES),
                "current": "temperature_2m", "timezone": config.TIMEZONE})
            r.raise_for_status()
            data = r.json()
            data = data if isinstance(data, list) else [data]
            self.temps = {m[0]: float(d["current"]["temperature_2m"]) for m, d in zip(D.MUNICIPALITIES, data)}
            self.source = "open-meteo live"
        except Exception as exc:                                 # offline -> same month/hour of 2025
            now = datetime.now()
            if self.archive is not None:
                a = self.archive[(self.archive.timestamp.dt.month == now.month) & (self.archive.timestamp.dt.hour == now.hour)]
                self.temps = a.groupby("municipality")["temperature_c"].mean().round(1).to_dict()
            else:
                self.temps = {m[0]: 27.0 for m in D.MUNICIPALITIES}
            self.source = f"archive fallback ({type(exc).__name__})"
        self.fetched = time.time()
        return self.temps


class Events:
    """Anomaly state machines. *_left = remaining ticks of an active event."""

    def __init__(self, fleet: pd.DataFrame, rng):
        n = len(fleet)
        self.rng = rng
        self.fleet = fleet
        self.theft_left, self.theft_k = np.zeros(n, int), np.ones(n)
        self.over_left, self.over_target = np.zeros(n, int), np.ones(n)
        self.fault_left, self.fault_k = np.zeros(n, int), np.ones(n)
        self.sag = {m: [0, 0.0] for m in D.MUNICIPALITY_NAMES}        # ticks left, depth (V)
        self.swell = {m: [0, 0.0] for m in D.MUNICIPALITY_NAMES}
        self.trip = {g: [0, 0.0] for g in D.GRIDS}                   # ticks left, Hz offset
        self.v_offset = {m: 0.0 for m in D.MUNICIPALITY_NAMES}
        self.freq = {g: D.NOMINAL_FREQUENCY for g in D.GRIDS}
        self.last_trigger = None

    def start_theft(self, idx):
        self.theft_left[idx] = self.rng.integers(150, 900, len(idx))
        self.theft_k[idx] = self.rng.uniform(0.05, 0.55, len(idx))

    def start_fault(self, idx):
        self.fault_left[idx] = self.rng.integers(60, 300, len(idx))
        low = self.rng.random(len(idx)) < 0.5
        self.fault_k[idx] = np.where(low, self.rng.uniform(0.15, 0.5, len(idx)), self.rng.uniform(1.8, 3.0, len(idx)))

    def step(self, scenario: str, trigger: str):
        r, n = self.rng, len(self.fleet)
        for m in self.v_offset:
            self.v_offset[m] = 0.95 * self.v_offset[m] + r.normal(0, 0.6)
        for g, sd in (("main", 0.05), ("oecusse", 0.09), ("atauro", 0.12)):
            self.freq[g] = D.NOMINAL_FREQUENCY + float(np.clip(0.6 * (self.freq[g] - 50) + r.normal(0, sd), -0.42, 0.42))
        # meter events
        free = (self.theft_left == 0) & (self.fault_left == 0)
        self.start_theft(np.flatnonzero(free & (r.random(n) < 6e-5)))
        self.start_fault(np.flatnonzero(free & (r.random(n) < 4e-5)))
        new_over = np.flatnonzero((self.over_left == 0) & (r.random(n) < 2e-4))
        self.over_left[new_over] = r.integers(15, 90, len(new_over))
        self.over_target[new_over] = r.uniform(1.05, 1.6, len(new_over))
        # grid events
        for m in D.MUNICIPALITY_NAMES:
            if self.sag[m][0] == 0 and r.random() < 6e-4:
                self.sag[m] = [int(r.integers(10, 45)), float(r.uniform(14, 35))]
            if self.swell[m][0] == 0 and r.random() < 3e-4:
                self.swell[m] = [int(r.integers(10, 30)), float(r.uniform(22, 34))]
        for g in D.GRIDS:
            if self.trip[g][0] == 0 and r.random() < 8e-4:
                self.trip[g] = [int(r.integers(5, 15)), float(r.choice([-1, 1], p=[0.8, 0.2]) * r.uniform(0.6, 1.5))]
        # one-shot triggers from the dashboard: "theft:10:<nonce>" / "meter_fault:5:<nonce>"
        if trigger and trigger != self.last_trigger:
            self.last_trigger = trigger
            kind, cnt = (trigger.split(":") + ["", "5"])[:2]
            cand = np.flatnonzero(free)
            pick = r.choice(cand, min(int(cnt or 5), len(cand)), replace=False) if len(cand) else []
            if kind == "theft":
                self.start_theft(np.asarray(pick, dtype=int))
            elif kind == "meter_fault":
                self.start_fault(np.asarray(pick, dtype=int))
        # persistent scenarios
        if scenario.startswith("voltage_sag"):
            m = scenario.split(":", 1)[1] if ":" in scenario else "Oecusse"
            self.sag[m] = [max(self.sag[m][0], 2), self.sag[m][1] or float(r.uniform(22, 32))]
        elif scenario.startswith("over_voltage"):
            m = scenario.split(":", 1)[1] if ":" in scenario else "Dili"
            self.swell[m] = [max(self.swell[m][0], 2), self.swell[m][1] or float(r.uniform(24, 32))]
        elif scenario == "generator_trip":
            self.trip["main"] = [max(self.trip["main"][0], 2), self.trip["main"][1] or -float(r.uniform(1.05, 1.4))]
        elif scenario == "overload_wave":
            pick = np.flatnonzero((self.over_left == 0) & (r.random(n) < 0.02))
            self.over_left[pick] = r.integers(10, 40, len(pick))
            self.over_target[pick] = r.uniform(1.1, 1.6, len(pick))

    def tick_down(self):
        for arr in (self.theft_left, self.over_left, self.fault_left):
            np.subtract(arr, 1, out=arr, where=arr > 0)
        self.theft_k[self.theft_left == 0] = 1.0
        self.fault_k[self.fault_left == 0] = 1.0
        for d in (self.sag, self.swell, self.trip):
            for k, v in d.items():
                if v[0] > 0:
                    v[0] -= 1
                if v[0] == 0:
                    v[1] = 0.0


def make_readings(fleet: pd.DataFrame, ev: Events, temps: dict, rng, interval_s: float) -> list[dict]:
    now = datetime.now()
    hour, weekend = now.hour, now.weekday() >= 5
    n = len(fleet)
    ctype = fleet.customer_type.to_numpy()
    kva = fleet.contracted_kva.to_numpy(dtype=float)
    muni = fleet.municipality.to_numpy()
    grid = fleet.grid.to_numpy()
    shape = D.SHAPE_TABLE[[D.TYPE_INDEX[t] for t in ctype], hour]
    wkf = np.array([D.WEEKEND_FACTOR[t] if weekend else 1.0 for t in ctype])
    temp = np.array([temps.get(m, 27.0) for m in muni], dtype=float)
    beta = np.array([D.TEMPERATURE_SENSITIVITY[t] for t in ctype])
    temp_f = np.clip(1 + beta * (temp - 27), 0.85, 1.45)
    noise = rng.lognormal(0, 0.12, n)
    spikes = np.where(rng.random(n) < 0.01, rng.uniform(1.25, 1.8, n), 1.0)
    s_kva = kva * fleet.peak_util.to_numpy(dtype=float) * shape * wkf * temp_f * noise * spikes
    s_kva = np.where(ev.over_left > 0, np.maximum(s_kva, kva * ev.over_target), s_kva)
    load_ratio = s_kva / kva
    pf = np.clip(fleet.pf_base.to_numpy(dtype=float) + rng.normal(0, 0.015, n), 0.55, 1.0)
    p = s_kva * pf

    feeder = D.LOAD_SHAPES["residential"][hour]
    drop = np.array([dict((m[0], m[6]) for m in D.MUNICIPALITIES)[x] for x in muni]) * D.FEEDER_DROP_SCALE
    sag = np.array([ev.sag[m][1] if ev.sag[m][0] > 0 else 0.0 for m in muni])
    swell = np.array([ev.swell[m][1] if ev.swell[m][0] > 0 else 0.0 for m in muni])
    v = (224.0 + np.array([ev.v_offset[m] for m in muni]) - D.NOMINAL_VOLTAGE * drop * feeder * fleet.distance_k.to_numpy(dtype=float)
         - 3.0 * np.clip(load_ratio, 0, 1.6) - sag + swell + rng.normal(0, 0.8, n))
    trip = np.array([ev.trip[g][1] if ev.trip[g][0] > 0 else 0.0 for g in grid])
    f = np.array([ev.freq[g] for g in grid]) + trip + rng.normal(0, 0.01, n)
    div = D.phase_divisor(fleet.phase.to_numpy())
    i = p * 1000 / (v * pf) / div

    label = D.assign_label({
        "Meter Fault": ev.fault_left > 0, "Energy Theft": ev.theft_left > 0, "Frequency Deviation": trip != 0,
        "Voltage Sag": sag > 0, "Over-Voltage": swell > 0, "Overload": load_ratio > 1.0}, n)

    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    sent_ms = int(time.time() * 1000)
    tk, fk = ev.theft_k, ev.fault_k
    out = []
    for k in range(n):
        rec = {
            "reading_id": uuid.uuid4().hex, "meter_id": fleet.meter_id.iat[k], "timestamp": ts,
            "municipality": muni[k], "customer_type": ctype[k], "contracted_kva": float(kva[k]),
            "phase": fleet.phase.iat[k],
            "voltage_v": round(float(v[k] + rng.normal(0, 1.2)), 2),
            "current_a": round(float(i[k] * tk[k] * fk[k] * rng.normal(1, 0.01)), 2),
            "power_kw": round(float(p[k] * tk[k] * rng.normal(1, 0.01)), 3),
            "energy_kwh": round(float(p[k] * tk[k] * interval_s / 3600), 5),
            "power_factor": round(float(np.clip(pf[k] + rng.normal(0, 0.008), 0.3, 1.0)), 3),
            "frequency_hz": round(float(f[k] + rng.normal(0, 0.02)), 2),
            "temperature_c": round(float(temp[k] + rng.normal(0, 0.3)), 1),
            "true_type": D.ANOMALY_TYPES[int(label[k])],
            "producer_ts": sent_ms,
        }
        x = rng.random()
        if x < 0.002:
            rec["voltage_v"] = None                 # missing value
        elif x < 0.004:
            rec["voltage_v"] = 0.0                  # invalid value
        elif x < 0.005:
            rec["power_factor"] = 1.4               # invalid value
        out.append(rec)
    return out


def get_json(session, url, retries=30):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as exc:  # API not up yet
            print(f"[bot] waiting for API ({exc.__class__.__name__}) {attempt + 1}/{retries}", flush=True)
            time.sleep(2)
    raise SystemExit(f"API not reachable: {url}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    ap.add_argument("--interval", type=float, default=None, help="seconds between ticks (default: dashboard setting)")
    ap.add_argument("--ticks", type=int, default=0, help="stop after N ticks (0 = forever)")
    ap.add_argument("--name", default="bot-live")
    args = ap.parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    s = requests.Session()
    fleet = pd.DataFrame(get_json(s, f"{args.api}/api/meters/live")["meters"])
    if fleet.empty:
        raise SystemExit("No live meters - run `python -m setup.init_db` first.")
    fleet["grid"] = fleet.municipality.map(D.MUNICIPALITY_GRID)
    rng = np.random.default_rng()
    ev, weather = Events(fleet, rng), Weather()
    # A trigger stored before this start was already used: only fire new ones.
    ev.last_trigger = get_json(s, f"{args.api}/api/scenario", retries=5).get("trigger") or None
    sent, tick, t_start = 0, 0, time.time()
    print(f"[bot] {len(fleet)} meters -> {args.api}/api/readings/batch", flush=True)
    while RUNNING:
        t0 = time.time()
        cfg = get_json(s, f"{args.api}/api/scenario", retries=5)
        interval = args.interval or cfg["interval_s"]
        ev.step(cfg["scenario"], cfg["trigger"])
        readings = make_readings(fleet, ev, weather.get(), rng, interval)
        ev.tick_down()
        try:
            r = s.post(f"{args.api}/api/readings/batch", timeout=15,
                       json={"bot_name": args.name, "scenario": cfg["scenario"], "readings": readings})
            r.raise_for_status()
            sent += len(readings)
        except Exception as exc:  # API or Kafka down: keep running, try again next tick
            print(f"[bot] post failed: {exc}", flush=True)
        tick += 1
        if tick % 5 == 1:
            anomalies = sum(1 for x in readings if x["true_type"] != D.NORMAL)
            print(f"[bot] tick {tick}: {len(readings)} readings ({anomalies} anomalous), total {sent:,} "
                  f"({sent / max(time.time() - t_start, 1e-6):.0f}/s) scenario={cfg['scenario']} weather={weather.source}",
                  flush=True)
        if args.ticks and tick >= args.ticks:
            break
        time.sleep(max(0.0, interval - (time.time() - t0)))
    print("[bot] stopped", flush=True)


if __name__ == "__main__":
    main()
