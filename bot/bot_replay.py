"""
bot_replay.py - replay the big dataset (data lake, raw CSV) through the API

Reads a raw part file of the 5GB / 10GB / 20GB dataset in time order and POSTs
the rows to the FastAPI gateway in batches, as if the meters were sending them
now. Timestamps are kept (the model's hour / weekend features must match the
values), so replayed readings show 2025 dates; the anomaly label of the dataset
travels as `true_type` for evaluation.

    python -m bot.bot_replay --dataset 5GB --rate 500
    python -m bot.bot_replay --file data/samples/sample_readings.csv --rate 20
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

FIELDS = ["meter_id", "timestamp", "municipality", "customer_type", "contracted_kva", "phase", "voltage_v",
          "current_a", "power_kw", "energy_kwh", "power_factor", "frequency_hz", "temperature_c"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    ap.add_argument("--dataset", default="5GB")
    ap.add_argument("--file", help="replay this CSV instead of the dataset's first part file")
    ap.add_argument("--rate", type=float, default=300, help="readings per second")
    ap.add_argument("--batch", type=int, default=300)
    ap.add_argument("--limit", type=int, default=0, help="stop after N readings (0 = whole file)")
    args = ap.parse_args()

    path = Path(args.file) if args.file else sorted((config.RAW_DIR / f"readings_{args.dataset}").glob("part-*.csv"))[0]
    s = requests.Session()
    sent = 0
    print(f"[replay] {path} -> {args.api}/api/readings/batch at {args.rate:.0f} readings/s", flush=True)
    for chunk in pd.read_csv(path, chunksize=args.batch):
        t0 = time.time()
        chunk = chunk.astype(object).where(chunk.notna(), None)
        readings = [{**{k: r[k] for k in FIELDS}, "reading_id": f"replay-{r.get('reading_id', uuid.uuid4().hex)}",
                     "true_type": r.get("anomaly_type"), "producer_ts": int(time.time() * 1000)}
                    for r in chunk.to_dict("records")]
        r = s.post(f"{args.api}/api/readings/batch", json={"bot_name": "bot-replay", "readings": readings}, timeout=30)
        r.raise_for_status()
        sent += len(readings)
        if sent % (args.batch * 10) == 0:
            print(f"[replay] {sent:,} readings sent", flush=True)
        if args.limit and sent >= args.limit:
            break
        time.sleep(max(0.0, len(readings) / args.rate - (time.time() - t0)))
    print(f"[replay] done: {sent:,} readings", flush=True)


if __name__ == "__main__":
    main()
