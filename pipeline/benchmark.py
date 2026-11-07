"""
Scalability test: Pandas (traditional, scale-up) vs Apache Spark (big data,
scale-out) on the SAME workload - the method of the individual presentation
"Why and when should we move from traditional to big data processing?".

Volumes   : 10k, 50k, 100k, 500k, 1M rows (capstone manual, section 14.D)
            + full 5 GB, 10 GB and 20 GB datasets
Workload  : read CSV -> clean (missing, duplicates, invalid) ->
            total kWh per municipality, top-10 meters, anomaly-type distribution,
            average voltage per customer type
Fairness  : every run is a fresh process (no warm JVM, no warm cache);
            both engines must return the same row count and total kWh.

    python -m pipeline.benchmark                 # all volumes, 3 runs
    python -m pipeline.benchmark --volumes 10k 5GB --runs 1

Memory guard: a worker that goes above 85 % of the RAM (or leaves < 0.5 GB free) is
stopped and stored as 'FAILED: out of memory' - Pandas must hold the whole dataset in
RAM, Spark processes it partition by partition and spills to disk.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

SUBSET_ROWS = {"10k": 10_000, "50k": 50_000, "100k": 100_000, "500k": 500_000, "1M": 1_000_000}
FULL = ["5GB", "10GB", "20GB"]
MEM_LIMIT_FRACTION = 0.85
MIN_FREE_BYTES = 0.5 * 1024**3
SUBSET_DIR = config.RAW_DIR / "subsets"


def make_subsets() -> None:
    """First N rows of the 5 GB dataset, one CSV per volume."""
    SUBSET_DIR.mkdir(parents=True, exist_ok=True)
    biggest = max(SUBSET_ROWS.values())
    header, lines = None, []
    for src in sorted((config.RAW_DIR / "readings_5GB").glob("part-*.csv")):   # read across part files
        with open(src, encoding="utf-8") as f:
            h = f.readline()
            header = header or h
            for line in f:
                lines.append(line)
                if len(lines) >= biggest:
                    break
        if len(lines) >= biggest:
            break
    for label, n in SUBSET_ROWS.items():
        out = SUBSET_DIR / f"readings_{label}.csv"
        if not out.exists():
            out.write_text(header + "".join(lines[:n]), encoding="utf-8")


def files_for(volume: str) -> list[Path]:
    if volume in SUBSET_ROWS:
        return [SUBSET_DIR / f"readings_{volume}.csv"]
    return sorted((config.RAW_DIR / f"readings_{volume}").glob("part-*.csv"))


# ---------------------------------------------------------------------------
# Workers (run in a fresh process)
# ---------------------------------------------------------------------------
def work_pandas(files: list[str]) -> dict:
    import pandas as pd
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    df = df.dropna().drop_duplicates(["meter_id", "timestamp"])
    df = df[df.voltage_v.between(150, 270) & df.power_factor.between(0.3, 1.0) & (df.power_kw >= 0)]
    by_muni = df.groupby("municipality")["energy_kwh"].sum().sort_values(ascending=False)
    top10 = df.groupby("meter_id")["energy_kwh"].sum().nlargest(10)
    dist = df["anomaly_type"].value_counts()
    volt = df.groupby("customer_type")["voltage_v"].mean()
    return {"rows": int(len(df)), "total_kwh": round(float(by_muni.sum()), 1), "top_meter": top10.index[0],
            "n_types_anomaly": int(dist.size), "n_types": int(volt.size)}


def work_spark(files: list[str]) -> dict:
    from pyspark.sql import functions as F

    from common.spark import get_spark, raw_schema, spark_path
    spark = get_spark("PowerPredict-Benchmark", ui=False)
    raw = spark.read.option("header", True).schema(raw_schema()).csv([spark_path(f) for f in files])
    # scale shuffle partitions with the input so cached blocks stay small (same rule as the ETL)
    spark.conf.set("spark.sql.shuffle.partitions", str(max(64, 2 * raw.rdd.getNumPartitions())))
    df = (raw
          .dropna().dropDuplicates(["meter_id", "timestamp"])
          .filter(F.col("voltage_v").between(150, 270) & F.col("power_factor").between(0.3, 1.0) & (F.col("power_kw") >= 0))
          .cache())
    rows = df.count()
    by_muni = df.groupBy("municipality").agg(F.sum("energy_kwh").alias("kwh")).collect()
    top10 = df.groupBy("meter_id").agg(F.sum("energy_kwh").alias("kwh")).orderBy(F.desc("kwh")).limit(10).collect()
    dist = df.groupBy("anomaly_type").count().collect()
    volt = df.groupBy("customer_type").agg(F.avg("voltage_v")).collect()
    out = {"rows": int(rows), "total_kwh": round(float(sum(r["kwh"] for r in by_muni)), 1),
           "top_meter": top10[0]["meter_id"], "n_types_anomaly": len(dist), "n_types": len(volt)}
    spark.stop()
    return out


def _worker_main(engine: str, files: list[str]) -> None:
    t = time.perf_counter()
    res = work_pandas(files) if engine == "pandas" else work_spark(files)
    res["seconds"] = time.perf_counter() - t
    print("RESULT " + json.dumps(res), flush=True)


# ---------------------------------------------------------------------------
def run_once(engine: str, volume: str) -> dict:
    files = [str(f) for f in files_for(volume)]
    cmd = [sys.executable, "-m", "pipeline.benchmark", "--worker", engine, "--files", *files]
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=str(config.ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    peak = 0.0
    oom = False
    limit = MEM_LIMIT_FRACTION * psutil.virtual_memory().total
    stop = threading.Event()

    def sample():
        nonlocal peak, oom
        try:
            p = psutil.Process(proc.pid)
            while not stop.is_set():
                try:
                    kids = p.children(recursive=True)
                    rss = p.memory_info().rss + sum(c.memory_info().rss for c in kids)
                    peak = max(peak, rss)
                    if rss > limit or psutil.virtual_memory().available < MIN_FREE_BYTES:
                        oom = True                      # stop before Windows starts swapping
                        for c in kids:
                            c.kill()
                        p.kill()
                        break
                except psutil.Error:
                    pass
                time.sleep(0.1)
        except psutil.Error:
            pass

    th = threading.Thread(target=sample, daemon=True)
    th.start()
    out, _ = proc.communicate()
    stop.set()
    th.join()
    wall = time.perf_counter() - t0
    line = next((ln for ln in out.splitlines() if ln.startswith("RESULT ")), None)
    size_mb = sum(Path(f).stat().st_size for f in files) / 1024**2
    if not line:
        if oom or "MemoryError" in out:
            return {"engine": engine, "volume": volume, "size_mb": round(size_mb, 2), "wall_seconds": round(wall, 3),
                    "peak_memory_mb": round(peak / 1024**2, 1), "rows": None, "total_kwh": None,
                    "failed": f"out of memory (peak {peak / 1024**3:.1f} GB, RAM {psutil.virtual_memory().total / 1024**3:.0f} GB)"}
        raise RuntimeError(f"{engine} {volume} failed:\n{out[-2000:]}")
    res = json.loads(line[7:])
    return {"engine": engine, "volume": volume, "size_mb": round(size_mb, 2), "wall_seconds": round(wall, 3),
            "peak_memory_mb": round(peak / 1024**2, 1), **res}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--worker", choices=["pandas", "spark"])
    ap.add_argument("--files", nargs="*")
    ap.add_argument("--volumes", nargs="*", default=list(SUBSET_ROWS) + FULL)
    ap.add_argument("--engines", nargs="*", default=["pandas", "spark"])
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--no-db", action="store_true")
    args = ap.parse_args()

    if args.worker:
        _worker_main(args.worker, args.files)
        return

    make_subsets()
    from common import db
    results = []
    if not args.no_db:
        with db.begin() as conn:
            conn.execute(db.benchmarks.delete().where(db.benchmarks.c.dataset_label.in_(args.volumes)))
    for volume in args.volumes:
        if not files_for(volume):
            print(f"skip {volume}: no files")
            continue
        for engine in args.engines:
            for run in range(1, args.runs + 1):
                r = run_once(engine, volume)
                results.append(r)
                if r.get("failed"):
                    print(f"{volume:>6} {engine:<6} run {run}: FAILED after {r['wall_seconds']:.1f}s - {r['failed']}", flush=True)
                else:
                    print(f"{volume:>6} {engine:<6} run {run}: {r['wall_seconds']:8.2f}s  peak {r['peak_memory_mb']:8.0f} MB  "
                          f"rows {r['rows']:,}  kWh {r['total_kwh']:,}", flush=True)
                if not args.no_db:
                    db.bulk_insert(db.benchmarks, [{
                        "engine": engine, "dataset_label": volume, "n_rows": r["rows"], "size_mb": r["size_mb"],
                        "seconds": r["wall_seconds"], "peak_memory_mb": r["peak_memory_mb"], "run_no": run,
                        "result_check": ("FAILED: " + r["failed"]) if r.get("failed")
                        else f"rows={r['rows']} kwh={r['total_kwh']}"}])
                if r.get("failed"):
                    break                               # same volume will fail again - next engine
    out = config.BENCH_DIR / f"benchmark_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"saved {out}")


if __name__ == "__main__":
    main()
