"""
CPU / memory usage of the benchmark job (manual section 14.B), without writing to tbl_benchmarks.

Runs the same worker as pipeline/benchmark.py (Pandas or Spark) and samples the whole computer's CPU %
and the job's memory every 0.5 s. Result -> cpu_usage.json (read by collect_facts.py).

    python docs/report_build/measure_cpu.py
"""
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
import config  # noqa: E402
from pipeline.benchmark import files_for  # noqa: E402

RUNS = [("pandas", "1M"), ("spark", "1M"), ("spark", "5GB"), ("spark", "20GB")]


def measure(engine: str, volume: str) -> dict:
    files = [str(f) for f in files_for(volume)]
    cmd = [sys.executable, "-m", "pipeline.benchmark", "--worker", engine, "--files", *files]
    cpu, mem = [], []
    stop = threading.Event()
    psutil.cpu_percent(None)
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=str(config.ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def sample():
        p = psutil.Process(proc.pid)
        while not stop.is_set():
            time.sleep(0.5)
            try:
                kids = p.children(recursive=True)
                mem.append(p.memory_info().rss + sum(c.memory_info().rss for c in kids))
            except psutil.Error:
                pass
            cpu.append(psutil.cpu_percent(None))

    th = threading.Thread(target=sample, daemon=True)
    th.start()
    out, _ = proc.communicate()
    stop.set()
    th.join()
    ok = any(ln.startswith("RESULT ") for ln in out.splitlines())
    res = {"engine": engine, "volume": volume, "ok": ok, "seconds": round(time.perf_counter() - t0, 1),
           "cpu_avg_pct": round(sum(cpu) / max(len(cpu), 1), 1), "cpu_peak_pct": round(max(cpu or [0]), 1),
           "mem_peak_gb": round(max(mem or [0]) / 1024**3, 2), "logical_cpus": psutil.cpu_count()}
    print(res, flush=True)
    return res


if __name__ == "__main__":
    results = [measure(e, v) for e, v in RUNS]
    (HERE / "cpu_usage.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    print("->", HERE / "cpu_usage.json")
