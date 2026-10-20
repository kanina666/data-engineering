import subprocess, sys, time, json, psutil, threading

PY = sys.executable
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
CONFIGS = {
    "MapReduce до (naive: 1 процесс, чанк 64 МБ, без combiner)":
        [PY, "mapreduce.py", "--mode", "naive", "--chunk-mb", "64", "--quiet", "--out-dir", "out/prof_naive"],
    "MapReduce после (opt: 8 mapper-процессов, 8 потоков read-ahead, чанк 16 МБ, combiner)":
        [PY, "mapreduce.py", "--mode", "opt", "--chunk-mb", "16", "--workers", "8", "--io-threads", "8", "--quiet", "--out-dir", "out/prof_opt"],
    "MapReduce после-2 (tuned: 4 mapper-процесса, 8 потоков read-ahead, чанк 8 МБ, combiner)":
        [PY, "mapreduce.py", "--mode", "opt", "--chunk-mb", "8", "--workers", "4", "--io-threads", "8", "--quiet", "--out-dir", "out/prof_tuned"],
    "DuckDB (SQL к S3)":
        [PY, "03_duckdb.py", "--out-dir", "out/prof_duck"],
}
SWEEP = {  # абляция: вклад каждой оптимизации (однократные прогоны)
    f"opt workers={w} io_threads={io} chunk={c}MB": [PY, "mapreduce.py", "--mode", "opt", "--chunk-mb", str(c),
        "--workers", str(w), "--io-threads", str(io), "--quiet", "--out-dir", "out/prof_sweep"]
    for w, io, c in ((8, 1, 16), (1, 8, 16), (4, 8, 16), (8, 4, 16), (8, 8, 8), (8, 8, 32), (12, 8, 16))
}


def measure(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    ps = psutil.Process(p.pid)
    peak, stop = 0, threading.Event()

    def poll():
        nonlocal peak
        while not stop.is_set():
            try:
                tot = 0
                for q in [ps] + ps.children(recursive=True):
                    try:
                        tot += q.memory_info().rss
                    except psutil.Error:
                        pass
                peak = max(peak, tot)
            except psutil.Error:
                break
            time.sleep(0.02)

    th = threading.Thread(target=poll, daemon=True); th.start()
    t0 = time.perf_counter()
    out = p.communicate()[0]
    el = time.perf_counter() - t0
    stop.set(); th.join()
    return el, peak / 2**20, out, p.returncode


res = {}
for name, cmd in {**CONFIGS}.items():
    res[name] = []
    for i in range(RUNS):
        el, mem, out, rc = measure(cmd)
        print(f"{name} | run {i+1}: {el:.1f} s, peak {mem:.0f} MB, rc={rc}", flush=True)
        res[name].append((el, mem))
sweep = {}
for name, cmd in SWEEP.items():
    el, mem, out, rc = measure(cmd)
    print(f"[sweep] {name}: {el:.1f} s, peak {mem:.0f} MB, rc={rc}", flush=True)
    sweep[name] = (el, mem)
json.dump({"runs": res, "sweep": sweep}, open("out/profile.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
