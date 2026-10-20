import argparse, csv, json, os, statistics, time
from collections import Counter, defaultdict
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED

from config import s3, BUCKET, KEY
from vac import is_java, skills_of, salary_of, rows_of, combos

KS = (1, 2, 3)


# ---------------------------------------------------------------- чтение чанков
def split_records(data):
    total = data.count(b'"')
    prev, q_after, p = len(data), 0, len(data)
    while True:
        p = data.rfind(b"\n", 0, p)
        if p < 0:
            return -1
        q_after += data.count(b'"', p, prev)
        prev = p
        if (total - q_after) % 2 == 0:
            return p


def _fetch(client, a, b):
    return client.get_object(Bucket=BUCKET, Key=KEY, Range=f"bytes={a}-{b}")["Body"].read()


def iter_raw(client, chunk_bytes, io_threads):
    size = client.head_object(Bucket=BUCKET, Key=KEY)["ContentLength"]
    ranges = [(p, min(p + chunk_bytes, size) - 1) for p in range(0, size, chunk_bytes)]
    if io_threads <= 1:
        for a, b in ranges:
            yield _fetch(client, a, b)
        return
    with ThreadPoolExecutor(io_threads) as ex:
        window = deque()
        it = iter(ranges)
        for _ in range(io_threads * 2):
            r = next(it, None)
            if r:
                window.append(ex.submit(_fetch, client, *r))
        while window:
            data = window.popleft().result()
            r = next(it, None)
            if r:
                window.append(ex.submit(_fetch, client, *r))
            yield data


def iter_blocks(client, chunk_bytes, io_threads=1):
    carry = b""
    for body in iter_raw(client, chunk_bytes, io_threads):
        data = carry + body
        cut = split_records(data)
        if cut < 0:
            carry = data
            continue
        yield data[: cut + 1]
        carry = data[cut + 1:]
    if carry.strip():
        yield carry

def _process(text, header, exclude=()):
    stats = Counter()
    out = []
    for r in rows_of(text, header):
        if r is None:
            stats["bad_rows"] += 1
            continue
        stats["rows"] += 1
        if not is_java(r["name"]):
            continue
        stats["java"] += 1
        sk = skills_of(r["key_skills"])
        sal = salary_of(r["currency"], r["salary_from"], r["salary_to"], r["gross"])
        if r["employer_id"] in exclude:
            sal = None
        if sk:
            stats["java_with_skills"] += 1
        if sal is not None:
            stats["java_with_salary"] += 1
        if sk and sal is not None:
            stats["java_skills_and_salary"] += 1
        out.append((sk, sal))
    return stats, out


def map_opt(block, header, drop_header, chunk_id, attempt, fail_chunk, exclude=()):
    if fail_chunk == chunk_id and attempt == 0:
        raise RuntimeError(f"injected mapper failure on chunk {chunk_id}")
    text = block.decode("utf-8")
    if drop_header:
        text = text[text.index("\n") + 1:]
    stats, vac = _process(text, header, exclude)
    cnt = {k: Counter() for k in KS}          # combiner: локальная свёртка
    sal = {k: defaultdict(list) for k in KS}
    for sk, s in vac:
        for k in KS:
            for key in combos(sk, k):
                cnt[k][key] += 1
                if s is not None:
                    sal[k][key].append(s)
    return stats, cnt, sal


def map_naive(block, header, drop_header, exclude=()):
    text = block.decode("utf-8")
    if drop_header:
        text = text[text.index("\n") + 1:]
    stats, vac = _process(text, header, exclude)
    emitted = []
    for sk, s in vac:
        for k in KS:
            for key in combos(sk, k):
                emitted.append((k, key, 1, s))
    return stats, emitted


def new_acc():
    return Counter(), {k: Counter() for k in KS}, {k: defaultdict(list) for k in KS}


def merge(acc, part):
    stats_acc, cnt_acc, sal_acc = acc
    stats, cnt, sal = part
    stats_acc.update(stats)
    for k in KS:
        cnt_acc[k].update(cnt[k])
        for key, v in sal[k].items():
            sal_acc[k][key].extend(v)


def header_of(block):
    first = block[: block.index(b"\n")].decode("utf-8").rstrip("\r")
    return next(csv.reader([first]))


def run(mode, chunk_mb, workers, fail_chunk, log, exclude=(), io_threads=8):
    client = s3()
    chunk = chunk_mb << 20
    acc = new_acc()
    stats_acc, cnt_acc, sal_acc = acc
    t0 = time.perf_counter()
    blocks = iter_blocks(client, chunk, io_threads if mode == 'opt' else 1)
    header, n_chunks, retries = None, 0, 0

    if mode == "naive":
        for i, block in enumerate(blocks):
            if header is None:
                header = header_of(block)
            stats, emitted = map_naive(block, header, i == 0, exclude)
            stats_acc.update(stats)
            for k, key, one, s in emitted:
                cnt_acc[k][key] += one
                if s is not None:
                    sal_acc[k][key].append(s)
            n_chunks += 1
            log(f"chunk {i}: {len(block)>>20} MB, java so far {stats_acc['java']}")
    else:
        with ProcessPoolExecutor(workers) as ex:
            pending = {}                       # future -> (chunk_id, block, drop, attempt)

            def submit(cid, block, drop, attempt):
                f = ex.submit(map_opt, block, header, drop, cid, attempt, fail_chunk, exclude)
                pending[f] = (cid, block, drop, attempt)

            def drain(limit):
                nonlocal retries
                while len(pending) > limit:
                    done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                    for f in done:
                        cid, block, drop, attempt = pending.pop(f)
                        try:
                            merge(acc, f.result())
                            log(f"chunk {cid}: ok (attempt {attempt})")
                        except Exception as e:
                            if attempt >= 2:
                                raise
                            retries += 1
                            log(f"chunk {cid}: FAILED ({e}); retry {attempt+1}")
                            submit(cid, block, drop, attempt + 1)

            for i, block in enumerate(blocks):
                if header is None:
                    header = header_of(block)
                submit(i, block, i == 0, 0)
                n_chunks += 1
                drain(workers * 2 - 1)
            drain(0)
    return acc, n_chunks, retries, time.perf_counter() - t0


def write_results(acc, out_dir, upload):
    stats, cnt, sal = acc
    client = s3()
    for k in KS:
        p = os.path.join(out_dir, f"skills_k{k}.csv")
        rows = sorted(cnt[k].items(), key=lambda kv: (-kv[1], kv[0]))
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([f"skill_{i+1}" for i in range(k)] + ["vacancies", "n_salary", "median_salary"])
            for key, c in rows:
                s = sal[k].get(key, [])
                w.writerow(list(key) + [c, len(s), round(statistics.median(s), 2) if s else ""])
        if upload:
            client.upload_file(p, BUCKET, f"results/mapreduce/skills_k{k}.csv")
    sp = os.path.join(out_dir, "mr_stats.json")
    json.dump(dict(stats), open(sp, "w"), ensure_ascii=False, indent=1)
    if upload:
        client.upload_file(sp, BUCKET, "results/mapreduce/stats.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "opt"], default="opt")
    ap.add_argument("--chunk-mb", type=int, default=16)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--fail-chunk", type=int, default=-1, help="id чанка, где mapper падает при первой попытке")
    ap.add_argument("--out-dir", default="out/mr")
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--exclude-employers", default="", help="id работодателей, чьи зарплаты не учитываются (этап 5)")
    ap.add_argument("--io-threads", type=int, default=8, help="потоки предвыборки Range-запросов (mode opt)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    log = (lambda m: None) if a.quiet else (lambda m: print(m, flush=True))
    acc, n_chunks, retries, el = run(a.mode, a.chunk_mb, a.workers, a.fail_chunk, log, tuple(x for x in a.exclude_employers.split(',') if x), a.io_threads)
    write_results(acc, a.out_dir, a.upload)
    print(f"MODE={a.mode} chunk={a.chunk_mb}MB workers={a.workers if a.mode=='opt' else 1} "
          f"io_threads={a.io_threads if a.mode=='opt' else 1} chunks={n_chunks} retries={retries} time={el:.2f}s")
    print("stats:", dict(acc[0]))
    print("distinct:", {f"k{k}": len(acc[1][k]) for k in KS},
          "total:", {f"k{k}": sum(acc[1][k].values()) for k in KS})
