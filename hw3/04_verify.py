import csv, sys, hashlib, math

MR, DK = "out/mr", "out/duck"
ok_all = True
for k in (1, 2, 3):
    def load(d):
        out = {}
        with open(f"{d}/skills_k{k}.csv", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                key = tuple(r[f"skill_{i+1}"] for i in range(k))
                out[key] = (int(r["vacancies"]), int(r["n_salary"]),
                            float(r["median_salary"]) if r["median_salary"] != "" else None)
        return out
    a, b = load(MR), load(DK)
    only_a, only_b = a.keys() - b.keys(), b.keys() - a.keys()
    diff_cnt = [x for x in a.keys() & b.keys() if a[x][:2] != b[x][:2]]
    diff_med = [x for x in a.keys() & b.keys()
                if (a[x][2] is None) != (b[x][2] is None)
                or (a[x][2] is not None and not math.isclose(a[x][2], b[x][2], abs_tol=0.01))]
    fp = lambda d: hashlib.md5("\n".join(f"{'|'.join(x)}:{v[0]}:{v[1]}" for x, v in sorted(d.items())).encode()).hexdigest()
    ok = not (only_a or only_b or diff_cnt or diff_med) and fp(a) == fp(b)
    ok_all &= ok
    print(f"k={k}: MR keys={len(a)} DuckDB keys={len(b)} sum MR={sum(v[0] for v in a.values())} "
          f"sum DuckDB={sum(v[0] for v in b.values())}")
    print(f"   only in MR={len(only_a)} only in DuckDB={len(only_b)} count mismatches={len(diff_cnt)} "
          f"median mismatches={len(diff_med)}")
    print(f"   md5 MR={fp(a)} md5 DuckDB={fp(b)} -> {'MATCH' if ok else 'DIFFER'}")
    top = sorted(a.items(), key=lambda kv: (-kv[1][0], kv[0]))[:5]
    for key, v in top:
        print(f"   top: {' + '.join(key)}: MR={v[0]} DuckDB={b[key][0]}")
print("RESULT:", "ALL MATCH" if ok_all else "MISMATCH")
sys.exit(0 if ok_all else 1)
