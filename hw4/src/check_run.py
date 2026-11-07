import argparse
import ast
import glob
import json
import os
import re
import sys
from datetime import datetime

from kazoo.client import KazooClient

HW3_OUT = "hw3/out"
TS = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}) UTC \[(\S+) pid=(\d+)\] \w+ (.*)$")


def ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")


def hw3_expected() -> dict:
    with open(os.path.join(HW3_OUT, "03_mapreduce_fault.txt"), encoding="utf-8") as f:
        text = f.read()
    stats = ast.literal_eval(re.search(r"^stats: (\{.*\})$", text, re.M).group(1))
    m = re.search(r"^distinct: (\{.*?\}) total: (\{.*?\})$", text, re.M)
    with open(os.path.join(HW3_OUT, "04_verify.txt"), encoding="utf-8") as f:
        md5 = {f"k{k}": h for k, h in re.findall(r"^k=(\d).*?md5 MR=([0-9a-f]{32})", f.read(), re.M | re.S)}
    return {"stats": stats, "distinct": ast.literal_eval(m.group(1)), "total": ast.literal_eval(m.group(2)),
            "md5": md5}


def read_logs(run_dir: str) -> dict:
    procs = {}
    for path in sorted(glob.glob(os.path.join(run_dir, "*.log"))):
        with open(path, encoding="utf-8") as f:
            lines = [m.groups() for m in map(TS.match, f) if m]
        procs[os.path.basename(path)] = lines
    return procs


def first(lines, needle):
    return next(((t, msg) for t, _, _, msg in lines if needle in msg), None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("events")
    ap.add_argument("--fencing", type=int, choices=(0, 1), required=True)
    ap.add_argument("--zk", default="zk1:2181,zk2:2182,zk3:2183")
    a = ap.parse_args()

    with open(a.events, encoding="utf-8") as f:
        events = dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)
    paused = events["paused_container"]
    pause_at = ts(events["pause_utc"])

    expected = hw3_expected()
    procs = read_logs(a.run_dir)
    writes, bad_version, failover, checks = [], [], None, []
    print(f"Запуск {a.run_dir}, FENCING={a.fencing}, приостановлен {paused} в {events['pause_utc']} UTC")
    for name, lines in procs.items():
        if name.startswith(paused):
            before = [(t, msg) for t, _, _, msg in lines
                      if ts(t) <= pause_at and ("чекпоинт:" in msg or "стал лидером" in msg)]
            if before:
                t, msg = before[-1]
                print(f"  {name}: последний запрос лидера к ZooKeeper перед паузой: {t} {msg[:60]}… "
                      f"({(pause_at - ts(t)).total_seconds():.3f} с до паузы)")
            if events.get("session_expired_utc"):
                expired = ts(events["session_expired_utc"])
                print(f"  сервер ZooKeeper истёк сессию лидера в {events['session_expired_utc']}: "
                      f"{(expired - pause_at).total_seconds():.3f} с после паузы")
        for t, _, _, msg in lines:
            if "записал результат" in msg:
                writes.append((name, t, msg))
            if "BadVersionError" in msg:
                bad_version.append((name, t, msg))
        leader = first(lines, "стал лидером")
        if leader and not name.startswith(paused):
            failover = (ts(leader[0]) - pause_at).total_seconds()
            print(f"  {name}: стал лидером в {leader[0]} -> смена лидера {failover:.3f} с после приостановки")
            resumed = first(lines, "продолжил с чекпоинта")
            print(f"    {resumed[0]} {resumed[1]}" if resumed else "    строки «продолжил с чекпоинта» нет")
        control = first(lines, "контрольные числа: ")
        if control:
            got = json.loads(control[1].split("контрольные числа: ", 1)[1])
            ok = got == expected
            checks.append(ok)
            print(f"  {name}: контрольные числа {'СОВПАЛИ' if ok else 'НЕ СОВПАЛИ'} с ДЗ № 3")
            for part in ("stats", "distinct", "total", "md5"):
                print(f"    {part:8} ДЗ3={json.dumps(expected[part], ensure_ascii=False)}")
                print(f"    {'':8} тут={json.dumps(got[part], ensure_ascii=False)}")

    print(f"  строк «записал результат»: {len(writes)}")
    for w in writes:
        print(f"    {w[0]}: {w[1]} {w[2]}")
    print(f"  строк с BadVersionError: {len(bad_version)}")
    for b in bad_version:
        print(f"    {b[0]}: {b[1]} {b[2]}")

    zk = KazooClient(hosts=a.zk)
    zk.start(timeout=30)
    try:
        raw, stat = zk.get("/result")
        epoch, _ = zk.get("/epoch")
    finally:
        zk.stop()
        zk.close()
    result = json.loads(raw)
    print(f"  /result: version={stat.version}, записал {result['by']}, эпоха {result['epoch']}; "
          f"/epoch={epoch.decode()}; контрольные числа в /result совпали с ДЗ № 3: {result['control'] == expected}")

    want_writes = 1 if a.fencing else 2
    verdict = {
        "строк «записал результат»": len(writes) == want_writes,
        "версия /result": stat.version == want_writes,
        "BadVersionError": bool(bad_version) == bool(a.fencing),
        "контрольные числа": bool(checks) and all(checks) and result["control"] == expected,
        "смена лидера измерена": failover is not None,
    }
    if a.fencing:
        verdict["/result от запасного"] = result["by"] != paused
    print(f"СВОДКА: fencing={a.fencing} writes={len(writes)} result_version={stat.version} "
          f"bad_version={'да' if bad_version else 'нет'} control_match={verdict['контрольные числа']} "
          f"failover_s={failover:.3f}" if failover is not None else "СВОДКА: смена лидера не найдена")
    for k, ok in verdict.items():
        print(f"  [{'x' if ok else ' '}] {k}")
    return 0 if all(verdict.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
