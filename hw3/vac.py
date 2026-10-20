import csv, io, re
from itertools import combinations

csv.field_size_limit(1 << 28)

JAVA_RE = re.compile(r"\bjava\b", re.I | re.A)
EXC_RE = re.compile(r"javascript|java\s+script", re.I | re.A)
NET_K = 0.87          # gross -> net (НДФЛ 13%)
MIN_SALARY = 10_000   # отсекаем явные опечатки (например 140 руб.)
MAX_SALARY = 1_500_000
KEEP = ("name", "key_skills", "salary_from", "salary_to", "currency", "gross", "area", "employer_id")


def is_java(name):
    return bool(name) and JAVA_RE.search(name) is not None and EXC_RE.search(name) is None


def skills_of(key_skills):
    if not key_skills:
        return []
    return sorted({s.strip() for s in key_skills.split(";") if s.strip()})


def salary_of(cur, sfrom, sto, gross):
    if cur != "RUR":
        return None
    vals = []
    for v in (sfrom, sto):
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            pass
    if not vals:
        return None
    s = sum(vals) / len(vals)
    if str(gross).strip().lower() == "true":
        s *= NET_K
    return s if MIN_SALARY <= s <= MAX_SALARY else None


def combos(sk, k):
    return combinations(sk, k)


def rows_of(text, header):
    idx = {c: i for i, c in enumerate(header) if c in KEEP}
    n = len(header)
    for r in csv.reader(io.StringIO(text, newline="")):
        if len(r) != n:
            yield None
            continue
        yield {c: r[i] for c, i in idx.items()}
