import sys, pandas as pd
from duck import connect, build

src, label = sys.argv[1], sys.argv[2]
excl = sys.argv[3] if len(sys.argv) > 3 else None
MIN_PAIR, MIN_TRIPLE, TOP = 15, 10, 10

con = connect(); build(con)
if excl:
    con.execute(f"UPDATE sk SET sal = NULL WHERE employer_id = '{excl}'")
    con.execute(f"UPDATE vac SET sal = NULL WHERE employer_id = '{excl}'")
SEG_N, SEG_MED = con.execute("""SELECT count(*), median(sal) FROM vac
    WHERE sal IS NOT NULL AND key_skills IS NOT NULL AND trim(key_skills) <> ''""").fetchone()

K = {k: pd.read_csv(f"{src}/skills_k{k}.csv") for k in (1, 2, 3)}
m1 = K[1].set_index("skill_1")
m2 = K[2].set_index(["skill_1", "skill_2"])


def employers(skills):
    cond = " AND ".join(f"EXISTS (SELECT 1 FROM sk x{i} WHERE x{i}.vacancy_id = s.vacancy_id AND x{i}.skill = ?)"
                        for i in range(len(skills)))
    return con.execute(f"SELECT count(DISTINCT employer_id) FROM (SELECT DISTINCT vacancy_id, employer_id "
                       f"FROM sk s WHERE sal IS NOT NULL) s WHERE {cond}", list(skills)).fetchone()[0]


rows = []
for r in K[2].itertuples(index=False):
    if r.n_salary < MIN_PAIR:
        continue
    a, b = r.skill_1, r.skill_2
    if (m1.loc[a, "n_salary"], a) < (m1.loc[b, "n_salary"], b):
        a, b = b, a
    rows.append(dict(base=a, added=b, n=int(r.n_salary), median_with=r.median_salary,
                     n_base=int(m1.loc[a, "n_salary"]), median_base=m1.loc[a, "median_salary"],
                     delta=r.median_salary - m1.loc[a, "median_salary"], skills=(r.skill_1, r.skill_2)))
pairs = pd.DataFrame(rows)

rows = []
for r in K[3].itertuples(index=False):
    if r.n_salary < MIN_TRIPLE:
        continue
    trio = [r.skill_1, r.skill_2, r.skill_3]
    best = None
    for add in trio:
        p = tuple(x for x in trio if x != add)
        if p in m2.index:
            n = int(m2.loc[p, "n_salary"])
            if best is None or (n, p) > (best[0], best[1]):
                best = (n, p, add, m2.loc[p, "median_salary"])
    n_b, p, add, med_b = best
    rows.append(dict(base=" + ".join(p), added=add, n=int(r.n_salary), median_with=r.median_salary,
                     n_base=n_b, median_base=med_b, delta=r.median_salary - med_b, skills=tuple(trio)))
triples = pd.DataFrame(rows)

print(f"[{label}] медиана сегмента Java (навыки и зарплата): {SEG_MED:.0f} руб., n={SEG_N}")
print(f"[{label}] пар с n>={MIN_PAIR}: {len(pairs)}; троек с n>={MIN_TRIPLE}: {len(triples)}")
pd.set_option("display.width", 250, "display.max_colwidth", 70)
for name, df in (("pairs", pairs), ("triples", triples)):
    df = df.sort_values(["delta", "n"], ascending=[False, False]).reset_index(drop=True)
    df["employers"] = [employers(s) for s in df["skills"]] if len(df) <= 5000 else None
    out = df.drop(columns="skills")
    out.round(0).to_csv(f"out/salary_{name}_{label}.csv", index=False)
    cols = ["base", "added", "n", "employers", "median_base", "median_with", "delta"]
    print(f"\n=== [{label}] {name}: топ-{TOP} ПОВЫШАЮТ ===")
    print(out.head(TOP)[cols].round(0).to_string(index=False))
    print(f"\n=== [{label}] {name}: топ-{TOP} ПОНИЖАЮТ ===")
    print(out.tail(TOP).iloc[::-1][cols].round(0).to_string(index=False))
