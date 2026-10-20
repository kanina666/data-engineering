import pandas as pd
from duck import connect, build

REGIONS = ["Москва", "Санкт-Петербург", "Новосибирск"]
con = connect(); build(con)
pd.set_option("display.width", 250, "display.max_colwidth", 60)
inr = ",".join(f"'{r}'" for r in REGIONS)

print("=== Таблица 1. Рынок и зарплаты (руб./мес. на руки, только RUR) ===")
t1 = con.execute(f"""
SELECT area AS region, count(*) AS vacancies,
  count(*) FILTER (WHERE key_skills IS NOT NULL AND trim(key_skills)<>'') AS with_skills,
  round(100.0*count(*) FILTER (WHERE key_skills IS NOT NULL AND trim(key_skills)<>'')/count(*),1) AS pct_skills,
  count(sal) AS with_salary,
  round(quantile_cont(sal,0.25)) AS p25, round(median(sal)) AS median, round(quantile_cont(sal,0.75)) AS p75,
  round(avg(sal)) AS mean,
  round(100.0*count(*) FILTER (WHERE schedule='Удаленная работа')/count(*),1) AS pct_remote,
  round(100.0*count(*) FILTER (WHERE status='active')/count(*),1) AS pct_active
FROM vac WHERE area IN ({inr}) GROUP BY area ORDER BY vacancies DESC""").df()
print(t1.to_string(index=False)); t1.to_csv("out/regions_t1.csv", index=False)

print("\n=== Медиана зарплаты по опыту ===")
t1b = con.execute(f"""
SELECT area AS region, experience, count(sal) AS n, round(median(sal)) AS median
FROM vac WHERE area IN ({inr}) AND sal IS NOT NULL GROUP BY 1,2 ORDER BY 1, median""").df()
print(t1b.pivot(index="experience", columns="region", values=["n", "median"]).to_string()); t1b.to_csv("out/regions_exp.csv", index=False)

print("\n=== Таблица 2. Топ-10 навыков по регионам (% от вакансий региона с навыками; lift к сегменту) ===")
con.execute(f"""CREATE OR REPLACE TEMP TABLE rsk AS
  SELECT s.*, v.area FROM sk s JOIN vac v USING (vacancy_id)""")
con.execute("""CREATE OR REPLACE TEMP TABLE seg AS
  SELECT skill, count(*) c FROM sk GROUP BY 1""")
segn = con.execute("SELECT count(DISTINCT vacancy_id) FROM sk").fetchone()[0]
frames = []
for reg in REGIONS:
    n = con.execute("SELECT count(DISTINCT vacancy_id) FROM rsk WHERE area=?", [reg]).fetchone()[0]
    d = con.execute(f"""
      SELECT ? AS region, r.skill, count(*) AS vacancies, round(100.0*count(*)/{n},1) AS pct,
             round((count(*)/{n}.0)/(g.c/{segn}.0),2) AS lift, round(median(r.sal)) AS median_salary, count(r.sal) AS n_sal
      FROM rsk r JOIN seg g USING (skill) WHERE r.area=? GROUP BY r.skill, g.c
      ORDER BY vacancies DESC, r.skill LIMIT 10""", [reg, reg]).df()
    d["rank"] = range(1, 11); frames.append(d)
t2 = pd.concat(frames); t2.to_csv("out/regions_t2.csv", index=False)
print(t2.pivot(index="rank", columns="region", values="skill").to_string())
print(t2[["region", "rank", "skill", "vacancies", "pct", "lift", "median_salary", "n_sal"]].to_string(index=False))

print("\n=== Специфичные навыки региона (lift, не менее 15 вакансий) ===")
frames = []
for reg in REGIONS:
    n = con.execute("SELECT count(DISTINCT vacancy_id) FROM rsk WHERE area=?", [reg]).fetchone()[0]
    d = con.execute(f"""
      SELECT ? AS region, r.skill, count(*) AS vacancies, round(100.0*count(*)/{n},1) AS pct,
             round((count(*)/{n}.0)/(g.c/{segn}.0),2) AS lift
      FROM rsk r JOIN seg g USING (skill) WHERE r.area=? GROUP BY r.skill, g.c HAVING count(*)>=15
      ORDER BY lift DESC LIMIT 7""", [reg, reg]).df()
    frames.append(d)
t3 = pd.concat(frames); t3.to_csv("out/regions_lift.csv", index=False); print(t3.to_string(index=False))

print("\n=== Топ-5 пар и троек навыков по регионам ===")
for k, extra in ((2, ""), (3, "")):
    frames = []
    for reg in REGIONS:
        j = " ".join(f"JOIN rsk {c} ON a.vacancy_id={c}.vacancy_id AND {p}.skill<{c}.skill AND {c}.area=a.area"
                     for c, p in (("b", "a"), ("c", "b"))[: k - 1])
        cols = "a.skill" + "".join(f" || ' + ' || {c}.skill" for c in "bc"[: k - 1])
        d = con.execute(f"""SELECT ? AS region, {cols} AS combo, count(*) AS vacancies, count(a.sal) AS n_sal,
               round(median(a.sal)) AS median_salary
               FROM rsk a {j} WHERE a.area=? GROUP BY 2 ORDER BY vacancies DESC, combo LIMIT 5""", [reg, reg]).df()
        frames.append(d)
    t = pd.concat(frames); t.to_csv(f"out/regions_k{k}.csv", index=False)
    print(f"-- k={k}"); print(t.to_string(index=False))
