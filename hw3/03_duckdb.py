import os, time, argparse
from duck import connect, build, SQL_K1, SQL_K2, SQL_K3, SQL_STATS, SRC
from config import s3, BUCKET

ap = argparse.ArgumentParser()
ap.add_argument("--out-dir", default="out/duck")
ap.add_argument("--upload", action="store_true")
ap.add_argument("--threads", type=int)
a = ap.parse_args()
os.makedirs(a.out_dir, exist_ok=True)

t0 = time.perf_counter()
con = connect(a.threads)
build(con)
print("source:", SRC)
print("stats:", con.execute(SQL_STATS).fetchall())
client = s3()
for k, q in ((1, SQL_K1), (2, SQL_K2), (3, SQL_K3)):
    p = os.path.join(a.out_dir, f"skills_k{k}.csv")
    con.execute(f"COPY ({q}) TO '{p}' (HEADER, DELIMITER ',')")
    n = con.execute(f"SELECT count(*), sum(vacancies) FROM read_csv('{p}')").fetchone()
    print(f"k={k}: distinct={n[0]} total={n[1]}")
    if a.upload:
        client.upload_file(p, BUCKET, f"results/duckdb/skills_k{k}.csv")
print("time=%.2fs" % (time.perf_counter() - t0))
