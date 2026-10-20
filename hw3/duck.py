import duckdb
from config import ENDPOINT, ACCESS, SECRET, BUCKET, KEY

NET_K, MIN_SALARY, MAX_SALARY = 0.87, 10_000, 1_500_000


def connect(threads=None):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = ENDPOINT.replace("http://", "")
    con.execute(f"""SET s3_endpoint='{host}'; SET s3_access_key_id='{ACCESS}';
                    SET s3_secret_access_key='{SECRET}'; SET s3_use_ssl=false;
                    SET s3_url_style='path'; SET s3_region='us-east-1';""")
    if threads:
        con.execute(f"SET threads={threads}")
    return con


SRC = f"s3://{BUCKET}/{KEY}"

# Условие варианта «Java» написано в SQL самостоятельно (RE2: \b только для ASCII, как re.A в Python).
# Одна строка таблицы vac = одна Java-вакансия; sal = зарплата в RUR на руки (середина вилки).
SQL_VAC = f"""
CREATE OR REPLACE TEMP TABLE vac AS
SELECT
  TRY_CAST(vacancy_id AS BIGINT) AS vacancy_id,
  area, name, key_skills, experience, schedule, status, employer_id, published_at, salary_from, salary_to, gross,
  CASE WHEN currency = 'RUR' THEN
     CASE WHEN s BETWEEN {MIN_SALARY} AND {MAX_SALARY} THEN s END
  END AS sal
FROM (
  SELECT *,
    coalesce((TRY_CAST(salary_from AS DOUBLE) + TRY_CAST(salary_to AS DOUBLE)) / 2,
             TRY_CAST(salary_from AS DOUBLE), TRY_CAST(salary_to AS DOUBLE))
      * CASE WHEN lower(trim(gross)) = 'true' THEN {NET_K} ELSE 1 END AS s
  FROM read_csv('{SRC}', header=true, all_varchar=true, quote='"', escape='"',
                max_line_size=33554432)
)
WHERE regexp_matches(name, '(?i)\\bjava\\b')
  AND NOT regexp_matches(name, '(?i)javascript|java\\s+script')
"""

SQL_SK = """
CREATE OR REPLACE TEMP TABLE sk AS
SELECT DISTINCT vacancy_id, trim(skill) AS skill, sal, employer_id
FROM (SELECT vacancy_id, sal, employer_id, unnest(string_split(key_skills, ';')) AS skill FROM vac)
WHERE trim(skill) <> ''
"""

SQL_K1 = """
SELECT skill AS skill_1, count(*) AS vacancies, count(sal) AS n_salary, round(median(sal), 2) AS median_salary
FROM sk GROUP BY 1 ORDER BY vacancies DESC, skill_1
"""

SQL_K2 = """
SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS vacancies, count(a.sal) AS n_salary,
       round(median(a.sal), 2) AS median_salary
FROM sk a JOIN sk b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
GROUP BY 1, 2 ORDER BY vacancies DESC, skill_1, skill_2
"""

SQL_K3 = """
SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS vacancies,
       count(a.sal) AS n_salary, round(median(a.sal), 2) AS median_salary
FROM sk a JOIN sk b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
          JOIN sk c ON a.vacancy_id = c.vacancy_id AND b.skill < c.skill
GROUP BY 1, 2, 3 ORDER BY vacancies DESC, skill_1, skill_2, skill_3
"""

SQL_STATS = """
SELECT count(*) AS java,
       count(*) FILTER (WHERE key_skills IS NOT NULL AND trim(key_skills) <> '') AS java_with_skills,
       count(sal) AS java_with_salary
FROM vac
"""


def build(con):
    con.execute(SQL_VAC)
    con.execute(SQL_SK)
