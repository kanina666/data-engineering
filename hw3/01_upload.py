import time, hashlib
from boto3.s3.transfer import TransferConfig
from config import s3, BUCKET, KEY

SRC = r"C:\Users\ninak\PycharmProjects\eng_dan\it_vacancies_2024.csv"
c = s3()
try:
    c.create_bucket(Bucket=BUCKET); print("bucket created:", BUCKET)
except Exception as e:
    print("bucket:", e)
t = time.time()
c.upload_file(SRC, BUCKET, KEY, Config=TransferConfig(multipart_threshold=64<<20, multipart_chunksize=64<<20))
print("uploaded in %.1fs" % (time.time() - t))
print("--- list_objects_v2 ---")
for o in c.list_objects_v2(Bucket=BUCKET).get("Contents", []):
    print(o["Key"], o["Size"], o["LastModified"])
h = hashlib.md5()
with open(SRC, "rb") as f:
    for ch in iter(lambda: f.read(1 << 20), b""): h.update(ch)
print("local md5:", h.hexdigest())
