import hashlib
import sys

from boto3.s3.transfer import TransferConfig

from config import BUCKET, KEY, s3

HW3_MD5 = "45041a115287e29b41fc8f82818ec5a6"


def main(src: str) -> int:
    client = s3()
    if BUCKET not in {b["Name"] for b in client.list_buckets()["Buckets"]}:
        client.create_bucket(Bucket=BUCKET)
    client.upload_file(src, BUCKET, KEY, Config=TransferConfig(multipart_threshold=64 << 20,
                                                                multipart_chunksize=64 << 20))
    size = client.head_object(Bucket=BUCKET, Key=KEY)["ContentLength"]
    h = hashlib.md5()
    with open(src, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    print(f"s3://{BUCKET}/{KEY} size={size} md5={h.hexdigest()} hw3_md5={HW3_MD5} "
          f"-> {'MATCH' if h.hexdigest() == HW3_MD5 else 'DIFFER'}")
    return 0 if h.hexdigest() == HW3_MD5 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
