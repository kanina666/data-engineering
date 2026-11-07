import os

import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
ACCESS, SECRET = "rustfsadmin", "rustfsadmin"
BUCKET = "hh-lake"
KEY = "dataset/it_vacancies_2024.csv"

# Правило варианта «Java»: в названии есть слово java, но это не javascript
JAVA_REGEX = r"(?i)\bjava\b"          # \b: не заденет javascript
EXCLUDE_REGEX = r"(?i)javascript|java\s+script"

def s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET, region_name="us-east-1",
        config=Config(retries={"max_attempts": 3}, connect_timeout=5, read_timeout=120))
