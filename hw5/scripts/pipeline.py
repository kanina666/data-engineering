import argparse
import csv
import hashlib
import io
import json
import os
import runpy
import sys
import tempfile
from collections import Counter
from contextlib import closing
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET = "vacancies"
KEY = "dataset/it_vacancies_2024.csv"
PREFIX = "hw5"
SOURCE_MD5 = "45041a115287e29b41fc8f82818ec5a6"
FIELDS = ("rank", "skill_1", "skill_2", "vacancies")
csv.field_size_limit(1 << 28)


def client():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=120),
    )


def get_json(s3, key):
    with closing(s3.get_object(Bucket=BUCKET, Key=key)["Body"]) as body:
        return json.load(body)


def put_json(s3, key, value):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(value, ensure_ascii=False).encode(),
                  ContentType="application/json")


class CsvLines:
    """Expose physical LF/CRLF lines and track the bytes consumed by csv.reader."""

    def __init__(self, stream):
        self.stream = stream
        self.offset = 0
        self.md5 = hashlib.md5()
        self.sha256 = hashlib.sha256()

    def __iter__(self):
        return self

    def __next__(self):
        raw = next(self.stream)
        self.offset += len(raw)
        self.md5.update(raw)
        self.sha256.update(raw)
        return raw.decode("utf-8")


def layout(path, count):
    size = path.stat().st_size
    with path.open("rb") as stream:
        lines = CsvLines(stream)
        reader = csv.reader(lines)
        header = next(reader)
        boundaries = [0]
        for _ in reader:
            if len(boundaries) < count and lines.offset * count >= size * len(boundaries):
                boundaries.append(lines.offset)
        boundaries.append(lines.offset)
    if len(boundaries) != count + 1 or sorted(set(boundaries)) != boundaries:
        raise ValueError("Could not create the requested number of nonempty ranges")
    if lines.md5.hexdigest() != SOURCE_MD5 or lines.offset != size:
        raise ValueError(f"Unexpected input: {lines.offset} bytes, MD5={lines.md5.hexdigest()}")
    return {"header": header, "size": size, "sha256": lines.sha256.hexdigest(),
            "parts": list(zip(boundaries, boundaries[1:]))}


def ingest(path, count, source_endpoint=None):
    if source_endpoint:
        source = boto3.client(
            "s3", endpoint_url=source_endpoint, region_name="us-east-1",
            aws_access_key_id=os.environ["SOURCE_ACCESS_KEY"],
            aws_secret_access_key=os.environ["SOURCE_SECRET_KEY"],
            config=Config(s3={"addressing_style": "path"}),
        )
        with tempfile.TemporaryDirectory() as directory:
            download = Path(directory) / "from-hw2.csv"
            source.download_file(BUCKET, KEY, str(download))
            print(f"downloaded source s3://{BUCKET}/{KEY} via {source_endpoint} "
                  f"({download.stat().st_size} bytes)")
            return ingest(download, count)
    manifest = layout(path, count)
    print(f"local bytes={manifest['size']} md5={SOURCE_MD5} sha256={manifest['sha256']}")
    s3 = client()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        s3.create_bucket(Bucket=BUCKET)
    s3.upload_file(str(path), BUCKET, KEY, Config=TransferConfig(
        multipart_threshold=64 << 20, multipart_chunksize=64 << 20, max_concurrency=4))
    digest = hashlib.sha256()
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    with closing(obj["Body"]) as body:
        for chunk in body.iter_chunks(4 << 20):
            digest.update(chunk)
    if obj["ContentLength"] != manifest["size"] or digest.hexdigest() != manifest["sha256"]:
        raise ValueError("S3 input differs from source")
    manifest["etag"] = obj["ETag"]
    put_json(s3, f"{PREFIX}/parts.json", manifest)
    print(f"s3://{BUCKET}/{KEY} sha256={digest.hexdigest()} MATCH")
    print(json.dumps(manifest, ensure_ascii=False))


def part_key(run_id, index):
    return f"{PREFIX}/runs/{run_id}/counts/{index:04d}.json"


def worker(run_id, fail_index):
    from vac import combos, is_java, skills_of

    index = int(os.environ["JOB_COMPLETION_INDEX"])
    s3 = client()
    manifest = get_json(s3, f"{PREFIX}/parts.json")
    start, end = manifest["parts"][index]
    obj = s3.get_object(Bucket=BUCKET, Key=KEY, Range=f"bytes={start}-{end - 1}",
                        IfMatch=manifest["etag"])
    expected_range = f"bytes {start}-{end - 1}/{manifest['size']}"
    if obj["ContentRange"] != expected_range:
        raise ValueError(f"Unexpected Content-Range: {obj['ContentRange']}")
    counts = Counter()
    stats = Counter()
    with io.TextIOWrapper(io.BufferedReader(obj["Body"]), encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        header = manifest["header"]
        if index == 0 and next(reader) != header:
            raise ValueError("CSV header differs from the manifest")
        name, skills = header.index("name"), header.index("key_skills")
        for row in reader:
            if len(row) != len(header):
                stats["bad_rows"] += 1
                continue
            stats["rows"] += 1
            if is_java(row[name]):
                stats["java"] += 1
                counts.update(combos(skills_of(row[skills]), 2))
    key = part_key(run_id, index)
    retry = False
    if index == fail_index:
        try:
            existing = get_json(s3, key)
            retry = (existing["sha256"], existing["range"]) == (manifest["sha256"], [start, end])
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchKey":
                raise
    put_json(s3, key, {"sha256": manifest["sha256"], "range": [start, end], "index": index,
                      "stats": stats, "counts": [(a, b, n) for (a, b), n in sorted(counts.items())]})
    print(f"index={index} range={start}:{end} {dict(stats)} pairs={len(counts)} sum={sum(counts.values())}"
          f" key={key} retry={retry}")
    if index == fail_index and not retry:
        sys.exit("Injected failure after the complete counter was uploaded")


def write_top(path, counts):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(FIELDS)
        top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        writer.writerows((i, *pair, n) for i, (pair, n) in enumerate(top, 1))


def reduce(run_id):
    s3 = client()
    manifest = get_json(s3, f"{PREFIX}/parts.json")
    total, stats = Counter(), Counter()
    for index, span in enumerate(manifest["parts"]):
        part = get_json(s3, part_key(run_id, index))
        if (part["index"], part["sha256"], part["range"]) != (index, manifest["sha256"], span):
            raise ValueError(f"Incorrect counter for part {index}")
        total.update({(a, b): n for a, b, n in part["counts"]})
        stats.update(part["stats"])
    print(f"parts={len(manifest['parts'])} {dict(stats)} pairs={len(total)} sum={sum(total.values())}")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "top.csv"
        write_top(path, total)
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/runs/{run_id}/top.csv", Body=path.read_bytes())
        print(path.read_text(encoding="utf-8"), end="")


def memory(run_id):
    import config

    config.s3 = client
    config.BUCKET = BUCKET
    config.KEY = KEY
    with tempfile.TemporaryDirectory() as directory:
        sys.argv = ["mapreduce.py", "--mode", "naive", "--chunk-mb", "16", "--out-dir", directory]
        runpy.run_module("mapreduce", run_name="__main__")
        with (Path(directory) / "skills_k2.csv").open(encoding="utf-8", newline="") as stream:
            counts = {(row["skill_1"], row["skill_2"]): int(row["vacancies"])
                      for row in csv.DictReader(stream)}
        top = Path(directory) / "top.csv"
        write_top(top, counts)
        client().put_object(Bucket=BUCKET, Key=f"{PREFIX}/{run_id}/top.csv", Body=top.read_bytes())
    for peak, maximum in (("memory.peak", "memory.max"),
                          ("memory/memory.max_usage_in_bytes", "memory/memory.limit_in_bytes")):
        base = Path("/sys/fs/cgroup")
        if (base / peak).is_file():
            print(f"peak_bytes={(base / peak).read_text().strip()} source={base / peak} "
                  f"limit_bytes={(base / maximum).read_text().strip()}")
            return
    raise RuntimeError("No container cgroup peak counter")


def compare(reference, actual):
    def rows(path):
        with path.open(encoding="utf-8", newline="") as stream:
            return [(int(r["rank"]), r["skill_1"], r["skill_2"], int(r["vacancies"]))
                    for r in csv.DictReader(stream)]

    expected, calculated = rows(reference), rows(actual)
    ok = [row[0] for row in expected] == list(range(1, 11)) and expected == calculated
    print(f"reference_rows={len(expected)} actual_rows={len(calculated)}")
    for left, right in zip(expected, calculated):
        print(f"{'MATCH' if left == right else 'DIFFER'} {left} | {right}")
    print(f"RESULT: {'10/10 MATCH' if ok else 'MISMATCH'}")
    if not ok:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    upload = sub.add_parser("ingest")
    upload.add_argument("path", type=Path, nargs="?")
    upload.add_argument("--parts", dest="count", type=int, default=8)
    upload.add_argument("--source-endpoint")
    for action in ("worker", "reduce", "memory"):
        command = sub.add_parser(action)
        command.add_argument("run_id")
        if action == "worker":
            command.add_argument("--fail-index", type=int, default=-1)
    fetch = sub.add_parser("fetch")
    fetch.add_argument("key")
    fetch.add_argument("path", type=Path)
    check = sub.add_parser("compare")
    check.add_argument("reference", type=Path)
    check.add_argument("actual", type=Path)
    args = vars(parser.parse_args())
    action = args.pop("action")
    if action == "ingest" and (args["path"] is None) == (args["source_endpoint"] is None):
        parser.error("Supply either a local CSV or --source-endpoint")
    if action == "fetch":
        client().download_file(BUCKET, args["key"], str(args["path"]))
    else:
        {"ingest": ingest, "worker": worker, "reduce": reduce, "memory": memory, "compare": compare}[action](**args)


if __name__ == "__main__":
    main()
