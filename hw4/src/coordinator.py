import csv
import gzip
import hashlib
import io
import json
import logging
import os
import signal
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, RolledBackError

from config import BUCKET, s3
from mapreduce import KS, header_of, iter_blocks, map_opt, merge, new_acc

ELECTION, EPOCH, CHECKPOINT, RESULT = "/election", "/epoch", "/checkpoint", "/result"
S3_PREFIX = "zk-hw"


@dataclass(frozen=True)
class Settings:
    name: str = os.environ.get("NAME", "coord")
    hosts: str = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2182,zk3:2183")
    session_timeout: float = float(os.environ.get("SESSION_TIMEOUT", "10"))
    fencing: bool = os.environ.get("FENCING", "0") == "1"
    chunk_mb: int = int(os.environ.get("CHUNK_MB", "16"))
    io_threads: int = int(os.environ.get("IO_THREADS", "4"))
    step_delay: float = float(os.environ.get("STEP_DELAY", "0"))
    ckpt_every: int = int(os.environ.get("CKPT_EVERY", "10"))
    log_dir: str = os.environ.get("LOG_DIR", "logs")


@dataclass
class Progress:
    acc: tuple = field(default_factory=new_acc)
    pos: int = 0
    chunks: int = 0
    header: list | None = None

    def dump(self) -> bytes:
        stats, cnt, sal = self.acc
        doc = {
            "pos": self.pos, "chunks": self.chunks, "header": self.header, "stats": dict(stats),
            "cnt": {k: [[list(key), c] for key, c in cnt[k].items()] for k in KS},
            "sal": {k: [[list(key), v] for key, v in sal[k].items()] for k in KS},
        }
        return gzip.compress(json.dumps(doc, ensure_ascii=False).encode())

    @classmethod
    def load(cls, blob: bytes) -> "Progress":
        doc = json.loads(gzip.decompress(blob))
        p = cls(pos=doc["pos"], chunks=doc["chunks"], header=doc["header"])
        stats, cnt, sal = p.acc
        stats.update(doc["stats"])
        for k in KS:
            cnt[k].update({tuple(key): c for key, c in doc["cnt"][str(k)]})
            sal[k].update({tuple(key): v for key, v in doc["sal"][str(k)]})
        return p


def control_numbers(acc) -> dict:
    stats, cnt, sal = acc

    def md5(k):
        lines = (f"{'|'.join(key)}:{c}:{len(sal[k].get(key, ()))}" for key, c in sorted(cnt[k].items()))
        return hashlib.md5("\n".join(lines).encode()).hexdigest()

    return {
        "stats": dict(stats),
        "distinct": {f"k{k}": len(cnt[k]) for k in KS},
        "total": {f"k{k}": sum(cnt[k].values()) for k in KS},
        "md5": {f"k{k}": md5(k) for k in KS},
    }


def results_csv(acc, k) -> bytes:
    _, cnt, sal = acc
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([f"skill_{i + 1}" for i in range(k)] + ["vacancies", "n_salary", "median_salary"])
    for key, c in sorted(cnt[k].items(), key=lambda kv: (-kv[1], kv[0])):
        s = sal[k].get(key, [])
        w.writerow(list(key) + [c, len(s), round(statistics.median(s), 2) if s else ""])
    return buf.getvalue().encode()


class Coordinator:
    def __init__(self, cfg: Settings, log: logging.Logger):
        self.cfg, self.log = cfg, log
        self.s3 = s3()
        self.zk = KazooClient(hosts=cfg.hosts, timeout=cfg.session_timeout)
        self.zk.add_listener(lambda state: log.warning("состояние сессии ZooKeeper: %s", state))
        self.epoch = 0

    def connect(self):
        self.zk.start(timeout=60)
        self.log.info("подключился к ZooKeeper %s, сессия 0x%x, таймаут сессии %.0f с",
                      self.cfg.hosts, self.zk.client_id[0], self.cfg.session_timeout)
        for path, value in ((EPOCH, b"0"), (CHECKPOINT, b""), (RESULT, b"")):
            try:
                self.zk.create(path, value, makepath=True)
            except NodeExistsError:
                pass

    def close(self):
        self.zk.stop()
        self.zk.close()

    def wait_for_leadership(self):
        me = self.zk.create(f"{ELECTION}/n_", self.cfg.name.encode(), ephemeral=True, sequence=True,
                            makepath=True)
        seq = me.rsplit("/", 1)[1]
        self.log.info("встал в очередь выборов: %s", me)
        while True:
            children = sorted(self.zk.get_children(ELECTION))
            i = children.index(seq)
            if i == 0:
                return
            prev = f"{ELECTION}/{children[i - 1]}"
            gone = threading.Event()
            if self.zk.exists(prev, watch=lambda _event: gone.set()):
                self.log.info("запасной: жду удаления предыдущего узла %s", prev)
                gone.wait()
                self.log.info("предыдущий узел %s удалён", prev)

    def take_epoch(self):
        while True:
            _, stat = self.zk.get(EPOCH)
            try:
                stat = self.zk.set(EPOCH, str(stat.version + 1).encode(), version=stat.version)
            except BadVersionError:
                continue
            self.epoch = stat.version
            return

    def publish(self, path, doc: dict):
        data = json.dumps(doc, ensure_ascii=False).encode()
        if not self.cfg.fencing:
            return self.zk.retry(self.zk.set, path, data)

        def fenced():
            tx = self.zk.transaction()
            tx.check(EPOCH, self.epoch)
            tx.set_data(path, data)
            results = tx.commit()
            for r in results:
                if isinstance(r, Exception) and not isinstance(r, RolledBackError):
                    raise r
            return results[1]

        return self.zk.retry(fenced)

    def restore(self) -> Progress:
        raw, _ = self.zk.get(CHECKPOINT)
        if not raw:
            self.log.info("чекпоинта нет, начинаю расчёт с начала")
            return Progress()
        meta = json.loads(raw)
        blob = self.s3.get_object(Bucket=BUCKET, Key=meta["key"])["Body"].read()
        p = Progress.load(blob)
        self.log.info("продолжил с чекпоинта: блоков=%d, смещение=%d, чекпоинт эпохи %d от %s (%s)",
                      p.chunks, p.pos, meta["epoch"], meta["by"], meta["key"])
        return p

    def checkpoint(self, p: Progress):
        key = f"{S3_PREFIX}/checkpoints/epoch{self.epoch}-chunk{p.chunks:04d}.json.gz"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=p.dump())
        stat = self.publish(CHECKPOINT, {"epoch": self.epoch, "by": self.cfg.name, "chunks": p.chunks,
                                         "pos": p.pos, "key": key})
        self.log.info("чекпоинт: блоков=%d, смещение=%d, %s, /checkpoint version=%d",
                      p.chunks, p.pos, key, stat.version)

    def compute(self, p: Progress):
        cfg = self.cfg
        for block in iter_blocks(self.s3, cfg.chunk_mb << 20, cfg.io_threads, start=p.pos):
            first = p.header is None
            if first:
                p.header = header_of(block)
            merge(p.acc, map_opt(block, p.header, first, p.chunks, 0, -1))
            p.pos += len(block)
            p.chunks += 1
            self.log.info("блок %d обработан, смещение=%d, java=%d", p.chunks, p.pos, p.acc[0]["java"])
            time.sleep(cfg.step_delay)
            if p.chunks % cfg.ckpt_every == 0:
                self.checkpoint(p)

    def run(self):
        self.connect()
        self.wait_for_leadership()
        self.take_epoch()
        self.log.info("стал лидером, эпоха=%d, FENCING=%d", self.epoch, self.cfg.fencing)
        p = self.restore()
        self.compute(p)
        control = control_numbers(p.acc)
        self.log.info("расчёт завершён, блоков=%d; контрольные числа: %s",
                      p.chunks, json.dumps(control, ensure_ascii=False))
        prefix = f"{S3_PREFIX}/results/epoch{self.epoch}"
        for k in KS:
            self.s3.put_object(Bucket=BUCKET, Key=f"{prefix}/skills_k{k}.csv", Body=results_csv(p.acc, k))
        stat = self.publish(RESULT, {"epoch": self.epoch, "by": self.cfg.name, "prefix": prefix,
                                     "control": control})
        self.log.info("записал результат: /result version=%d, эпоха=%d, %s", stat.version, self.epoch, prefix)


def setup_logging(cfg: Settings) -> logging.Logger:
    os.makedirs(cfg.log_dir, exist_ok=True)
    started = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    path = os.path.join(cfg.log_dir, f"{cfg.name}-{started}-pid{os.getpid()}.log")
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d UTC [%(name)s pid=%(process)d] %(levelname)s %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    fmt.converter = time.gmtime
    handlers = [logging.FileHandler(path, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    return logging.getLogger(cfg.name)


def main() -> int:
    cfg = Settings()
    log = setup_logging(cfg)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    log.info("старт координатора %s: %s", cfg.name, cfg)
    coord = Coordinator(cfg, log)
    try:
        coord.run()
    except BadVersionError:
        current, _ = coord.zk.get(EPOCH)
        log.error("BadVersionError: запись отклонена — моя эпоха %d устарела, текущая эпоха %s; "
                  "результат не записан, завершаюсь", coord.epoch, current.decode())
        return 2
    finally:
        coord.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
