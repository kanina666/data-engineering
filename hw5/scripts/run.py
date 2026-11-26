import argparse
import base64
import json
import os
import re
import secrets
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import yaml

import pipeline


ROOT = Path(__file__).resolve().parents[1]
MANIFESTS = ROOT / "manifests"
EVIDENCE = ROOT / "evidence"
RESULTS = ROOT / "results"
KUBE = ["kubectl", "--context", "kind-hw5"]
NS = ["-n", "hw5"]
QUOTA_NS = ["-n", "hw5-quota"]


class Journal:
    def __init__(self, name):
        self.path = EVIDENCE / f"{name}.log"
        self.path.write_text("", encoding="utf-8")

    def say(self, message):
        for original, replacement in ((sys.executable, "python"), (str(ROOT.parent), ".")):
            message = message.replace(original, replacement)
            message = message.replace(original.replace("\\", "\\\\"), replacement)
        print(message, flush=True)
        with self.path.open("a", encoding="utf-8") as file:
            file.write(message + "\n")

    def run(self, args, *, allowed=(0,), stdin=None):
        self.say("$ " + subprocess.list2cmdline([str(arg) for arg in args]))
        result = subprocess.run(args, input=stdin, capture_output=True, text=True,
                                encoding="utf-8", errors="replace", timeout=660)
        if result.stdout:
            self.say(result.stdout.rstrip())
        if result.stderr:
            self.say(result.stderr.rstrip())
        if result.returncode not in allowed:
            raise RuntimeError(f"Command exited {result.returncode}. See {self.path.name}")
        return result.stdout, result.stderr

    def kube(self, *args, **kwargs):
        return self.run([*KUBE, *args], **kwargs)[0]

    def obj(self, *args):
        return json.loads(self.kube(*args, "-o", "json"))

    def apply(self, obj):
        path = EVIDENCE / f"{obj['metadata']['name']}.yaml"
        path.write_text(yaml.safe_dump(obj, sort_keys=False), encoding="utf-8")
        self.kube("apply", "-f", path)


def eventually(probe, timeout=150):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(2)
    raise TimeoutError(f"Condition did not hold after {timeout}s")


def setup(log):
    manifest = MANIFESTS / "rustfs.yaml"
    log.kube("apply", "-f", manifest)
    secret = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
              "metadata": {"name": "s3-credentials", "namespace": "hw5"},
              "stringData": {key: os.environ[key] for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}}
    log.kube("apply", "-f", "-", stdin=json.dumps(secret))
    log.kube(*NS, "rollout", "status", "deployment/rustfs", "--timeout=120s")
    log.kube(*NS, "get", "pods,svc", "-o", "wide")


def job(log, name, args, memory, request=64, *, indexed=False):
    spec = yaml.safe_load((MANIFESTS / "job.yaml").read_text(encoding="utf-8"))
    spec["metadata"]["name"] = name
    container = spec["spec"]["template"]["spec"]["containers"][0]
    container["args"] = args
    container["resources"]["requests"]["memory"] = f"{request}Mi"
    container["resources"]["limits"]["memory"] = f"{memory}Mi"
    if indexed:
        spec["spec"].update(completionMode="Indexed", completions=8, parallelism=4,
                            backoffLimit=8, backoffLimitPerIndex=1)
    log.apply(spec)


def wait_job(log, name, succeeds=True):
    desired = "complete" if succeeds else "failed"
    log.kube(*NS, "wait", f"--for=condition={desired}", f"job/{name}", "--timeout=610s")
    log.kube(*NS, "get", "job", name, "-o", "wide")
    selector = f"batch.kubernetes.io/job-name={name}"
    pods = log.obj(*NS, "get", "pods", "-l", selector)["items"]
    logs = {}
    for pod in sorted(pods, key=lambda item: item["metadata"]["name"]):
        pod_name = pod["metadata"]["name"]
        logs[pod_name] = log.kube(*NS, "logs", pod_name)
        log.kube(*NS, "get", "events", "--field-selector", f"involvedObject.name={pod_name}")
    return pods, logs


def data(args):
    log = Journal("s3")
    setup(log)
    forward = [*KUBE, *NS, "port-forward", "--address=127.0.0.1", "service/rustfs", "19050:9000"]
    log.say("$ " + subprocess.list2cmdline(forward))
    path = EVIDENCE / "port-forward.log"
    with path.open("w", encoding="utf-8") as port_log:
        proc = subprocess.Popen(forward, stdout=port_log, stderr=subprocess.STDOUT,
                                env={**os.environ, "KUBECTL_PORT_FORWARD_WEBSOCKETS": "false"})
        try:
            os.environ["S3_ENDPOINT"] = "http://127.0.0.1:19050"
            eventually(lambda: "Forwarding from" in (EVIDENCE / "port-forward.log").read_text())
            source = (["--source-endpoint", args.source_endpoint] if args.source_endpoint
                      else [str(args.dataset)])
            script = str(Path(pipeline.__file__))
            log.run([sys.executable, "-u", script, "ingest", *source])
            log = Journal("parallel")
            job(log, "skill-workers", ["worker", "parallel-v1", "--fail-index", "2"],
                192, indexed=True)
            pods, _ = wait_job(log, "skill-workers")
            retries = [p for p in pods if p["status"]["phase"] == "Failed"]
            if len(retries) != 1 or retries[0]["metadata"]["annotations"][
                "batch.kubernetes.io/job-completion-index"] != "2":
                raise RuntimeError("Expected one failed attempt for index 2")
            job(log, "skill-reduce", ["reduce", "parallel-v1"], 192)
            wait_job(log, "skill-reduce")
            log.run([sys.executable, script, "fetch", "hw5/runs/parallel-v1/top.csv",
                     RESULTS / "parallel.csv"])
            Journal("compare").run([sys.executable, script, "compare",
                                    RESULTS / "reference.csv", RESULTS / "parallel.csv"])
            log = Journal("memory")
            job(log, "memory-low", ["memory", "memory-low"], 64, request=32)
            pods, _ = wait_job(log, "memory-low", succeeds=False)
            terminated = pods[0]["status"]["containerStatuses"][0]["state"]["terminated"]
            if terminated["reason"] != "OOMKilled":
                raise RuntimeError(f"Expected OOMKilled, received {terminated}")

            job(log, "memory-measure", ["memory", "memory-measure"], 1024, request=128)
            _, outputs = wait_job(log, "memory-measure")
            peak = int(re.search(r"peak_bytes=(\d+)", next(iter(outputs.values())))[1])
            mib = 1 << 20
            margin = max((peak + 4) // 5, 32 * mib)
            working = (peak + margin + mib - 1) // mib
            log.say(f"peak_bytes={peak} peak_MiB={peak / mib:.3f} "
                    f"margin_MiB={margin / mib:.3f} working_limit_MiB={working}")

            job(log, "memory-working", ["memory", "memory-working"], working, request=128)
            wait_job(log, "memory-working")
            log.run([sys.executable, script, "fetch", "hw5/memory-working/top.csv",
                     RESULTS / "single.csv"])
            log.run([sys.executable, script, "compare", RESULTS / "reference.csv",
                     RESULTS / "single.csv"])
        finally:
            proc.terminate()
            proc.wait(timeout=10)
    retain_forward_summary(path)


def retain_forward_summary(path):
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    keep = ("Forwarding from ", "Handling connection", "Error", "error:", "E")
    path.write_text("".join(line + "\n" for line in lines if line.startswith(keep)), encoding="utf-8")


def millicores(quantity):
    if quantity.endswith("m"):
        return int(quantity[:-1])
    return int(Decimal(quantity) * 1000)


def free_cpu(log):
    node = log.obj("get", "nodes")["items"][0]
    name = node["metadata"]["name"]
    allocatable = millicores(node["status"]["allocatable"]["cpu"])
    pods = log.obj("get", "pods", "-A", "--field-selector", f"spec.nodeName={name}")["items"]
    allocated = 0
    for pod in pods:
        if pod["status"]["phase"] in ("Succeeded", "Failed"):
            continue
        spec = pod["spec"]
        base = spec.get("resources", {}).get("requests", {}).get("cpu")
        if base is None:
            sidecars = 0
            init_peak = 0
            for container in spec.get("initContainers", []):
                request = cpu_request(container)
                if container.get("restartPolicy") == "Always":
                    sidecars += request
                    init_peak = max(init_peak, sidecars)
                else:
                    init_peak = max(init_peak, sidecars + request)
            base = f"{max(init_peak, sidecars + sum(map(cpu_request, spec['containers'])))}m"
        request = millicores(base) + millicores(spec.get("overhead", {}).get("cpu", "0"))
        allocated += request
        log.say(f"request {pod['metadata']['namespace']}/{pod['metadata']['name']} = {request}m")
    free = allocatable - allocated
    log.say(f"node={name} allocatable={allocatable}m existing_requests={allocated}m free={free}m")
    if free < 500:
        raise ValueError("The experiment needs at least 500m free CPU")
    return name, free


def cpu_request(container):
    return millicores(container.get("resources", {}).get("requests", {}).get("cpu", "0"))


def pod(log, namespace, name, cpu, *, node=None, priority=None, limit=None):
    spec = {"restartPolicy": "Never", "terminationGracePeriodSeconds": 3,
            "containers": [{"name": "sleep", "image": "hw5-pairs:v1", "imagePullPolicy": "Never",
                            "command": ["sleep", "3600"],
                            "resources": {"requests": {"cpu": f"{cpu}m", "memory": "32Mi"},
                                          "limits": {"cpu": f"{limit or cpu}m", "memory": "64Mi"}}}]}
    if node:
        spec["nodeSelector"] = {"kubernetes.io/hostname": node}
    if priority:
        spec["priorityClassName"] = priority
    obj = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": namespace},
           "spec": spec}
    if name == "quota-over":
        path = EVIDENCE / f"{name}.yaml"
        path.write_text(yaml.safe_dump(obj, sort_keys=False), encoding="utf-8")
        return path
    log.apply(obj)


def events(log, name):
    return log.kube(*NS, "get", "events", "--field-selector", f"involvedObject.name={name}")


def ready(log, namespace, name):
    log.kube("-n", namespace, "wait", "--for=condition=Ready", f"pod/{name}", "--timeout=120s")


def resources():
    log = Journal("cpu")
    log.kube("apply", "-f", "-", stdin=json.dumps(
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "hw5"}}))
    node, free = free_cpu(log)
    pod(log, "hw5", "cpu-holder", free - 100, node=node)
    ready(log, "hw5", "cpu-holder")
    pod(log, "hw5", "cpu-waiter", 200, node=node)
    eventually(lambda: "Insufficient cpu" in events(log, "cpu-waiter"))
    assert log.kube(*NS, "get", "pod", "cpu-waiter", "-o", "jsonpath={.status.phase}").strip() == "Pending"
    log.kube(*NS, "describe", "pod", "cpu-waiter")
    log.kube(*NS, "delete", "pod", "cpu-holder", "--timeout=45s")
    ready(log, "hw5", "cpu-waiter")
    log.kube(*NS, "get", "pod", "cpu-waiter", "-o", "wide")
    events(log, "cpu-waiter")
    log.kube(*NS, "delete", "pod", "cpu-waiter", "--timeout=45s")

    log = Journal("preemption")
    log.kube("apply", "-f", MANIFESTS / "priority.yaml")
    node, free = free_cpu(log)
    pod(log, "hw5", "priority-low", free - 100, node=node, priority="hw5-low")
    ready(log, "hw5", "priority-low")
    log.kube(*NS, "get", "pod", "priority-low", "-o", "wide")
    pod(log, "hw5", "priority-high", 200, node=node, priority="hw5-high")
    ready(log, "hw5", "priority-high")
    log.kube(*NS, "get", "pods", "--field-selector", "metadata.name=priority-high", "-o", "wide")
    assert "Preempted" in events(log, "priority-low")
    assert "Scheduled" in events(log, "priority-high")
    log.kube(*NS, "get", "pod", "priority-low", "--ignore-not-found")
    log.kube(*NS, "delete", "pod", "priority-high", "--timeout=45s")

    log = Journal("quota")
    log.kube("apply", "-f", MANIFESTS / "quota.yaml")
    eventually(lambda: log.obj(*QUOTA_NS, "get", "resourcequota", "budget").get("status", {}).get("hard"))
    pod(log, "hw5-quota", "quota-ok", 300, limit=350)
    ready(log, "hw5-quota", "quota-ok")
    log.kube(*QUOTA_NS, "get", "pod", "quota-ok", "-o", "wide")
    path = pod(log, "hw5-quota", "quota-over", 250, limit=250)
    _, stderr = log.run([*KUBE, "create", "-f", path], allowed=(1,))
    assert "Forbidden" in stderr and "exceeded quota" in stderr
    log.kube(*QUOTA_NS, "describe", "resourcequota", "budget")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("data", "resources", "setup"))
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--source-endpoint")
    args = parser.parse_args()
    EVIDENCE.mkdir(exist_ok=True)
    if args.stage == "resources":
        resources()
        return
    if args.stage == "data" and bool(args.dataset) == bool(args.source_endpoint):
        parser.error("Supply either --dataset or --source-endpoint")
    credentials = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
    if not all(os.environ.get(key) for key in credentials):
        if any(os.environ.get(key) for key in credentials):
            parser.error("Supply both S3 credential variables")
        existing = subprocess.run(
            [*KUBE, *NS, "get", "secret", "s3-credentials", "-o", "json"],
            capture_output=True, text=True, timeout=30,
        )
        stored = json.loads(existing.stdout)["data"] if existing.returncode == 0 else {}
        for key in credentials:
            os.environ[key] = base64.b64decode(stored[key]).decode() if stored else secrets.token_hex(18)
    if args.stage == "setup":
        setup(Journal("setup"))
    else:
        data(args)


if __name__ == "__main__":
    main()
