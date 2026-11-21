# SPDX-License-Identifier: Apache-2.0
"""The CPU probe and Prometheus: deploying them, noticing that they are there, and taking a run's series out of Prometheus.

The probe (components/cpuprobe) is a DaemonSet that reads the cgroup counters of the benchmark's containers at the moment it is asked.
Prometheus scrapes it every second and keeps the series on a PersistentVolumeClaim (an EBS volume on EKS). The load generator asks the
probes itself at the start and end of every burst, so a burst's report carries the broker's and callout's CPU; Prometheus adds the
continuous record, which is exported into the run's results when the run completes.

Monitoring is optional and is found, not required: a run uses it when it is installed (`bench monitoring up`), and otherwise reads CPU
the way it did before (the driver's `kubectl exec`, whole storms only). `bench run --no-monitoring` ignores an installed one.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from . import k8s
from .config import NAMESPACE
from .runner import Runner

OVERLAYS = {"local": "deploy/monitoring/overlays/local", "eks": "deploy/monitoring/overlays/eks"}
PROBE_PORT = 9100
PROBE_ADDR = f"cpuprobe.{NAMESPACE}.svc.cluster.local:{PROBE_PORT}"   # the headless Service: resolves to every probe pod
PROMETHEUS_PVC = "data-prometheus-0"                                  # what the StatefulSet's volumeClaimTemplate makes
METRICS = ("storm_cpu_usage_seconds_total", "storm_cpu_periods_total", "storm_cpu_throttled_periods_total",
           "storm_cpu_throttled_seconds_total", "storm_cpu_limit_cores")
STEP_S = 1                  # the scrape interval: a finer step would only repeat samples
MAX_POINTS = 9000           # per query: Prometheus refuses more than 11,000 points per series, so a long run is fetched in windows


def overlay_for(target: str) -> str:
    if target not in OVERLAYS:
        raise ValueError(f"no monitoring overlay for target {target!r}")
    return OVERLAYS[target]


def _k(target: str, *args: str) -> list[str]:
    return k8s.kubectl_command("-n", NAMESPACE, *args, target=target)


def installed(run: Runner, target: str) -> bool:
    """Whether the probe and Prometheus are both in the cluster. A read-only yes/no."""
    try:
        return run.succeeds(_k(target, "get", "daemonset/cpuprobe", "statefulset/prometheus"))
    except k8s.NoCluster:   # a run only asks once its cluster has been read, so this means "nothing there to find"
        return False


def up(run: Runner, target: str) -> None:
    """Apply the monitoring overlay and wait for the probe on every node and for Prometheus."""
    run(k8s.kubectl_command("apply", "-k", overlay_for(target), target=target))
    run(_k(target, "rollout", "status", "daemonset/cpuprobe", "--timeout=180s"))
    run(_k(target, "rollout", "status", "statefulset/prometheus", "--timeout=300s"))


def down(run: Runner, target: str, purge: bool = False) -> None:
    """Remove the probe and Prometheus. The claim, and the data on it, stay unless `purge`: deleting the claim deletes the volume (on EKS an
    EBS volume that bills for as long as it exists, so it is deleted before the cluster is)."""
    run(k8s.kubectl_command("delete", "-k", overlay_for(target), "--ignore-not-found", "--wait=true", target=target))
    if purge:
        run(_k(target, "delete", "pvc", PROMETHEUS_PVC, "--ignore-not-found", "--wait=true", "--timeout=300s"))


def range_path(query: str, start: float, end: float, step: int = STEP_S) -> str:
    """The API-server proxy path for a Prometheus range query: kubectl can GET it with the cluster's own credentials, no port-forward."""
    qs = urllib.parse.urlencode({"query": query, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": f"{step}s"})
    return f"/api/v1/namespaces/{NAMESPACE}/services/prometheus:9090/proxy/api/v1/query_range?{qs}"


def windows(start: float, end: float, step: int = STEP_S, max_points: int = MAX_POINTS) -> list[tuple[float, float]]:
    """[start, end] cut into windows of at most max_points steps, which do not overlap and between them include every sample (the last
    window can be a single instant, which is a valid query)."""
    if end <= start:
        return []
    size = step * max_points
    out, t = [], start
    while t <= end:
        out.append((t, min(t + size, end)))
        t += size + step
    return out


def _fetch(run: Runner, target: str, path: str, attempts: int = 3) -> dict:
    last: Exception | None = None
    for _ in range(attempts):
        try:
            doc = json.loads(run.output(k8s.kubectl_command("get", "--raw", path, target=target), quiet=True))
            if doc.get("status") == "success":
                return doc
            last = RuntimeError(f"Prometheus said {doc.get('status')}: {doc.get('error', '')}")
        except (subprocess.CalledProcessError, ValueError) as e:
            last = e
        time.sleep(1)
    raise RuntimeError(f"could not read {path.split('?')[0]}: {last}")


def _key(metric: dict) -> tuple:
    return tuple(sorted(metric.items()))


def export(run: Runner, target: str, out: Path, start: float, end: float, *, say=print) -> dict:
    """Write the series of a run (from `start` to `end`, epoch seconds) into out/prometheus/<metric>.json, merged across windows, and an
    index.json saying what they are. Returns the index. Raises when Prometheus cannot be read: the caller decides what that costs."""
    folder = out / "prometheus"
    folder.mkdir(parents=True, exist_ok=True)
    index: dict = {"source": "Prometheus, scraping the CPU probes every second", "start": start, "end": end, "step_s": STEP_S, "metrics": {}}
    for metric in METRICS:
        merged: dict[tuple, dict] = {}
        for lo, hi in windows(start, end):
            for series in _fetch(run, target, range_path(metric, lo, hi))["data"]["result"]:
                got = merged.setdefault(_key(series["metric"]), {"metric": series["metric"], "values": []})
                got["values"].extend(series["values"])
        result = sorted(merged.values(), key=lambda s: (s["metric"].get("component", ""), s["metric"].get("pod", ""), s["metric"].get("container", "")))
        (folder / f"{metric}.json").write_text(json.dumps({"resultType": "matrix", "result": result}) + "\n")
        index["metrics"][metric] = {"series": len(result), "samples": sum(len(s["values"]) for s in result)}
    (folder / "index.json").write_text(json.dumps(index, indent=1) + "\n")
    say(f"exported {sum(m['samples'] for m in index['metrics'].values())} samples of {len(METRICS)} metrics from Prometheus to {folder}")
    return index
