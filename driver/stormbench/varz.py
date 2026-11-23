# SPDX-License-Identifier: Apache-2.0
"""The broker's own `/varz`: memory and connection counts, which the CPU probe does not have (`components/cpuprobe/cgroup.go` reads
cgroup CPU only). `nats-server`'s monitoring HTTP port is already enabled (`http: 0.0.0.0:8222`, `deploy/stack/base/broker-config.yaml`)
but nothing else in this benchmark reads it.

Why this exists: `plans/held-connection-exhaustion.md` asks whether a sustained volume of `forged`/`incompleteHandshake` connections
(each held open for the broker's whole `authorization.timeout`) grows the broker's resident memory toward its cgroup limit, or its live
connection count past what the arrival rate alone predicts. A single reading cannot show that; this module can be polled on an interval
through a burst's whole window to get a time series instead of two snapshots.

How it is read, same as the block profiles in `profiling.py`: no port-forward, the API server's own pod-proxy subresource
(`kubectl get --raw /api/.../pods/<pod>:8222/proxy/varz`), so no `NetworkPolicy` change is needed (it never touches pod-to-pod
networking) and it works whether or not the generator's own egress is restricted.

The field names below (`mem`, `connections`, `total_connections`, `cpu`, `max_connections`) are `nats-server`'s own `Varz` struct
(`server/monitor.go`) and all five were confirmed against a real broker in `eks-exhaustion-calibration-2026-11-22`. `parse` still returns
whatever of them is present and does not fail on new or missing fields, so a future `nats-server` renaming one shows up as a `None`
field, not a crash."""
from __future__ import annotations

import json
import subprocess
import time

from . import k8s
from .config import NAMESPACE
from .runner import Runner

PORT = 8222
FIELDS = ("mem", "connections", "total_connections", "cpu", "max_connections")


def _proxy(pod: str, rest: str) -> str:
    return f"/api/v1/namespaces/{NAMESPACE}/pods/{pod}:{PORT}/proxy/{rest}"


def parse(text: str) -> dict | None:
    """The fields of interest out of a `/varz` response's JSON text; None when it is not JSON (an empty or malformed reply, e.g. the
    broker was not ready yet). A field `/varz` does not have (a renamed one, an older or newer `nats-server`) is left out, not defaulted,
    so a caller can tell "zero" from "not read"."""
    try:
        doc = json.loads(text)
    except ValueError:
        return None
    if not isinstance(doc, dict):
        return None
    return {k: doc[k] for k in FIELDS if k in doc}


def fetch(run: Runner, target: str, pod: str) -> dict | None:
    """One reading of the broker pod's `/varz`, tagged with the wall-clock time it was taken. None when it could not be read at all
    (the pod is gone, or answered something that is not JSON) -- the caller decides whether that is worth stopping over."""
    try:
        text = run.output(k8s.kubectl_command("get", "--raw", _proxy(pod, "varz"), target=target), quiet=True)
    except subprocess.CalledProcessError:
        return None
    got = parse(text)
    return {**got, "atMs": int(time.time() * 1000)} if got is not None else None


def poll(run: Runner, target: str, pod: str, interval_s: float, stop, *, say=print) -> list[dict]:
    """Read `/varz` every `interval_s` seconds until `stop` (a `threading.Event`, or anything with `.is_set()`) is set, meant to run in
    its own thread alongside a burst so the result is a time series through the whole window, not two snapshots at its edges. A reading
    that fails is skipped, not fatal: the poll keeps going on its own schedule (a slow or missed read should not also push every later
    one later), and says so once rather than once per miss."""
    out: list[dict] = []
    said = False
    while not stop.is_set():
        reading = fetch(run, target, pod)
        if reading is not None:
            out.append(reading)
        elif not said:
            say(f"varz: could not read {pod}'s /varz (will keep trying, quietly)")
            said = True
        stop.wait(interval_s)
    return out
