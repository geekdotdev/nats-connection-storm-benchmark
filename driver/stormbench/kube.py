# SPDX-License-Identifier: Apache-2.0
"""The Kubernetes backend for the run loop: what "a fresh broker" and "a storm" mean on a cluster.

Every kubectl call goes through k8s.kubectl_command, so it uses the benchmark's own kubeconfig and never the user's
default context. Targets: local (kind, exercised) and eks (UNTESTED against a real cluster).
"""
from __future__ import annotations

import copy
import json
import tempfile
import time
from pathlib import Path
from typing import Callable, Sequence

from . import k8s, variants as variants_mod
from .ab import StormFailed, Variant
from .runner import Runner

NAMESPACE = "storm-bench"
# The overlay each target applies by default. `bench run --stack-overlay` picks another one, e.g. the cluster variant, and a
# variant can name its own.
STACK_OVERLAYS = {"local": "deploy/stack/overlays/local", "eks": "deploy/stack/overlays/eks"}
LOADGEN_OVERLAYS = {"local": "deploy/loadgen/overlays/local", "eks": "deploy/loadgen/overlays/eks"}


class KubeBackend:
    def __init__(self, run: Runner, *, target: str = "local", stack: str | None = None, loadgen: str | None = None,
                 settle_s: float = 8.0, job_timeout_s: float = 900.0, poll_s: float = 2.0,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> None:
        self.run, self.target = run, target
        self.stack = stack or STACK_OVERLAYS[target]
        self.loadgen = loadgen or LOADGEN_OVERLAYS[target]
        self.settle_s, self.job_timeout_s, self.poll_s = settle_s, job_timeout_s, poll_s
        self.sleep, self.clock = sleep, clock
        self._job: dict | None = None
        self._tmp: tempfile.TemporaryDirectory | None = None

    def __enter__(self) -> "KubeBackend":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        """Remove the generated kustomizations."""
        if self._tmp:
            self._tmp.cleanup()
            self._tmp = None

    def _apply_variant(self, v: Variant, stamp: str) -> None:
        """Apply the variant's kustomization (its overlay, broker image and settings, and a restart stamp). Written fresh each
        time, because the stamp is what makes the broker and callout pods new."""
        if self._tmp is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="storm-variants-")
        d = variants_mod.write(v, self.stack, Path(self._tmp.name), stamp)
        self.run(self._k("apply", "-k", str(d)))

    def _k(self, *args: str) -> list[str]:
        return k8s.kubectl_command(*args, target=self.target)

    def _kc(self, *args: str) -> list[str]:
        return self._k("-n", NAMESPACE, *args)

    # -- setup ---------------------------------------------------------------------------------------------
    def prepare(self, variants: Sequence[Variant]) -> None:
        """Apply the stack in the first variant's configuration, wait for it, and load the storm Job template. The Secrets must
        exist (bench secrets create). Applying the first variant here, rather than the bare overlay, means the broker never
        runs a placeholder image the cluster may not have."""
        self._apply_variant(variants[0], time.strftime("%Y-%m-%dT%H:%M:%S%z"))
        for d in ("statefulset/broker", "deployment/callout"):
            self.run(self._kc("rollout", "status", d, "--timeout=180s"))
        rendered = self.run.output(self._k("kustomize", self.loadgen))
        as_json = self.run.output(self._k("create", "--dry-run=client", "-o", "json", "-f", "-"), input=rendered)
        self._job = json.loads(as_json)

    # -- Backend -------------------------------------------------------------------------------------------
    def start_variant(self, variant: Variant) -> dict:
        # One apply puts the stack in the variant's configuration and replaces the broker and callout pods (the stamp), so a
        # setting an earlier variant made is removed, not left behind.
        self._apply_variant(variant, time.strftime("%Y-%m-%dT%H:%M:%S%z"))
        for d in ("statefulset/broker", "deployment/callout"):
            self.run(self._kc("rollout", "status", d, "--timeout=180s"))
        self.sleep(self.settle_s)  # let the callout connect and the broker settle before the first storm
        return self._broker_facts()

    def _broker_facts(self) -> dict:
        pods = json.loads(self.run.output(self._kc("get", "pods", "-l", "app.kubernetes.io/name=broker", "-o", "json"), quiet=True))
        live = [p for p in pods["items"] if "deletionTimestamp" not in p["metadata"]]
        cs = live[0]["status"]["containerStatuses"][0] if live else {}
        version = self.run.output(self._kc("exec", "statefulset/broker", "--", "/usr/local/bin/nats-server", "--version"), quiet=True).strip()
        return {"image_id": cs.get("imageID", ""), "pod_image": cs.get("image", ""), "server_version": version}

    def run_storm(self, label: str, env: dict[str, str]) -> str:
        if self._job is None:
            raise RuntimeError("prepare() has not been called")
        self.run(self._kc("delete", "job", "loadgen", "--ignore-not-found", "--wait=true"))
        job = copy.deepcopy(self._job)
        container = job["spec"]["template"]["spec"]["containers"][0]
        vars_ = {e["name"]: e for e in container.get("env", [])}
        for k, v in env.items():
            vars_[k] = {"name": k, "value": v}
        container["env"] = list(vars_.values())
        self.run(self._k("create", "-f", "-"), input=json.dumps(job))

        deadline = self.clock() + self.job_timeout_s
        while True:
            st = json.loads(self.run.output(self._kc("get", "job", "loadgen", "-o", "json"), quiet=True)).get("status", {})
            if st.get("succeeded", 0) >= 1 or st.get("failed", 0) >= 1:
                break
            if self.clock() > deadline:
                raise StormFailed(f"{label}: the Job did not finish within {self.job_timeout_s:.0f} s")
            self.sleep(self.poll_s)
        log = self.run.output(self._kc("logs", "job/loadgen"), quiet=True)
        if st.get("failed", 0) >= 1:
            raise StormFailed(f"{label}: the Job failed; last log lines:\n" + "\n".join(log.splitlines()[-5:]))
        return log

    # -- facts for environment.json -------------------------------------------------------------------------
    def describe(self) -> dict:
        nodes = json.loads(self.run.output(self._k("get", "nodes", "-o", "json"), quiet=True))["items"]
        return {"nodes": [{"os_image": n["status"]["nodeInfo"]["osImage"], "kernel": n["status"]["nodeInfo"]["kernelVersion"],
                           "kubelet": n["status"]["nodeInfo"]["kubeletVersion"],
                           "container_runtime": n["status"]["nodeInfo"]["containerRuntimeVersion"],
                           "cpu": n["status"]["capacity"]["cpu"], "memory": n["status"]["capacity"]["memory"]} for n in nodes]}
