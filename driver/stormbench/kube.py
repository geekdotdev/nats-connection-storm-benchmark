# SPDX-License-Identifier: Apache-2.0
"""The Kubernetes backend for the run loop: what "a fresh broker" and "a storm" mean on a cluster.

Every kubectl call goes through k8s.kubectl_command, so it uses the benchmark's own kubeconfig and never the user's
default context. Targets: local (kind, exercised) and eks (UNTESTED against a real cluster).
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Sequence

from . import cpu as cpu_mod, k8s, monitoring, variants as variants_mod, varz as varz_mod
from .config import NAMESPACE
from .ab import StormFailed, Variant
from .runner import Runner

# The overlay each target applies by default. `bench run --stack-overlay` picks another one, e.g. the cluster variant, and a
# variant can name its own.
STACK_OVERLAYS = {"local": "deploy/stack/overlays/local", "eks": "deploy/stack/overlays/eks"}
LOADGEN_OVERLAYS = {"local": "deploy/loadgen/overlays/local", "eks": "deploy/loadgen/overlays/eks"}


# What the broker and generator pods wait for in operator mode, made by the callout pod's provision init container.
PROVISION_OBJECTS = ("storm-operator", "storm-callout-auth", "storm-generator-auth")
PROVISION_HINT = ("the broker and generator pods wait for objects (the ConfigMap storm-operator, the Secrets storm-callout-auth and "
                  "storm-generator-auth) that the callout pod's init container `provision` creates: look at "
                  f"`kubectl -n {NAMESPACE} logs deployment/callout -c provision` and `kubectl -n {NAMESPACE} describe pod -l app.kubernetes.io/name=callout`")


def job_name(label: str) -> str:
    """The name of the Job that runs one storm: `loadgen-<label>-<6 hex of the label's SHA-1>`. Each storm has a Job of its own, which
    stays on the cluster until the run is over, so that its log can be read later than the moment it finished (a Job's pod, and its log,
    are kept until the Job is deleted). Kubernetes names are lowercase and cannot hold an underscore; the hash keeps two labels that
    differ only in case, or only in `_` against `-`, apart. At most 55 characters."""
    slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")[:40]
    return f"loadgen-{slug}-{hashlib.sha1(label.encode()).hexdigest()[:6]}"


def label_value(text: str) -> str:
    """A Kubernetes label value (at most 63 characters of letters, digits, `-`, `_`, `.`): the text, or a shortened one with a hash."""
    clean = re.sub(r"[^A-Za-z0-9_.-]", "_", text)
    return clean if len(clean) <= 63 else clean[:54] + "-" + hashlib.sha1(text.encode()).hexdigest()[:8]


# Where the CPU counters of the long-lived components are read from: (component, pod selector, container).
CPU_COMPONENTS = (("broker", "app.kubernetes.io/name=broker", "broker"), ("callout", "app.kubernetes.io/name=callout", "callout"))
CGROUP_FILES = ("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max")

# The labels on a storm's Job: which run, which unit and attempt of it (a unit may be run again, from a fresh broker, and the Jobs of the
# earlier attempt must never be taken for the new one's), and which storm.
LBL_RUN, LBL_UNIT, LBL_ATTEMPT, LBL_STORM = "storm-bench/run-id", "storm-bench/unit", "storm-bench/attempt", "storm-bench/label"
LOADGEN_SELECTOR = "app.kubernetes.io/name=loadgen"


def waiting_report(pods: list[dict], events: list[dict]) -> list[str]:
    """Why each pod that is not ready is not: the reason each container waits or failed, and the latest warning event about it.
    Pure, so it can be tested with canned kubectl output. Ends with a hint when a pod waits for what the provision init container makes."""
    lines: list[str] = []
    for pod in pods:
        name = pod["metadata"]["name"]
        status = pod.get("status", {})
        # A finished pod (the previous storm Job's) and a terminating one (the pod this rollout is replacing) are not what is stuck.
        if status.get("phase") == "Succeeded" or "deletionTimestamp" in pod["metadata"]:
            continue
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", [])):
            continue
        facts: list[str] = []
        for kind, key in (("init container", "initContainerStatuses"), ("container", "containerStatuses")):
            for cs in status.get(key, []):
                state = cs.get("state", {})
                if "waiting" in state:
                    w = state["waiting"]
                    facts.append(f"{kind} {cs['name']} waiting: {w.get('reason', '?')}" + (f" ({w['message']})" if w.get("message") else ""))
                elif "terminated" in state and state["terminated"].get("exitCode", 0) != 0:
                    t = state["terminated"]
                    facts.append(f"{kind} {cs['name']} exited {t.get('exitCode')}: {t.get('reason', '?')}" + (f" ({t['message']})" if t.get("message") else ""))
                last = cs.get("lastState", {}).get("terminated")
                if last and last.get("exitCode", 0) != 0 and "waiting" in state:  # a container that keeps failing is waiting to restart: say why it failed
                    facts.append(f"{kind} {cs['name']} last exited {last.get('exitCode')}" + (f": {last['message'].strip()[:300]}" if last.get("message") else ""))
        mine = [e for e in events if e.get("involvedObject", {}).get("name") == name and e.get("type") == "Warning"]
        mine.sort(key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or "")
        seen: set[str] = set()
        for e in reversed(mine):  # the latest of each reason
            if e.get("reason") not in seen:
                seen.add(e.get("reason"))
                facts.append(f"event {e.get('reason')}: {e.get('message', '')}")
            if len(seen) >= 3:
                break
        lines.append(f"  pod {name} ({status.get('phase', '?')}): " + ("; ".join(facts) if facts else "not ready, no reason reported"))
    if any(o in line for line in lines for o in PROVISION_OBJECTS):
        lines.append("  hint: " + PROVISION_HINT)
    return lines


class KubeBackend:
    def __init__(self, run: Runner, *, target: str = "local", stack: str | None = None, loadgen: str | None = None,
                 settle_s: float = 8.0, job_timeout_s: float = 900.0, poll_s: float = 2.0,
                 read_retries: int = 60, read_retry_wait_s: float = 10.0, read_cpu: bool = True, cpuprobe: bool = False,
                 read_varz: bool = False, varz_interval_s: float = 2.0,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> None:
        self.run, self.target = run, target
        self.stack = stack or STACK_OVERLAYS[target]
        self.loadgen = loadgen or LOADGEN_OVERLAYS[target]
        self.settle_s, self.job_timeout_s, self.poll_s = settle_s, job_timeout_s, poll_s
        self.read_retries, self.read_retry_wait_s = read_retries, read_retry_wait_s
        self.probe_fallback = False                  # set when the generator turned out not to report the probes' readings (see run_storm)
        self.cpuprobe = cpuprobe                     # the CPU probes are installed: the generator asks them itself (STORM_CPUPROBE)
        self.read_cpu = read_cpu                     # read the broker's and callout's CPU around each storm (cpu.py); --no-cpu turns it off
        self.read_varz, self.varz_interval_s = read_varz, varz_interval_s   # poll the broker's /varz through a storm (varz.py); off by default
        self.sleep, self.clock = sleep, clock
        self._job: dict | None = None
        self._ctx = {"run_id": "", "unit": "", "attempt": ""}
        self._generator_resources: dict = {}         # the current variant's cpu/memory for the storm Jobs (variants.py)
        self.last_cpu: dict | None = None            # the CPU use of the storm run_storm ran last (see cpu.py); None when it could not be read
        self.last_varz: list[dict] | None = None     # the /varz series of the storm run_storm ran last (see varz.py); None when none was read
        self._cpu_targets: list[tuple[str, str, str]] | None = None
        self._cpu_notes: set[str] = set()
        self._varz_pod: str | None = None
        self._varz_notes: set[str] = set()
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
    def prepare(self, variants: Sequence[Variant], *, apply: bool = True) -> None:
        """Apply the stack in the first variant's configuration, wait for it, and load the storm Job template. The Secrets must
        exist (bench secrets create). Applying the first variant here, rather than the bare overlay, means the broker never
        runs a placeholder image the cluster may not have.

        With apply=False nothing is changed on the cluster: only the Job template is loaded. A resumed run does that. Applying
        the first variant restarts the broker (every apply carries a new restart stamp), and a unit that a stopped driver left
        running can be finished from the cluster only while its broker is still the pod it was measured on. Every unit's own
        start_variant applies its full configuration anyway, so the up-front apply is only needed on a cluster the run has not touched."""
        if apply:
            self._apply_variant(variants[0], time.strftime("%Y-%m-%dT%H:%M:%S%z"))
            for d in ("statefulset/broker", "deployment/callout"):
                self._wait_rollout(d)
        rendered = self.run.output(self._k("kustomize", self.loadgen))
        as_json = self.run.output(self._k("create", "--dry-run=client", "-o", "json", "-f", "-"), input=rendered)
        # The overlay may render more than the Job (eks-restricted also adds a NetworkPolicy): kubectl prints each resource as
        # its own JSON value, one after another, not a single document, so this must pick the Job out rather than assume it's
        # the only thing there.
        objs = [o for o in k8s.json_stream(as_json) if o.get("kind") == "Job"]
        if len(objs) != 1:
            raise RuntimeError(f"{self.loadgen} must render exactly one Job, found {len(objs)}")
        self._job = objs[0]

    # -- Backend -------------------------------------------------------------------------------------------
    def start_variant(self, variant: Variant) -> dict:
        # One apply puts the stack in the variant's configuration and replaces the broker and callout pods (the stamp), so a
        # setting an earlier variant made is removed, not left behind.
        self._apply_variant(variant, time.strftime("%Y-%m-%dT%H:%M:%S%z"))
        for d in ("statefulset/broker", "deployment/callout"):
            self._wait_rollout(d)
        self._cpu_targets = None   # new pods: found again at the first reading
        self._varz_pod = None      # the broker pod is new too
        self._generator_resources = dict(variant.generator_resources)
        self.sleep(self.settle_s)  # let the callout connect and the broker settle before the first storm
        return self._broker_facts()

    def _wait_rollout(self, what: str) -> None:
        """Wait for a rollout. When it does not finish, say which pods are not ready and why before the error, because a broker pod
        that waits for a ConfigMap the callout's init container has not made looks like a broker that will not start."""
        try:
            self.run(self._kc("rollout", "status", what, "--timeout=180s"))
        except subprocess.CalledProcessError:
            print(f"bench: {what} did not become ready; pods that are not ready:\n{self._not_ready()}", file=sys.stderr)
            raise

    def _not_ready(self) -> str:
        try:
            pods = json.loads(self.run.output(self._kc("get", "pods", "-o", "json"), quiet=True))["items"]
            events = json.loads(self.run.output(self._kc("get", "events", "--field-selector", "type=Warning", "-o", "json"), quiet=True))["items"]
        except (subprocess.CalledProcessError, ValueError, KeyError) as e:
            return f"  (could not read the pods: {e})"
        return "\n".join(waiting_report(pods, events)) or "  (none: every pod is ready)"

    def _broker_facts(self) -> dict:
        pods = json.loads(self.run.output(self._kc("get", "pods", "-l", "app.kubernetes.io/name=broker", "-o", "json"), quiet=True))
        live = [p for p in pods["items"] if "deletionTimestamp" not in p["metadata"]]
        cs = live[0]["status"]["containerStatuses"][0] if live else {}
        version = self.run.output(self._kc("exec", "statefulset/broker", "--", "/usr/local/bin/nats-server", "--version"), quiet=True).strip()
        return {"image_id": cs.get("imageID", ""), "pod_image": cs.get("image", ""), "server_version": version,
                "pod_uid": live[0]["metadata"].get("uid", "") if live else ""}

    def broker_uid(self) -> str:
        """The UID of the broker pod running now ("" if none): the identity of the broker a unit ran on, which a resumed run checks
        before it takes the storms a stopped driver left on the cluster as part of that unit."""
        pods = json.loads(self._read(self._kc("get", "pods", "-l", "app.kubernetes.io/name=broker", "-o", "json"), "reading the broker pod"))
        live = [p for p in pods["items"] if "deletionTimestamp" not in p["metadata"]]
        return live[0]["metadata"].get("uid", "") if live else ""

    # -- CPU use around a storm ----------------------------------------------------------------------------------
    def _cpu_note(self, text: str) -> None:
        if text not in self._cpu_notes:                 # said once each: a reading that cannot be made is not worth a line per storm
            self._cpu_notes.add(text)
            print(f"bench: CPU use not recorded: {text}", file=sys.stderr, flush=True)

    def _find_cpu_targets(self) -> list[tuple[str, str, str]]:
        """The pods whose CPU is read, found once per unit (a broker restart makes new pods, and start_variant forgets these)."""
        found = []
        for comp, selector, container in CPU_COMPONENTS:
            try:
                pods = json.loads(self.run.output(self._kc("get", "pods", "-l", selector, "-o", "json"), quiet=True))["items"]
            except (subprocess.CalledProcessError, ValueError, KeyError) as e:
                self._cpu_note(f"could not list the {comp} pods ({e})")
                continue
            found += [(comp, p["metadata"]["name"], container) for p in pods if "deletionTimestamp" not in p["metadata"]]
        return found

    def cpu_snapshot(self) -> dict:
        """{component: {pod: cumulative counters}}, read now. One `kubectl exec ... cat` per pod, a single attempt each: this is a
        measurement taken beside the storm, and a failure to make it must neither stop the run nor make it wait."""
        if self._cpu_targets is None:
            self._cpu_targets = self._find_cpu_targets()
        snap: dict = {}
        for comp, pod, container in self._cpu_targets:
            try:
                text = self.run.output(self._kc("exec", pod, "-c", container, "--", "cat", *CGROUP_FILES), quiet=True)
            except subprocess.CalledProcessError as e:
                self._cpu_note(f"could not read the cgroup of {pod} (exit {e.returncode}); the image needs `cat` and the node cgroup v2")
                continue
            reading = cpu_mod.parse(text)
            if reading is None:
                self._cpu_note(f"{pod} has no cgroup v2 cpu.stat (cgroup v1 nodes are not read)")
                continue
            snap.setdefault(comp, {})[pod] = reading
        return snap

    # -- the broker's /varz through a storm -----------------------------------------------------------------------
    def _find_varz_pod(self) -> str | None:
        """The broker pod's name, found once per unit (a broker restart makes a new pod, and start_variant forgets this). None, said
        once, when it cannot be found: a missing broker is covered elsewhere (_wait_rollout), so this just has nothing to poll."""
        if self._varz_pod is None:
            try:
                pods = json.loads(self.run.output(self._kc("get", "pods", "-l", "app.kubernetes.io/name=broker", "-o", "json"), quiet=True))["items"]
            except (subprocess.CalledProcessError, ValueError, KeyError) as e:
                self._varz_say(f"could not list the broker pods for /varz ({e})")
                return None
            live = [p["metadata"]["name"] for p in pods if "deletionTimestamp" not in p["metadata"]]
            if not live:
                self._varz_say("no broker pod to read /varz from")
                return None
            self._varz_pod = live[0]
        return self._varz_pod

    def _varz_say(self, message: str) -> None:
        if message not in self._varz_notes:   # said once per distinct message, like a CPU reading that cannot be made (_cpu_note)
            self._varz_notes.add(message)
            print(f"bench: {message}", file=sys.stderr, flush=True)

    def _start_varz_poll(self) -> tuple[threading.Thread, threading.Event, list] | None:
        """Starts a background poll of the broker's /varz, if read_varz is on and the broker pod can be found. The result list is
        filled in by the polling thread; read it only after joining (see _stop_varz_poll)."""
        if not self.read_varz:
            return None
        pod = self._find_varz_pod()
        if pod is None:
            return None
        stop = threading.Event()
        result: list[dict] = []
        thread = threading.Thread(target=lambda: result.extend(varz_mod.poll(self.run, self.target, pod, self.varz_interval_s, stop, say=self._varz_say)),
                                  daemon=True)
        thread.start()
        return thread, stop, result

    def _stop_varz_poll(self, polling: tuple[threading.Thread, threading.Event, list] | None) -> list[dict] | None:
        """Stops a poll _start_varz_poll began and returns its series (None if none was started, or it read nothing)."""
        if polling is None:
            return None
        thread, stop, result = polling
        stop.set()
        thread.join(timeout=self.varz_interval_s * 5)
        if thread.is_alive():
            self._varz_say("a /varz poll did not stop in time; its thread is left running (daemon, so it will not block process exit)")
        return result or None

    # -- the storms' Jobs ----------------------------------------------------------------------------------------
    def set_context(self, run_id: str, unit: str, attempt: str) -> None:
        """What the Jobs made from now on are labelled with: the run, the unit and the attempt at it."""
        self._ctx = {"run_id": run_id, "unit": unit, "attempt": attempt}

    def clear_unit_jobs(self, unit: str) -> None:
        """Delete the Jobs an earlier attempt at this unit left: its measurements are not kept, so neither are their logs."""
        self.run(self._kc("delete", "jobs", "-l", f"{LBL_UNIT}={label_value(unit)}", "--ignore-not-found", "--wait=true"))

    def delete_storm_jobs(self) -> None:
        """Delete every storm Job there is (those of an earlier run, and the single `loadgen` Job older drivers made)."""
        self.run(self._kc("delete", "jobs", "-l", LOADGEN_SELECTOR, "--ignore-not-found", "--wait=true"))

    def cleanup_jobs(self, run_id: str) -> None:
        """Delete a finished run's Jobs. Best effort: the results are already on disk, so a failure here is only said."""
        try:
            self.run(self._kc("delete", "jobs", "-l", f"{LBL_RUN}={label_value(run_id)}", "--ignore-not-found", "--wait=false"))
        except subprocess.CalledProcessError as e:
            print(f"bench: could not delete the run's Jobs (exit {e.returncode}); `bench cluster down` or the next run removes them", file=sys.stderr)

    def _read(self, cmd: Sequence[str], what: str) -> str:
        """Run a read-only kubectl command and return its output, trying again when it fails. A run takes an hour or more, and the
        machine running it can lose its network or DNS for a moment (it did, once, between two job polls: `lookup ...eks.amazonaws.com:
        no such host`), which must not end the run with a finished storm's report still in the pod's log. Only reads are retried:
        they change nothing, so asking twice is harmless. After `read_retries` further failures the last error is raised."""
        for attempt in range(self.read_retries + 1):
            try:
                return self.run.output(cmd, quiet=True)
            except subprocess.CalledProcessError as e:
                if attempt == self.read_retries:
                    raise
                print(f"bench: {what} failed (exit {e.returncode}); trying again in {self.read_retry_wait_s:.0f} s "
                      f"({attempt + 1}/{self.read_retries})", file=sys.stderr, flush=True)
                self.sleep(self.read_retry_wait_s)
        raise AssertionError("unreachable")

    def run_storm(self, label: str, env: dict[str, str]) -> str:
        if self._job is None:
            raise RuntimeError("prepare() has not been called")
        name = job_name(label)
        # Only this storm's own Job is deleted, if an earlier attempt left one: the Jobs of the other storms stay, with their logs.
        self.run(self._kc("delete", "job", name, "--ignore-not-found", "--wait=true"))
        job = copy.deepcopy(self._job)
        job.setdefault("metadata", {})["name"] = name
        job["metadata"].setdefault("labels", {}).update({LBL_STORM: label_value(label), LBL_RUN: label_value(self._ctx["run_id"] or "none"),
                                                         LBL_UNIT: label_value(self._ctx["unit"] or "none"),
                                                         LBL_ATTEMPT: label_value(self._ctx["attempt"] or "none")})
        container = job["spec"]["template"]["spec"]["containers"][0]
        vars_ = {e["name"]: e for e in container.get("env", [])}
        for k, v in env.items():
            vars_[k] = {"name": k, "value": v}
        if self.cpuprobe:
            vars_["STORM_CPUPROBE"] = {"name": "STORM_CPUPROBE", "value": monitoring.PROBE_ADDR}
        container["env"] = list(vars_.values())
        if self._generator_resources:   # the variant's, as both requests and limits; what it leaves out stays as the overlay has it
            res = container.setdefault("resources", {})
            res.setdefault("requests", {}).update(self._generator_resources)
            res.setdefault("limits", {}).update(self._generator_resources)
        before = self.cpu_snapshot() if self.read_cpu else {}   # as late as it can be: just before the storm's Job exists
        polling = self._start_varz_poll()   # a time series through the whole storm, not two snapshots: held connections, not CPU, is the question
        self.run(self._k("create", "-f", "-"), input=json.dumps(job))
        try:
            log = self._finished_log(name, label)
        finally:
            self.last_varz = self._stop_varz_poll(polling)   # stopped even when the storm failed, so no poll thread outlives its storm
        if self.cpuprobe and '"runs"' in log and '"probeCpu"' not in log:
            # A generator from before the probe ignores STORM_CPUPROBE and says nothing about it. That is a fine generator to run (to compare
            # against an older one, say); what it must not do is leave the run with no broker or callout CPU while the run claims the probes
            # were used. So from the next storm the driver reads the cgroups itself, as it does without monitoring, and says so.
            self.cpuprobe, self.read_cpu, self.probe_fallback = False, True, True
            print(f"bench: the load generator's image did not report probe readings (it predates the CPU probe): from the next storm the broker's and "
                  "callout's CPU is read by the driver, once per storm, as without monitoring. Storm " + label + " has none.", file=sys.stderr, flush=True)
        after = self.cpu_snapshot() if self.read_cpu else {}    # and right after it has ended and its connections are closed
        diffs = cpu_mod.snapshot_diff(before, after)
        self.last_cpu = {"source": "cgroup v2 cpu.stat and cpu.max, read with kubectl exec just before the Job was created and just after it finished",
                         "components": diffs} if diffs else None
        return log

    def _finished_log(self, name: str, label: str) -> str:
        """Wait for the Job to end and return its log. The pod and its log stay on the cluster until the Job is deleted, so this can
        be done at any later time, by any driver."""
        deadline = self.clock() + self.job_timeout_s
        while True:
            st = json.loads(self._read(self._kc("get", "job", name, "-o", "json"), "reading the Job's status")).get("status", {})
            if st.get("succeeded", 0) >= 1 or st.get("failed", 0) >= 1:
                break
            if self.clock() > deadline:
                raise StormFailed(f"{label}: the Job did not finish within {self.job_timeout_s:.0f} s")
            self.sleep(self.poll_s)
        log = self._read(self._kc("logs", f"job/{name}"), "reading the Job's log")
        if st.get("failed", 0) >= 1:
            raise StormFailed(f"{label}: the Job failed; last log lines:\n" + "\n".join(log.splitlines()[-5:]))
        return log

    def collect_storm(self, label: str, attempt: str) -> str | None:
        """The log of a storm's Job that an earlier driver started, waiting for it if it is still running; None when there is no such
        Job, or the Job belongs to another attempt at the unit (an attempt's measurements are only ever mixed with its own)."""
        name = job_name(label)
        self.last_cpu = None                               # a storm read afterwards has no readings taken around it
        self.last_varz = None                              # nor a /varz series: there was no live poll to take
        out = self._read(self._kc("get", "job", name, "-o", "json", "--ignore-not-found"), "looking for the storm's Job")
        if not out.strip():
            return None
        if json.loads(out)["metadata"].get("labels", {}).get(LBL_ATTEMPT) != label_value(attempt):
            return None
        return self._finished_log(name, label)

    # -- facts for environment.json -------------------------------------------------------------------------
    def describe(self) -> dict:
        nodes = json.loads(self.run.output(self._k("get", "nodes", "-o", "json"), quiet=True))["items"]
        return {"nodes": [{"os_image": n["status"]["nodeInfo"]["osImage"], "kernel": n["status"]["nodeInfo"]["kernelVersion"],
                           "kubelet": n["status"]["nodeInfo"]["kubeletVersion"],
                           "container_runtime": n["status"]["nodeInfo"]["containerRuntimeVersion"],
                           "cpu": n["status"]["capacity"]["cpu"], "memory": n["status"]["capacity"]["memory"]} for n in nodes]}
