# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
from pathlib import Path

import pytest

from stormbench import ab, config, kube, report, runcmd
from stormbench.runner import Runner

A = ab.Variant("main", "a" * 40, "localhost/nats-server:aaaaaaaaaaaa")
B = ab.Variant("fixed", "b" * 40, "localhost/nats-server:bbbbbbbbbbbb")


def report_line(p50: float, connections: int = 10, runs: int = 3) -> str:
    one = {"connections": connections, "seconds": 0.2, "outcomes": {"admitted": connections, "refused": 0, "timeout": 0, "error": 0},
           "authMs": {"n": connections, "p50": p50, "p90": p50, "p99": p50, "max": p50}}
    return "console report\n" + ab.REPORT_PREFIX + json.dumps({"run": "x", "runs": [one] * runs}) + "\n"


class Fake:
    """A backend that records what it was asked and answers with a report whose p50 depends on the arm."""
    def __init__(self, p50=None, fail_on=None):
        self.calls, self.p50, self.fail_on = [], p50 or {"main": 40.0, "fixed": 25.0}, fail_on

    def start_variant(self, arm):
        self.calls.append(("start", arm.label))
        return {"server_version": "v-test"}

    def run_storm(self, label, env):
        self.calls.append(("storm", label, env))
        if label == self.fail_on:
            raise ab.StormFailed(f"{label}: boom")
        return report_line(self.p50[label.split("-")[0]], int(env["STORM_CONNECTIONS"]))


def test_plan_validates():
    with pytest.raises(ValueError):
        ab.Plan(variants=(A, A))
    with pytest.raises(ValueError):
        ab.Plan(variants=(A, ab.Variant("bad label!", "b" * 40, "x")))
    with pytest.raises(ValueError):
        ab.Plan(variants=(A, B), blocks=0)


def test_the_order_of_the_arms_alternates_by_block():
    steps = list(ab.schedule(ab.Plan(variants=(A, B), blocks=3, sizes=(800,))))
    order = []
    for s in steps:
        if (s.block, s.variant.label) not in order:
            order.append((s.block, s.variant.label))
    assert order == [(1, "main"), (1, "fixed"), (2, "fixed"), (2, "main"), (3, "main"), (3, "fixed")]
    assert [s.phase.name for s in steps[:4]] == ["warm", "idle", "n800", "warm"]
    assert steps[0].label == "main-b1-warm"


def test_each_arm_gets_a_fresh_broker_once_per_block_before_its_storms(tmp_path):
    fake, plan = Fake(), ab.Plan(variants=(A, B), blocks=2, sizes=(800,))
    ab.run_blocks(plan, fake, tmp_path / "raw", sleep=lambda s: None, log=lambda m: None)
    kinds = [c[0] for c in fake.calls]
    assert kinds.count("start") == 4 and kinds.count("storm") == 12
    assert fake.calls[0] == ("start", "main") and fake.calls[1][0] == "storm"
    assert [c[1] for c in fake.calls if c[0] == "start"] == ["main", "fixed", "fixed", "main"]


def test_raw_files_are_named_and_complete(tmp_path):
    raw = tmp_path / "raw"
    ab.run_blocks(ab.Plan(variants=(A, B), blocks=1, sizes=(800,)), Fake(), raw, sleep=lambda s: None,
                  now=lambda: "T", log=lambda m: None)
    names = sorted(p.name for p in raw.iterdir())
    assert "DONE" in names and "main-b1.meta.json" in names and "fixed-b1.meta.json" in names
    for phase in ("warm", "idle", "n800"):
        assert f"main-b1-{phase}.json" in names and f"main-b1-{phase}.txt" in names
    meta = json.loads((raw / "main-b1.meta.json").read_text())
    assert meta["commit"] == "a" * 40 and meta["image"] == A.image and meta["server_version"] == "v-test" and meta["block"] == 1
    assert ab.runs_of(json.loads((raw / "main-b1-n800.json").read_text()))[0]["connections"] == 800


def test_the_storm_environment_carries_the_phase(tmp_path):
    fake = Fake()
    ab.run_blocks(ab.Plan(variants=(A, B), blocks=1, sizes=(800,), threads=2), fake, tmp_path / "raw", sleep=lambda s: None, log=lambda m: None)
    env = next(c[2] for c in fake.calls if c[0] == "storm" and c[1] == "main-b1-idle")
    assert env["STORM_CONNECTIONS"] == "1" and env["STORM_RUNS"] == "40" and env["STORM_THREADS"] == "2"
    assert env["STORM_LABEL"] == "main-b1-idle" and env["STORM_JSON"] == "-"


def test_a_failed_storm_stops_the_run_and_keeps_what_was_collected(tmp_path):
    raw = tmp_path / "raw"
    with pytest.raises(ab.StormFailed):
        ab.run_blocks(ab.Plan(variants=(A, B), blocks=1, sizes=(800,)), Fake(fail_on="main-b1-idle"), raw, sleep=lambda s: None, log=lambda m: None)
    assert (raw / "main-b1-warm.json").exists() and not (raw / "main-b1-idle.json").exists() and not (raw / "DONE").exists()


def test_parse_report_takes_the_last_report_line_and_complains_when_there_is_none():
    assert ab.parse_report("a\n" + ab.REPORT_PREFIX + '{"x": 1}\n' + ab.REPORT_PREFIX + '{"x": 2}\n') == {"x": 2}
    with pytest.raises(ab.StormFailed):
        ab.parse_report("the generator crashed\n")
    assert ab.runs_of({"connections": 5}) == [{"connections": 5}]


# -- the Kubernetes backend, with a runner that answers from a script --------------------------------------------------
class Scripted(Runner):
    def __init__(self, outputs):
        super().__init__()
        self.cmds, self.stdin, self.outputs = [], [], list(outputs)

    def __call__(self, cmd, *, input=None, env=None):
        self.cmds.append(cmd)
        self.stdin.append(input)

    def output(self, cmd, *, input=None, env=None, dry_run_value="", quiet=False):
        self.cmds.append(cmd)
        self.stdin.append(input)
        out = self.outputs.pop(0)
        if isinstance(out, Exception):
            raise out
        return out


@pytest.fixture
def kubeconfig(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(kc))
    return str(kc)


def backend(run, **kw):
    b = kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None, **kw)
    b._job = {"spec": {"template": {"spec": {"containers": [{"name": "loadgen", "env": [{"name": "STORM_CONNECTIONS", "value": "800"}, {"name": "STORM_URL", "value": "wss://broker:8443"}]}]}}}}
    return b


def test_prepare_points_the_broker_at_a_real_image_before_waiting_for_it(kubeconfig):
    run = Scripted(["kind: Job", json.dumps({"kind": "Job", "spec": {}})])
    with kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None) as b:
        b.prepare((A, B))
        verbs = [next(a for a in c if a in ("apply", "set", "rollout", "kustomize", "create")) for c in run.cmds]
        assert verbs[:3] == ["apply", "rollout", "rollout"]  # one apply puts the first variant's image in; no placeholder image
        assert (Path(run.cmds[0][-1]) / "kustomization.yaml").is_file()  # applied from a generated kustomization
        assert A.image in (Path(run.cmds[0][-1]) / "kustomization.yaml").read_text()
        assert run.stdin[-1] == "kind: Job"  # the rendered Job is converted to JSON through kubectl
        tmp = run.cmds[0][-1]
    assert not Path(tmp).exists()  # the generated kustomizations are removed when the backend closes


def test_run_storm_replaces_the_job_merges_env_and_returns_the_log(kubeconfig):
    run = Scripted([json.dumps({"status": {}}), json.dumps({"status": {"succeeded": 1}}), "THE LOG"])
    log = backend(run).run_storm("main-b1-n800", {"STORM_CONNECTIONS": "1600", "STORM_LABEL": "main-b1-n800"})
    assert log == "THE LOG"
    assert run.cmds[0][:6] == ["kubectl", "--kubeconfig", kubeconfig, "-n", "storm-bench", "delete"]
    created = json.loads(run.stdin[1])["spec"]["template"]["spec"]["containers"][0]["env"]
    env = {e["name"]: e["value"] for e in created}
    assert env == {"STORM_CONNECTIONS": "1600", "STORM_URL": "wss://broker:8443", "STORM_LABEL": "main-b1-n800"}
    assert all(c[:3] == ["kubectl", "--kubeconfig", kubeconfig] for c in run.cmds)


def test_run_storm_reports_a_failed_job_with_the_end_of_its_log(kubeconfig):
    run = Scripted([json.dumps({"status": {"failed": 1}}), "l1\nl2\nboom"])
    with pytest.raises(ab.StormFailed) as e:
        backend(run).run_storm("x", {})
    assert "boom" in str(e.value)


NO_DNS = subprocess.CalledProcessError(1, ["kubectl"], stderr="dial tcp: lookup x.eks.amazonaws.com: no such host")


def test_run_storm_survives_a_failed_poll_and_a_failed_log_read(kubeconfig, capsys):
    run = Scripted([json.dumps({"status": {}}), NO_DNS, NO_DNS, json.dumps({"status": {"succeeded": 1}}), NO_DNS, "THE LOG"])
    sleeps = []
    b = kube.KubeBackend(run, read_cpu=False, sleep=sleeps.append, read_retry_wait_s=7.0)
    b._job = {"spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    assert b.run_storm("x", {}) == "THE LOG"
    assert sleeps.count(7.0) == 3                              # one wait per failed read
    assert "reading the Job's status failed" in capsys.readouterr().err
    assert sum(1 for c in run.cmds if "logs" in c) == 2        # the log was asked for twice


def test_run_storm_stops_retrying_a_read_that_keeps_failing(kubeconfig):
    run = Scripted([json.dumps({"status": {}})] + [NO_DNS] * 5)
    b = kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None, read_retries=3)
    b._job = {"spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    with pytest.raises(subprocess.CalledProcessError):
        b.run_storm("x", {})
    assert sum(1 for c in run.cmds if "get" in c and "job" in c) == 1 + 4   # the first poll, then the failed one and its 3 retries


def test_run_storm_gives_up_when_the_job_never_finishes(kubeconfig):
    ticks = iter(range(0, 10_000, 100))
    run = Scripted([json.dumps({"status": {}})] * 50)
    b = kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None, clock=lambda: next(ticks), job_timeout_s=250)
    b._job = {"spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    with pytest.raises(ab.StormFailed):
        b.run_storm("x", {})


def test_start_variant_applies_that_variants_kustomization_and_waits(kubeconfig):
    pods = json.dumps({"items": [{"metadata": {}, "status": {"containerStatuses": [{"image": "i", "imageID": "sha256:abc"}]}}]})
    run = Scripted([pods, "nats-server: v2.99"])
    with backend(run) as b:
        facts = b.start_variant(B)
        apply = next(c for c in run.cmds if "apply" in c)
        text = (Path(apply[-1]) / "kustomization.yaml").read_text()
        assert B.image in text and "storm-bench/restarted-at" in text  # the new image and the restart stamp go in one apply
        assert [c[-2] for c in run.cmds if "rollout" in c] == ["statefulset/broker", "deployment/callout"]
        assert not any("patch" in c or "set" in c for c in run.cmds)  # nothing is changed behind apply's back
    assert facts == {"image_id": "sha256:abc", "pod_image": "i", "server_version": "nats-server: v2.99", "pod_uid": ""}   # (the fixture's pod has no uid)


# -- the report --------------------------------------------------------------------------------------------------------
def write_run(tmp_path, slow, fast, blocks=8):
    raw = tmp_path / "raw"
    raw.mkdir()
    (tmp_path / "environment.json").write_text(json.dumps({"arms": [{"label": "main", "commit": "a" * 40, "image": "x"}, {"label": "fixed", "commit": "b" * 40, "image": "y"}],
                                                         "design": {"phases": ["warm", "idle", "n800"]}}))
    for blk in range(1, blocks + 1):
        for label, base in (("main", slow), ("fixed", fast)):
            (raw / f"{label}-b{blk}.meta.json").write_text("{}")
            (raw / f"{label}-b{blk}-n800.json").write_text(json.dumps({"runs": json.loads(report_line(base + blk * 0.1, 800).split(ab.REPORT_PREFIX)[1])["runs"]}))
    return tmp_path


def test_the_report_finds_a_clear_difference(tmp_path):
    text = report.summarize(write_run(tmp_path, 40.0, 25.0))
    assert "n800" in text and "change fixed vs main" in text
    assert "-3" in text.split("change fixed vs main")[1].split("%")[0] or "-4" in text.split("change fixed vs main")[1].split("%")[0]
    assert "p=0.00016" in text  # every fixed block faster than every main block: the smallest an exact 8 v 8 test can give


def test_the_report_finds_no_difference_between_identical_arms(tmp_path):
    text = report.summarize(write_run(tmp_path, 30.0, 30.0))
    assert "+0.0%" in text and "p=1.00000" in text


def test_perm_p_is_exact_for_tiny_samples():
    assert report.perm_p([1.0, 2.0], [3.0, 4.0]) == pytest.approx(2 / 6)
    assert report.perm_p([1.0], [1.0]) == 1.0


# -- the run command ---------------------------------------------------------------------------------------------------
def test_arm_image_validates_the_commit():
    assert runcmd.arm_image("c" * 40, "localhost/") == "localhost/nats-server:cccccccccccc"
    with pytest.raises(ValueError):
        runcmd.arm_image("main", "localhost/")


def test_environment_has_no_host_identity():
    env = runcmd.environment(ab.Plan(variants=(A, B), blocks=2), {"nodes": []}, 8.0)
    text = json.dumps(env)
    import getpass, socket
    assert getpass.getuser() not in text and socket.gethostname() not in text
    assert env["design"]["per_phase"]["idle"] == {"connections": 1, "runs": 40, "pause_ms": 100, "hold_ms": 100}


def test_the_eks_target_uses_the_eks_kubeconfig_and_overlays(monkeypatch, tmp_path):
    local, remote = tmp_path / "local", tmp_path / "eks"
    local.write_text("x")
    remote.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(local))
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(remote))
    run = Scripted(["kind: Job", json.dumps({"kind": "Job", "spec": {}})])
    b = kube.KubeBackend(run, target="eks", sleep=lambda s: None)
    assert (b.stack, b.loadgen) == ("deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks")
    b.prepare((A, B))
    assert all(c[:3] == ["kubectl", "--kubeconfig", str(remote)] for c in run.cmds)
    assert run.cmds[0][-2] == "-k" and run.cmds[0][-1].endswith("/main")
    cluster = kube.KubeBackend(run, target="eks", stack="deploy/stack/overlays/eks-cluster")
    assert cluster.stack.endswith("eks-cluster") and cluster.loadgen.endswith("loadgen/overlays/eks")


def test_the_image_prefix_follows_the_target(monkeypatch):
    monkeypatch.setenv("REGISTRY", "ghcr.io/someone/")
    assert runcmd.image_prefix_for("local", None) == "localhost/"
    assert runcmd.image_prefix_for("eks", None) == "ghcr.io/someone/"
    assert runcmd.image_prefix_for("eks", "other.example/x/") == "other.example/x/"
    assert runcmd.arm_image("c" * 40, runcmd.image_prefix_for("eks", None)) == "ghcr.io/someone/nats-server:cccccccccccc"
    monkeypatch.delenv("REGISTRY")
    with pytest.raises(config.MissingSecret):
        runcmd.image_prefix_for("eks", None)
    assert runcmd.environment(ab.Plan(variants=(A, B), blocks=2), {}, 8.0, "eks")["target"] == "eks"
