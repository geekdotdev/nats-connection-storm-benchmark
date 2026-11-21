# SPDX-License-Identifier: Apache-2.0
"""The CPU probe and Prometheus: deploying them, finding them, exporting a run's series, and the manifests' properties."""
import datetime as dt
import json
import re
import shutil
import subprocess
import urllib.parse
from pathlib import Path

import pytest

from stormbench import ab, cli, kube, monitoring, runcmd, state, variants
from stormbench.config import REPO_ROOT
from stormbench.runner import Runner
from test_jobs import Scripted, kubeconfig  # noqa: F401
from test_state import FakeCluster, env_of, go, parse, run_args


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)
    monkeypatch.setattr(monitoring.time, "sleep", lambda s: None)


# -- the words and the windows ------------------------------------------------------------------------------------------------------------------
def test_the_probe_is_reached_by_the_headless_services_name():
    assert monitoring.PROBE_ADDR == "cpuprobe.storm-bench.svc.cluster.local:9100"
    assert monitoring.overlay_for("local") == "deploy/monitoring/overlays/local"
    with pytest.raises(ValueError):
        monitoring.overlay_for("mars")


@pytest.mark.parametrize("start,end", [(0, 100), (0, 9000), (0, 9001), (1000, 40000), (5, 5.5)])
def test_windows_cover_the_range_without_overlap_and_within_the_points_limit(start, end):
    ws = monitoring.windows(start, end)
    assert ws[0][0] == start and ws[-1][1] == end
    assert all(hi - lo <= monitoring.STEP_S * monitoring.MAX_POINTS and hi >= lo for lo, hi in ws)
    assert all(b[0] > a[1] for a, b in zip(ws, ws[1:]))                                  # no sample is fetched twice
    assert all((hi - lo) / monitoring.STEP_S + 1 <= 11000 for lo, hi in ws)              # Prometheus's own limit on points per series


def test_an_empty_or_backwards_range_has_no_windows():
    assert monitoring.windows(10, 10) == [] and monitoring.windows(10, 5) == []


def test_the_range_query_goes_through_the_api_servers_proxy_url_encoded():
    path = monitoring.range_path('rate(x{a="b"}[1s])', 1795300000.5, 1795300100, 1)
    assert path.startswith("/api/v1/namespaces/storm-bench/services/prometheus:9090/proxy/api/v1/query_range?")
    qs = urllib.parse.parse_qs(path.split("?")[1])
    assert qs["query"] == ['rate(x{a="b"}[1s])'] and qs["start"] == ["1795300000.500"] and qs["step"] == ["1s"]


# -- deploying ---------------------------------------------------------------------------------------------------------------------------------
class Recorder(Runner):
    def __init__(self, outputs=(), ok=True):
        super().__init__()
        self.cmds, self.outputs, self.ok = [], list(outputs), ok

    def __call__(self, cmd, *, input=None, env=None):
        self.cmds.append(cmd)

    def output(self, cmd, *, input=None, env=None, dry_run_value="", quiet=False):
        self.cmds.append(cmd)
        out = self.outputs.pop(0)
        if isinstance(out, Exception):
            raise out
        return out

    def succeeds(self, cmd):
        self.cmds.append(cmd)
        return self.ok


def verbs(run):
    return [next(a for a in c if a in ("apply", "delete", "rollout", "get")) for c in run.cmds]


def test_up_applies_the_overlay_then_waits_for_the_probes_and_prometheus(kubeconfig):
    run = Recorder()
    monitoring.up(run, "local")
    assert verbs(run) == ["apply", "rollout", "rollout"]
    assert run.cmds[0][-2:] == ["-k", "deploy/monitoring/overlays/local"]
    assert [c[-2] for c in run.cmds[1:]] == ["daemonset/cpuprobe", "statefulset/prometheus"]


def test_down_keeps_the_volume_unless_asked_and_purge_deletes_the_claim_so_ebs_is_released(kubeconfig):
    run = Recorder()
    monitoring.down(run, "local")
    assert verbs(run) == ["delete"] and "-k" in run.cmds[0]
    run = Recorder()
    monitoring.down(run, "local", purge=True)
    assert verbs(run) == ["delete", "delete"] and run.cmds[1][-5:-3] == ["pvc", "data-prometheus-0"] and "--wait=true" in run.cmds[1]


def test_installed_asks_for_both_and_no_cluster_means_not_installed(kubeconfig):
    run = Recorder(ok=True)
    assert monitoring.installed(run, "local") is True and run.cmds[0][-2:] == ["daemonset/cpuprobe", "statefulset/prometheus"]
    assert monitoring.installed(Recorder(ok=False), "local") is False


def test_installed_without_a_kubeconfig_is_false_not_an_error():
    assert monitoring.installed(Runner(), "local") is False             # conftest points the kubeconfig nowhere


# -- exporting ---------------------------------------------------------------------------------------------------------------------------------
def matrix(*series):
    return json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [{"metric": m, "values": v} for m, v in series]}})


B0 = {"__name__": "storm_cpu_usage_seconds_total", "component": "broker", "pod": "broker-0", "container": "broker", "node": "n1"}
C0 = {"__name__": "storm_cpu_usage_seconds_total", "component": "callout", "pod": "callout-x", "container": "callout", "node": "n2"}


def test_a_run_is_exported_per_metric_merged_across_windows_and_indexed(kubeconfig, tmp_path):
    long_end = 1000 + 9000 + 5000                                         # two windows
    first = matrix((B0, [[1000, "1"], [1001, "2"]]), (C0, [[1000, "10"]]))
    second = matrix((B0, [[10001, "3"]]), (C0, [[10001, "11"]]))
    outs = []
    for _ in monitoring.METRICS:
        outs += [first, second]
    run = Recorder(outs)
    index = monitoring.export(run, "local", tmp_path, 1000, long_end, say=lambda m: None)
    usage = json.loads((tmp_path / "prometheus" / "storm_cpu_usage_seconds_total.json").read_text())
    assert [s["metric"]["component"] for s in usage["result"]] == ["broker", "callout"]
    assert usage["result"][0]["values"] == [[1000, "1"], [1001, "2"], [10001, "3"]]         # the two windows joined, in order
    assert set(p.name for p in (tmp_path / "prometheus").iterdir()) == {f"{m}.json" for m in monitoring.METRICS} | {"index.json"}
    assert index["metrics"]["storm_cpu_usage_seconds_total"] == {"series": 2, "samples": 5}
    assert json.loads((tmp_path / "prometheus" / "index.json").read_text())["step_s"] == 1
    assert len([c for c in run.cmds if "--raw" in c]) == len(monitoring.METRICS) * 2         # one query per metric per window


def test_a_prometheus_that_cannot_be_read_is_an_error_after_a_few_tries(kubeconfig, tmp_path):
    boom = subprocess.CalledProcessError(1, ["kubectl"])
    run = Recorder([boom, boom, boom])
    with pytest.raises(RuntimeError, match="could not read"):
        monitoring.export(run, "local", tmp_path, 1000, 1100, say=lambda m: None)
    assert len(run.cmds) == 3
    err = Recorder([json.dumps({"status": "error", "error": "bad query"})] * 3)
    with pytest.raises(RuntimeError, match="bad query"):
        monitoring.export(err, "local", tmp_path, 1000, 1100, say=lambda m: None)


def test_a_try_that_works_after_a_failure_is_enough(kubeconfig, tmp_path):
    boom = subprocess.CalledProcessError(1, ["kubectl"])
    outs = []
    for _ in monitoring.METRICS:
        outs += [boom, matrix((B0, [[1000, "1"]]))]
    index = monitoring.export(Recorder(outs), "local", tmp_path, 1000, 1100, say=lambda m: None)
    assert index["metrics"]["storm_cpu_usage_seconds_total"]["samples"] == 1


# -- the run uses them when they are there ----------------------------------------------------------------------------------------------------
def with_monitoring(monkeypatch, there=True):
    calls = {"installed": 0, "exported": []}
    monkeypatch.setattr(monitoring, "installed", lambda run, target: calls.__setitem__("installed", calls["installed"] + 1) or there)
    monkeypatch.setattr(monitoring, "export", lambda run, target, out, start, end, say=print: calls["exported"].append((target, out, start, end)) or {})
    return calls


def test_a_run_with_monitoring_installed_has_the_generator_ask_the_probes_and_exports_at_the_end(tmp_path, monkeypatch, capsys):
    calls = with_monitoring(monkeypatch)
    cluster = FakeCluster()
    assert go(run_args(tmp_path), state.MemoryStore(), cluster) == 0
    assert cluster.kwargs[-1]["cpuprobe"] is True and cluster.kwargs[-1]["read_cpu"] is False        # the probes replace the driver's own readings
    assert "monitoring: the CPU probes and Prometheus are installed" in capsys.readouterr().out
    env = env_of(tmp_path / "out")
    assert env["cpu_source"] == "probe" and env["monitoring"] == {"probe": True, "prometheus": True, "scrape_interval_s": 1}
    (target, out, start, end), = calls["exported"]
    assert target == "local" and out == tmp_path / "out"
    assert start == pytest.approx(dt.datetime.fromisoformat(env["date"]).timestamp() - 15) and end > start


def test_a_run_without_monitoring_reads_cpu_the_old_way_and_says_how_to_add_it(tmp_path, monkeypatch, capsys):
    calls = with_monitoring(monkeypatch, there=False)
    cluster = FakeCluster()
    assert go(run_args(tmp_path), state.MemoryStore(), cluster) == 0
    assert cluster.kwargs[-1]["cpuprobe"] is False and cluster.kwargs[-1]["read_cpu"] is True
    assert "monitoring: not installed (`bench --target local monitoring up` adds it)" in capsys.readouterr().out
    assert env_of(tmp_path / "out")["cpu_source"] == "exec" and "monitoring" not in env_of(tmp_path / "out") and calls["exported"] == []


def test_no_monitoring_ignores_an_installed_one_and_no_cpu_turns_the_exec_readings_off_too(tmp_path, monkeypatch):
    calls = with_monitoring(monkeypatch)
    cluster = FakeCluster()
    assert go(run_args(tmp_path, "--no-monitoring", "--no-cpu"), state.MemoryStore(), cluster) == 0
    assert cluster.kwargs[-1]["cpuprobe"] is False and cluster.kwargs[-1]["read_cpu"] is False
    assert calls["installed"] == 0 and calls["exported"] == [] and env_of(tmp_path / "out")["cpu_source"] == "none"


def test_a_resumed_run_finds_monitoring_too_and_exports_when_it_completes(tmp_path, monkeypatch):
    calls = with_monitoring(monkeypatch)
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    assert calls["exported"] == []                                                                   # a failed run exports nothing yet
    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again) == 0
    assert again.kwargs[-1]["cpuprobe"] is True and len(calls["exported"]) == 1


def test_an_export_that_fails_is_said_and_does_not_fail_the_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(monitoring, "installed", lambda run, target: True)
    def broken(*a, **k):
        raise RuntimeError("prometheus is down")
    monkeypatch.setattr(monitoring, "export", broken)
    assert go(run_args(tmp_path), state.MemoryStore(), FakeCluster()) == 0
    err = capsys.readouterr().err
    assert "could not export Prometheus's series (prometheus is down)" in err and "monitoring export" in err


def test_the_storm_job_is_told_where_the_probes_are_only_when_there_are_probes(kubeconfig):
    def env_of_job(cpuprobe):
        run = Scripted([json.dumps({"status": {"succeeded": 1}}), "LOG"])
        b = kube.KubeBackend(run, sleep=lambda s: None, read_cpu=False, cpuprobe=cpuprobe)
        b._job = {"metadata": {"name": "loadgen"}, "spec": {"template": {"spec": {"containers": [{"name": "loadgen", "env": [{"name": "A", "value": "1"}]}]}}}}
        b.run_storm("x-b1-n800", {})
        made = json.loads(run.stdin[run.cmds.index(next(c for c in run.cmds if "create" in c))])
        return {e["name"]: e["value"] for e in made["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env_of_job(True)["STORM_CPUPROBE"] == monitoring.PROBE_ADDR
    assert "STORM_CPUPROBE" not in env_of_job(False)


def test_a_generator_that_predates_the_probe_is_run_with_the_drivers_own_cpu_readings_and_says_so(kubeconfig, capsys):
    """Found on EKS: an old loadgen digest ignores STORM_CPUPROBE, so a run with monitoring on had no broker or callout CPU at all and said
    it had used the probes. An older generator is a legitimate thing to run: the driver reads the cgroups itself instead, and says so."""
    old = json.dumps({"runs": [{"cgroupCpu": {}}]})
    run = Scripted([json.dumps({"status": {"succeeded": 1}}), old, json.dumps({"status": {"succeeded": 1}}), old])
    b = kube.KubeBackend(run, sleep=lambda s: None, read_cpu=False, cpuprobe=True)
    b._job = {"metadata": {"name": "loadgen"}, "spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    assert b.run_storm("x-b1-warm", {}) == old
    assert b.probe_fallback and not b.cpuprobe and b.read_cpu
    assert "did not report probe readings" in capsys.readouterr().err
    made = json.loads(run.stdin[run.cmds.index(next(c for c in run.cmds if "create" in c))])
    assert "STORM_CPUPROBE" in {e["name"] for e in made["spec"]["template"]["spec"]["containers"][0]["env"]}   # the first storm was asked, in case
    # a new generator that could not read the probes still reports the key (null), and is not mistaken for an old one
    ok = kube.KubeBackend(Scripted([json.dumps({"status": {"succeeded": 1}}), json.dumps({"runs": [{"probeCpu": None}]})]), sleep=lambda s: None, read_cpu=False, cpuprobe=True)
    ok._job = b._job
    ok.run_storm("x-b1-warm", {})
    assert ok.cpuprobe and not ok.probe_fallback


def test_the_environment_says_when_the_cpu_was_read_by_the_driver_after_all(tmp_path):
    (tmp_path / "environment.json").write_text(json.dumps({"cpu_source": "probe", "monitoring": {"probe": True}}))
    runcmd.note_probe_fallback(tmp_path)
    env = json.loads((tmp_path / "environment.json").read_text())
    assert env["cpu_source"].startswith("exec (the load generator's image predates the CPU probe")


def test_a_variant_cannot_point_the_generator_elsewhere():
    with pytest.raises(ValueError, match="STORM_CPUPROBE"):
        variants.parse({"variant": [{"label": "a", "image": "x", "storm": {"STORM_CPUPROBE": "evil:1"}}, {"label": "b", "image": "y"}]}, "localhost/", REPO_ROOT)


# -- the commands ---------------------------------------------------------------------------------------------------------------------------
def test_the_monitoring_command_parses_and_export_needs_a_directory():
    a = cli.build_parser().parse_args(["--target", "eks", "monitoring", "down", "--purge"])
    assert (a.cmd, a.action, a.purge, a.target) == ("monitoring", "down", True, "eks")
    with pytest.raises(ValueError, match="needs the results directory"):
        runcmd.execute_monitoring(Runner(), cli.build_parser().parse_args(["monitoring", "export"]))


def test_exporting_a_finished_run_takes_its_own_window(tmp_path, monkeypatch):
    out = tmp_path / "run"
    (out / "raw").mkdir(parents=True)
    (out / "environment.json").write_text(json.dumps({"date": "2026-11-21T11:44:36-08:00", "target": "local", "variants": [{"label": "a", "commit": "", "image": "x"}, {"label": "b", "commit": "", "image": "y"}], "design": {"phases": ["n800"]}}))
    (out / "raw" / "DONE").write_text("2026-11-21T12:44:36-0800\n")
    seen = []
    monkeypatch.setattr(monitoring, "export", lambda run, target, o, start, end, say=print: seen.append((start, end)) or {})
    assert runcmd.execute_monitoring(Runner(), cli.build_parser().parse_args(["monitoring", "export", str(out)])) == 0
    start = dt.datetime.fromisoformat("2026-11-21T11:44:36-08:00").timestamp()
    assert seen == [(start - 15, start + 3600 + 5)]                                  # from the run's start to when it was done


# -- the manifests ---------------------------------------------------------------------------------------------------------------------------
BASE = (REPO_ROOT / "deploy" / "monitoring" / "base")


def docs_of(text):
    return [d for d in text.split("\n---\n") if d.strip()]


@pytest.mark.skipif(not shutil.which("kubectl"), reason="needs kubectl kustomize")
def render(overlay):
    return subprocess.run(["kubectl", "kustomize", str(REPO_ROOT / "deploy" / "monitoring" / "overlays" / overlay)], capture_output=True, text=True, check=True).stdout


def test_the_probe_reads_the_nodes_cgroups_read_only_with_no_privilege_and_no_cpu_limit():
    text = (BASE / "cpuprobe.yaml").read_text()
    ds = [d for d in docs_of(text) if "kind: DaemonSet" in d][0]
    assert "hostPath: {path: /sys/fs/cgroup, type: Directory}" in ds and "readOnly: true" in ds
    assert "privileged" not in ds and "allowPrivilegeEscalation: false" in ds and "readOnlyRootFilesystem: true" in ds and 'drop: ["ALL"]' in ds
    assert "runAsNonRoot: true" in ds and "hostPID" not in ds and "hostNetwork" not in ds
    assert "operator: Exists" in ds                                       # on the tainted broker nodes too
    assert not re.search(r"limits:\s*\{[^}]*cpu", ds)                      # a CPU limit could throttle the thing that measures throttling


def test_the_probes_service_account_can_only_get_and_list_pods_in_the_namespace():
    text = (BASE / "cpuprobe.yaml").read_text()
    role = [d for d in docs_of(text) if "kind: Role\n" in d][0]
    assert "resources: [pods]" in role and "verbs: [get, list]" in role and role.count("- apiGroups") == 1
    assert "ClusterRole" not in text                                      # namespaced, not cluster-wide


def test_the_monitoring_pods_do_not_carry_the_label_the_probe_reads_so_it_never_measures_itself():
    for f in ("cpuprobe.yaml", "prometheus.yaml"):
        text = (BASE / f).read_text()
        assert "part-of: storm-bench-monitoring" in text and not re.search(r"part-of: storm-bench\b(?!-)", text)
    assert '"app.kubernetes.io/part-of=storm-bench"' in (REPO_ROOT / "components" / "cpuprobe" / "main.go").read_text()


def test_prometheus_is_pinned_scrapes_every_second_and_keeps_its_data_on_a_claim():
    text = (BASE / "prometheus.yaml").read_text()
    assert re.search(r"image: quay\.io/prometheus/prometheus@sha256:[0-9a-f]{64}", text)
    assert "scrape_interval: 1s" in text and "scrape_timeout: 1s" in text and "cpuprobe.storm-bench.svc.cluster.local" in text
    assert "volumeClaimTemplates" in text and "accessModes: [ReadWriteOnce]" in text and "--storage.tsdb.path=/prometheus" in text
    assert "emptyDir" not in text and "nfs" not in text.lower().replace("not be on nfs", "")
    assert "runAsNonRoot: true" in text and "readOnlyRootFilesystem: true" in text and "fsGroup: 65534" in text


@pytest.mark.skipif(not shutil.which("kubectl"), reason="needs kubectl kustomize")
def test_the_kind_overlay_maps_the_local_probe_image_and_uses_the_local_storage_class():
    out = render("local")
    assert "image: localhost/cpuprobe:dev" in out and "storageClassName: standard" in out
    assert out.count("namespace: storm-bench") >= 8 and "kind: StatefulSet" in out
