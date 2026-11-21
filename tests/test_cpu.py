# SPDX-License-Identifier: Apache-2.0
"""CPU use and throttling per storm: reading the cgroup counters, differencing them, storing them and reporting them."""
import json
import subprocess

import pytest

from stormbench import ab, cli, cpu, kube, report
from test_jobs import Scripted, kubeconfig  # noqa: F401  (the fake kubectl and its kubeconfig fixture)

CPU_STAT = "usage_usec {u}\nuser_usec {us}\nsystem_usec {sy}\ncore_sched.force_idle_usec 0\nnr_periods {p}\nnr_throttled {t}\nthrottled_usec {tu}\nnr_bursts 0\nburst_usec 0\n"


def reading(usage=1_000_000, periods=0, throttled=0, throttled_us=0, limit="max 100000"):
    return CPU_STAT.format(u=usage, us=usage * 6 // 10, sy=usage * 4 // 10, p=periods, t=throttled, tu=throttled_us) + limit + "\n"


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)


# -- reading and differencing ---------------------------------------------------------------------------------------------------------------
def test_the_counters_and_the_limit_are_read_from_cpu_stat_and_cpu_max():
    r = cpu.parse(reading(usage=1812754, periods=40, throttled=7, throttled_us=350000, limit="300000 100000"))
    assert r == {"usage_usec": 1812754, "user_usec": 1087652, "system_usec": 725101, "nr_periods": 40, "nr_throttled": 7,
                 "throttled_usec": 350000, "limit_cores": 3.0}
    assert cpu.parse(reading(limit="max 100000"))["limit_cores"] is None                 # no limit is not zero cores
    assert cpu.parse(reading(limit="50000 100000"))["limit_cores"] == 0.5


@pytest.mark.parametrize("text", ["", "garbage\n", "nr_periods 4\nnr_throttled 1\n", "usage_usec abc\n",
                                  "cpu 123\ncpu0 12\n"])                                  # empty, nonsense, no usage_usec, cgroup v1-ish
def test_a_text_that_is_not_a_cgroup_v2_cpu_stat_is_not_read(text):
    assert cpu.parse(text) is None


def test_the_use_in_a_storm_is_the_difference_and_a_restarted_container_has_none():
    b = cpu.parse(reading(usage=1_000_000, periods=10, throttled=1, throttled_us=20_000, limit="200000 100000"))
    a = cpu.parse(reading(usage=3_500_000, periods=30, throttled=6, throttled_us=220_000, limit="200000 100000"))
    d = cpu.diff(b, a)
    assert d["usage_usec"] == 2_500_000 and d["nr_periods"] == 20 and d["nr_throttled"] == 5 and d["throttled_usec"] == 200_000 and d["limit_cores"] == 2.0
    assert cpu.diff(a, b) is None                                                         # counters went backwards: the pod restarted
    assert cpu.diff(None, a) is None and cpu.diff(b, None) is None


def test_a_snapshot_difference_drops_pods_without_a_sound_reading():
    ok_b, ok_a = cpu.parse(reading(1_000_000)), cpu.parse(reading(2_000_000))
    before = {"broker": {"broker-0": ok_b, "broker-1": cpu.parse(reading(9_000_000))}, "callout": {"c1": ok_b}}
    after = {"broker": {"broker-0": ok_a, "broker-1": cpu.parse(reading(100))},            # broker-1 restarted
             "callout": {"c2": ok_a}}                                                      # the callout pod was replaced: no earlier reading
    d = cpu.snapshot_diff(before, after)
    assert list(d) == ["broker"] and list(d["broker"]) == ["broker-0"] and d["broker"]["broker-0"]["usage_usec"] == 1_000_000


# -- what a storm's file says -------------------------------------------------------------------------------------------------------------------
def burst(conns=800, seconds=0.5, gen=None):
    return {"connections": conns, "seconds": seconds, "outcomes": {"admitted": conns, "refused": 0, "timeout": 0, "error": 0},
            "authMs": {"n": conns, "p50": 1.0, "p90": 1.0, "p99": 1.0, "max": 1.0}, **({"cgroupCpu": gen} if gen else {})}


def pod(usage_s, periods=0, throttled=0, throttled_s=0.0, limit=None):
    return {"usage_usec": int(usage_s * 1e6), "user_usec": 0, "system_usec": 0, "nr_periods": periods, "nr_throttled": throttled,
            "throttled_usec": int(throttled_s * 1e6), "limit_cores": limit}


def storm(*, broker=None, callout=None, gen=None, runs=8, conns=800, seconds=0.5):
    comps = {k: v for k, v in (("broker", broker), ("callout", callout)) if v}
    return {"run": "x", "runs": [burst(conns, seconds, gen) for _ in range(runs)], **({"cpu": {"components": comps}} if comps else {})}


def test_cpu_per_connection_and_cores_busy_come_from_the_storms_totals():
    s = storm(broker={"broker-0": pod(12.8, periods=1000, throttled=250, throttled_s=4.0, limit=3.0)}, callout={"c": pod(6.4, limit=1.0)})
    st = cpu.storm_stats(s)
    # 8 bursts x 800 connections = 6400 connections; 8 bursts x 0.5 s = 4 s of bursting
    assert st["broker"]["ms_per_connection"] == pytest.approx(2.0) and st["broker"]["cores_busy"] == pytest.approx(3.2)
    assert st["broker"]["throttled_pct"] == 25.0 and st["broker"]["throttled_s"] == 4.0 and st["broker"]["limit_cores"] == 3.0
    assert st["callout"]["ms_per_connection"] == pytest.approx(1.0) and st["callout"]["cores_busy"] == pytest.approx(1.6)
    assert st["callout"]["throttled_pct"] is None                                           # no periods counted: no limit enforced
    assert "generator" not in st


def test_a_cluster_of_brokers_is_summed_and_a_missing_limit_makes_the_limit_unknown():
    s = storm(broker={"broker-0": pod(4.0, limit=3.0), "broker-1": pod(4.0, limit=3.0), "broker-2": pod(4.0, limit=None)})
    st = cpu.storm_stats(s)["broker"]
    assert st["cores_busy"] == pytest.approx(3.0) and st["limit_cores"] is None
    assert cpu.storm_stats(storm(broker={"b0": pod(1.0, limit=3.0), "b1": pod(1.0, limit=3.0)}))["broker"]["limit_cores"] == 6.0


def test_the_generators_own_bursts_are_summarised_per_burst():
    gen = {"usageUsec": 900_000, "periods": 10, "throttledPeriods": 4, "throttledUsec": 150_000, "limitCores": 2.0}
    st = cpu.storm_stats(storm(gen=gen))["generator"]
    assert st["cores_busy"] == pytest.approx(1.8) and st["ms_per_connection"] == pytest.approx(1.125)   # 0.9 s over a 0.5 s burst of 800
    assert st["throttled_pct"] == 40.0 and st["throttled_s"] == 0.15 and st["limit_cores"] == 2.0


def test_a_storm_without_cpu_readings_has_no_cpu_stats():
    assert cpu.storm_stats(storm()) == {}
    assert cpu.storm_stats({**burst(), "cgroupCpu": None}) == {}                              # a single-burst report, generator could not read it


def test_a_single_burst_report_is_read_too():
    one = {**burst(1, 1.0, {"usageUsec": 500_000, "periods": 0, "throttledPeriods": 0, "throttledUsec": 0, "limitCores": None}), "cpu": {"components": {"broker": {"b": pod(2.0)}}}}
    st = cpu.storm_stats(one)
    assert st["broker"]["cores_busy"] == pytest.approx(2.0) and st["generator"]["cores_busy"] == pytest.approx(0.5)


# -- the report ---------------------------------------------------------------------------------------------------------------------------------
def test_the_report_section_shows_each_variant_beside_the_others_with_the_limit():
    main = {1: [storm(broker={"b": pod(12.8, limit=3.0)}), storm(broker={"b": pod(12.8, limit=3.0)})], 2: [storm(broker={"b": pod(16.0, limit=3.0)})]}
    nolock = {1: [storm(broker={"b": pod(9.6, limit=3.0)})], 2: [storm(broker={"b": pod(9.6, limit=3.0)})]}
    text = "\n".join(cpu.summarize({"main": main, "nolock": nolock}, ["main", "nolock"]))
    assert "broker CPU ms per connection" in text and "broker cores busy while bursting (limit 3)" in text
    row = [l for l in text.splitlines() if "broker CPU ms per connection" in l][0].split()
    assert float(row[-2]) == pytest.approx((2.0 + 2.5) / 2) and float(row[-1]) == pytest.approx(1.5)      # mean over blocks of each block's median
    rows = [l.split()[0] for l in text.splitlines() if l.startswith("    ") and l.split()[0] in ("broker", "callout", "generator")]
    assert set(rows) == {"broker"}                                                                     # rows only for what was recorded
    assert "no p values" in text


def test_a_variant_without_readings_shows_a_dash_and_results_without_any_show_nothing():
    some = {1: [storm(broker={"b": pod(4.0, limit=3.0)})]}
    text = "\n".join(cpu.summarize({"a": some, "b": {1: [storm()]}}, ["a", "b"]))
    assert [l for l in text.splitlines() if "ms per connection" in l][0].rstrip().endswith("-")
    assert cpu.summarize({"a": {1: [storm()]}, "b": {1: [storm()]}}, ["a", "b"]) == []                  # an older result: reported as it always was


def test_the_summary_of_a_run_directory_carries_the_cpu_section(tmp_path):
    out = tmp_path / "run"
    (out / "raw").mkdir(parents=True)
    (out / "environment.json").write_text(json.dumps({"target": "local", "date": "d", "variants": [{"label": "a", "commit": "", "image": "x"}, {"label": "b", "commit": "", "image": "y"}],
                                                      "design": {"phases": ["warm", "idle", "n800"]}}))
    for v, usage in (("a", 12.8), ("b", 9.6)):
        (out / "raw" / f"{v}-b1.meta.json").write_text("{}")
        (out / "raw" / f"{v}-b1-n800.json").write_text(json.dumps(storm(broker={"broker-0": pod(usage, limit=3.0)})))
    text = report.summarize(out)
    assert "CPU (broker and callout: the whole storm" in text and "broker CPU ms per connection" in text and "2.00" in text and "1.50" in text


# -- reading the cgroups of the running pods ------------------------------------------------------------------------------------------------------
PODS = lambda *names: json.dumps({"items": [{"metadata": {"name": n}} for n in names]})


def snap_backend(outputs, **kw):
    return kube.KubeBackend(Scripted(outputs), sleep=lambda s: None, **kw)


def test_each_pod_is_read_with_one_exec_and_the_pods_are_found_once(kubeconfig):
    run = Scripted([PODS("broker-0", "broker-1"), PODS("callout-abc"), reading(1), reading(2), reading(3), reading(4), reading(5), reading(6)])
    b = kube.KubeBackend(run, sleep=lambda s: None)
    first = b.cpu_snapshot()
    assert {c: sorted(p) for c, p in first.items()} == {"broker": ["broker-0", "broker-1"], "callout": ["callout-abc"]}
    assert first["broker"]["broker-1"]["usage_usec"] == 2
    execs = [c[5:] for c in run.cmds if "exec" in c]
    assert execs[0] == ["exec", "broker-0", "-c", "broker", "--", "cat", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max"]
    assert execs[2] == ["exec", "callout-abc", "-c", "callout", "--", "cat", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max"]
    b.cpu_snapshot()                                                                          # the second reading does not list the pods again
    assert sum(1 for c in run.cmds if "get" in c and "pods" in c) == 2 and sum(1 for c in run.cmds if "exec" in c) == 6


def test_a_reading_that_cannot_be_made_is_said_once_and_never_stops_anything(kubeconfig, capsys):
    boom = subprocess.CalledProcessError(126, ["kubectl"])
    run = Scripted([PODS("broker-0"), PODS("callout-abc"), boom, reading(1), boom, boom])
    b = kube.KubeBackend(run, sleep=lambda s: None)
    assert list(b.cpu_snapshot()) == ["callout"]                                              # the broker's failed, the callout's did not
    assert b.cpu_snapshot() == {}
    err = capsys.readouterr().err
    assert err.count("could not read the cgroup of broker-0") == 1 and "needs `cat`" in err   # the same note is not repeated per storm


def test_a_cgroup_v1_node_is_said_not_to_be_read(kubeconfig, capsys):
    run = Scripted([PODS("broker-0"), PODS(), "cpu 100\ncpu0 40\n"])
    assert kube.KubeBackend(run, sleep=lambda s: None).cpu_snapshot() == {}
    assert "no cgroup v2 cpu.stat" in capsys.readouterr().err


def test_a_storm_is_bracketed_by_two_readings_and_keeps_their_difference(kubeconfig):
    run = Scripted([PODS("broker-0"), PODS("callout-abc"),
                    reading(1_000_000, limit="300000 100000"), reading(500_000, limit="100000 100000"),            # before
                    json.dumps({"status": {"succeeded": 1}}), "THE LOG",
                    reading(4_000_000, periods=20, throttled=5, throttled_us=90_000, limit="300000 100000"),    # after
                    reading(1_500_000, periods=10, throttled=10, throttled_us=500_000, limit="100000 100000")])
    b = kube.KubeBackend(run, sleep=lambda s: None)
    b._job = {"metadata": {"name": "loadgen"}, "spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    assert b.run_storm("main-b1-n800", {}) == "THE LOG"
    verbs = [next(a for a in c if a in ("exec", "create", "logs", "delete", "get")) for c in run.cmds]
    made = verbs.index("create")
    assert verbs[:made].count("exec") == 2 and verbs[made - 1] == "exec"                              # one pair of readings, the last thing before the Job is made...
    assert verbs[made:].count("exec") == 2 and verbs[-2:] == ["exec", "exec"] and verbs.index("logs") < len(verbs) - 2   # ...and one pair after it ended and its log was read
    c = b.last_cpu["components"]
    assert c["broker"]["broker-0"]["usage_usec"] == 3_000_000 and c["broker"]["broker-0"]["limit_cores"] == 3.0
    assert c["callout"]["callout-abc"]["usage_usec"] == 1_000_000 and c["callout"]["callout-abc"]["nr_throttled"] == 10
    assert "cgroup v2" in b.last_cpu["source"]


def test_a_storm_still_runs_when_no_cpu_can_be_read_and_without_the_switch_nothing_is_read(kubeconfig):
    boom = subprocess.CalledProcessError(1, ["kubectl"])
    run = Scripted([PODS("broker-0"), PODS(), boom, json.dumps({"status": {"succeeded": 1}}), "LOG", boom])
    b = kube.KubeBackend(run, sleep=lambda s: None)
    b._job = {"metadata": {"name": "loadgen"}, "spec": {"template": {"spec": {"containers": [{"name": "loadgen"}]}}}}
    assert b.run_storm("x-b1-warm", {}) == "LOG" and b.last_cpu is None
    run2 = Scripted([json.dumps({"status": {"succeeded": 1}}), "LOG"])
    b2 = kube.KubeBackend(run2, sleep=lambda s: None, read_cpu=False)
    b2._job = b._job
    assert b2.run_storm("x-b1-warm", {}) == "LOG" and b2.last_cpu is None and not any("exec" in c for c in run2.cmds)


def test_a_storm_read_afterwards_has_no_readings(kubeconfig):
    b = kube.KubeBackend(Scripted([json.dumps({"metadata": {"labels": {kube.LBL_ATTEMPT: "att"}}}), json.dumps({"status": {"succeeded": 1}}), "LOG"]), sleep=lambda s: None)
    b.last_cpu = {"components": {"broker": {}}}
    assert b.collect_storm("main-b1-n800", "att") == "LOG" and b.last_cpu is None


# -- storing it and switching it off -------------------------------------------------------------------------------------------------------------
def test_the_driver_stores_the_readings_in_the_storms_report_beside_the_generators(tmp_path):
    from test_jobs import Cluster, PLAN, report_text

    class Metered(Cluster):
        last_cpu = {"source": "s", "components": {"broker": {"broker-0": pod(2.0)}}}

    raw = tmp_path / "raw"
    ab.run_blocks(PLAN, Metered(), raw, sleep=lambda s: None, now=lambda: "t", log=lambda m: None)
    rep = json.loads((raw / "main-b1-n800.json").read_text())
    assert rep["cpu"]["components"]["broker"]["broker-0"]["usage_usec"] == 2_000_000 and "runs" in rep      # the generator's own keys are untouched
    bare = tmp_path / "bare"
    ab.run_blocks(PLAN, Cluster(), bare, sleep=lambda s: None, now=lambda: "t", log=lambda m: None)
    assert "cpu" not in json.loads((bare / "main-b1-n800.json").read_text())                              # nothing read: no key, no null


def test_no_cpu_is_a_run_option_and_reaches_the_backend(tmp_path):
    from test_state import FakeCluster, go, run_args
    from stormbench import state
    cluster = FakeCluster()
    assert go(run_args(tmp_path, "--no-cpu"), state.MemoryStore(), cluster) == 0
    assert cluster.kwargs[-1]["read_cpu"] is False
    other = FakeCluster()
    assert go(run_args(tmp_path / "x"), state.MemoryStore(), other) == 0
    assert other.kwargs[-1]["read_cpu"] is True
    assert cli.build_parser().parse_args(["run", "--resume", "--no-cpu"]).no_cpu is True


# -- the probes' per-burst readings ---------------------------------------------------------------------------------------------------------
def pc(component, pod, usage_s, periods=0, throttled=0, throttled_s=0.0, limit=None):
    return {"component": component, "pod": pod, "container": component, "usageUsec": int(usage_s * 1e6), "userUsec": 0, "systemUsec": 0,
            "periods": periods, "throttledPeriods": throttled, "throttledUsec": int(throttled_s * 1e6), "limitCores": limit}


def probed_storm(per_burst, runs=4, conns=800, seconds=0.5, **kw):
    return {"run": "x", "runs": [{**burst(conns, seconds), "probeCpu": {"probes": 2, "errors": [], "containers": per_burst}} for _ in range(runs)], **kw}


def test_the_probes_readings_give_a_bursts_own_cpu_not_one_diluted_by_the_pauses():
    st = cpu.storm_stats(probed_storm([pc("broker", "broker-0", 1.2, periods=5, throttled=1, throttled_s=0.02, limit=3.0),
                                       pc("callout", "callout-x", 0.46, periods=5, throttled=2, throttled_s=0.18, limit=1.0)]))
    # one burst: 800 connections in 0.5 s
    assert st["broker"]["cores_busy"] == pytest.approx(2.4) and st["broker"]["ms_per_connection"] == pytest.approx(1.5)
    assert st["callout"]["cores_busy"] == pytest.approx(0.92) and st["callout"]["throttled_pct"] == 40.0 and st["callout"]["throttled_s"] == pytest.approx(0.18)
    assert st["broker"]["limit_cores"] == 3.0 and st["callout"]["limit_cores"] == 1.0 and st["broker"]["source"] == "probe"


def test_a_cluster_of_brokers_is_summed_per_burst_and_the_generators_own_pod_is_left_to_its_own_reading():
    st = cpu.storm_stats(probed_storm([pc("broker", "broker-0", 0.5, limit=3.0), pc("broker", "broker-1", 0.5, limit=3.0), pc("broker", "broker-2", 0.5, limit=3.0),
                                       pc("loadgen", "loadgen-x", 9.9)]))
    assert st["broker"]["cores_busy"] == pytest.approx(3.0) and st["broker"]["limit_cores"] == 9.0
    assert "loadgen" not in st


def test_the_probes_win_over_the_drivers_whole_storm_reading_for_the_same_component():
    s = probed_storm([pc("broker", "broker-0", 1.0, limit=3.0)], cpu={"components": {"broker": {"broker-0": pod(99.0, limit=3.0)}, "callout": {"c": pod(3.2, limit=1.0)}}})
    st = cpu.storm_stats(s)
    assert st["broker"]["source"] == "probe" and st["broker"]["cores_busy"] == pytest.approx(2.0)      # not the 99 CPU-seconds of the exec reading
    assert "source" not in st["callout"]                                                               # nothing from the probes for it: the exec reading stands


def test_a_burst_the_probes_could_not_read_is_left_out_of_the_median():
    s = probed_storm([pc("broker", "b", 1.0)], runs=3)
    s["runs"][0]["probeCpu"] = None
    s["runs"][1]["probeCpu"] = {"probes": 2, "errors": ["x: timed out"], "containers": []}
    assert cpu.storm_stats(s)["broker"]["cores_busy"] == pytest.approx(2.0)                             # the one burst that has a reading


def test_the_report_header_says_which_source_the_cpu_came_from():
    probed = {1: [probed_storm([pc("broker", "b", 1.0, limit=3.0)])]}
    execd = {1: [storm(broker={"b": pod(4.0, limit=3.0)})]}
    assert "each burst: the probes' readings" in "\n".join(cpu.summarize({"a": probed, "b": probed}, ["a", "b"]))
    assert "read from their cgroups by the driver" in "\n".join(cpu.summarize({"a": execd, "b": execd}, ["a", "b"]))
    mixed = "\n".join(cpu.summarize({"a": probed, "b": execd}, ["a", "b"]))
    assert "per burst from the probes where there are probes" in mixed
