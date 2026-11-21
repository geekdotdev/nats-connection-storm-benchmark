# SPDX-License-Identifier: Apache-2.0
"""One Job per storm, kept until the run is over, and a stopped run's unit finished from what the cluster still holds."""
import json
import re
import subprocess

import pytest

from stormbench import ab, cli, kube, runcmd, state
from stormbench.config import REPO_ROOT
from stormbench.runner import Runner

A, B = "a" * 40, "b" * 40
DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def report_text(connections, runs=2):
    one = {"connections": connections, "seconds": 0.2, "outcomes": {"admitted": connections, "refused": 0, "timeout": 0, "error": 0},
           "failures": {}, "authMs": {"n": connections, "p50": 30.0, "p90": 40.0, "p99": 50.0, "max": 60.0}}
    return "console\n" + ab.REPORT_PREFIX + json.dumps({"run": "x", "runs": [one] * runs}) + "\n"


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)


# -- the names ---------------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("label", ["main-b1-warm", "mux_nolock-b8-n1600", "Main-b1-n800", "a" * 90 + "-b1-n800", "x_y-b3-idle"])
def test_a_storms_job_name_is_a_valid_kubernetes_name(label):
    name = kube.job_name(label)
    assert DNS_LABEL.match(name) and len(name) <= 63 and name.startswith("loadgen-")


def test_job_names_are_stable_and_never_collide_for_labels_that_differ():
    assert kube.job_name("mux_main-b4-n800") == kube.job_name("mux_main-b4-n800")
    names = {kube.job_name(l) for l in ("Main-b1-n800", "main-b1-n800", "a_b-b1-n800", "a-b-b1-n800", "mux_main-b1-n800", "mux-main-b1-n800")}
    assert len(names) == 6                                             # case and `_` against `-` are told apart by the hash


def test_a_label_value_is_valid_and_a_long_one_is_shortened_the_same_way_each_time():
    v = kube.label_value("a" * 100)
    assert len(v) <= 63 and re.match(r"^[A-Za-z0-9][-A-Za-z0-9_.]*[A-Za-z0-9]$", v) and v == kube.label_value("a" * 100)
    assert kube.label_value("mux_nolock-b8-n1600") == "mux_nolock-b8-n1600"


# -- the backend against a scripted kubectl ----------------------------------------------------------------------------------------------
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
    b._job = {"metadata": {"name": "loadgen", "labels": {"app.kubernetes.io/name": "loadgen"}},
              "spec": {"template": {"spec": {"containers": [{"name": "loadgen", "env": [{"name": "STORM_CONNECTIONS", "value": "800"}]}]}}}}
    return b


def job_json(attempt, succeeded=0, failed=0):
    return json.dumps({"metadata": {"labels": {kube.LBL_ATTEMPT: attempt}}, "status": {"succeeded": succeeded, "failed": failed}})


def test_each_storm_gets_its_own_labelled_job_and_only_its_own_is_deleted_first(kubeconfig):
    run = Scripted([json.dumps({"status": {"succeeded": 1}}), "THE LOG"])
    b = backend(run)
    b.set_context("run123", "mux_main-b4", "att0001")
    assert b.run_storm("mux_main-b4-n800", {"STORM_LABEL": "mux_main-b4-n800"}) == "THE LOG"
    name = kube.job_name("mux_main-b4-n800")
    assert run.cmds[0][5:] == ["delete", "job", name, "--ignore-not-found", "--wait=true"]       # not `delete job loadgen`, and no selector
    made = json.loads(run.stdin[1])
    assert made["metadata"]["name"] == name
    assert made["metadata"]["labels"] == {"app.kubernetes.io/name": "loadgen", kube.LBL_STORM: "mux_main-b4-n800", kube.LBL_RUN: "run123",
                                          kube.LBL_UNIT: "mux_main-b4", kube.LBL_ATTEMPT: "att0001"}
    assert run.cmds[-1][5:] == ["logs", f"job/{name}"]                                         # the log of that Job, by its name


def test_collect_storm_finds_nothing_where_there_is_no_job_or_it_is_another_attempts(kubeconfig):
    assert backend(Scripted([""])).collect_storm("main-b1-n800", "att0001") is None
    assert backend(Scripted([job_json("someone-elses", succeeded=1)])).collect_storm("main-b1-n800", "att0001") is None


def test_collect_storm_reads_a_finished_jobs_log_and_waits_for_a_running_one(kubeconfig):
    run = Scripted([job_json("att0001", succeeded=1), json.dumps({"status": {"succeeded": 1}}), "THE LOG"])
    assert backend(run).collect_storm("main-b1-n800", "att0001") == "THE LOG"
    assert any(c[-2:] == ["logs", f"job/{kube.job_name('main-b1-n800')}"] for c in run.cmds)
    waiting = Scripted([job_json("att0001"), json.dumps({"status": {"active": 1}}), json.dumps({"status": {"active": 1}}),
                        json.dumps({"status": {"succeeded": 1}}), "LATE LOG"])
    assert backend(waiting).collect_storm("main-b1-n800", "att0001") == "LATE LOG"


def test_collect_storm_reports_a_failed_job(kubeconfig):
    run = Scripted([job_json("att0001", failed=1), json.dumps({"status": {"failed": 1}}), "l1\nboom"])
    with pytest.raises(ab.StormFailed, match="boom"):
        backend(run).collect_storm("main-b1-n800", "att0001")


def test_the_brokers_identity_is_recorded_and_can_be_read_again(kubeconfig):
    pods = json.dumps({"items": [{"metadata": {"uid": "u-123"}, "status": {"containerStatuses": [{"imageID": "i", "image": "j"}]}},
                                 {"metadata": {"uid": "u-old", "deletionTimestamp": "t"}, "status": {}}]})
    run = Scripted([pods, "nats-server: v2.99", pods])
    b = backend(run)
    assert b._broker_facts()["pod_uid"] == "u-123"
    assert b.broker_uid() == "u-123"                                                            # the pod being deleted is not the broker
    assert backend(Scripted([json.dumps({"items": []})])).broker_uid() == ""


def test_the_jobs_of_a_unit_or_a_finished_run_are_deleted_by_label(kubeconfig, capsys):
    run = Scripted([])
    b = backend(run)
    b.clear_unit_jobs("main-b4")
    b.delete_storm_jobs()
    b.cleanup_jobs("run123")
    assert [c[5:] for c in run.cmds] == [["delete", "jobs", "-l", "storm-bench/unit=main-b4", "--ignore-not-found", "--wait=true"],
                                         ["delete", "jobs", "-l", "app.kubernetes.io/name=loadgen", "--ignore-not-found", "--wait=true"],
                                         ["delete", "jobs", "-l", "storm-bench/run-id=run123", "--ignore-not-found", "--wait=false"]]

    class Failing(Scripted):
        def __call__(self, cmd, **kw):
            raise subprocess.CalledProcessError(1, cmd)
    backend(Failing([])).cleanup_jobs("run123")                                                  # said, not raised: the results are on disk
    assert "could not delete the run's Jobs" in capsys.readouterr().err


def test_the_manifest_keeps_the_name_and_label_the_driver_selects_by_and_has_no_ttl():
    text = (REPO_ROOT / "deploy" / "loadgen" / "base" / "job.yaml").read_text()
    assert "ttlSecondsAfterFinished" not in text.replace("There is no ttlSecondsAfterFinished", "")
    assert "name: loadgen" in text and "app.kubernetes.io/name: loadgen" in text and "activeDeadlineSeconds: 1800" in text


# -- finishing a stopped unit from the cluster ------------------------------------------------------------------------------------------------
class Cluster:
    """A backend that remembers what it was asked, with Jobs on the 'cluster' that a stopped driver left."""
    def __init__(self, uid="u1", jobs=None):
        self.uid, self.jobs = uid, dict(jobs or {})          # jobs: storm label -> (attempt, log)
        self.started, self.stormed, self.contexts, self.cleared, self.collected = [], [], [], [], []
        self.deleted_all = 0
        self.cleaned = []

    def set_context(self, run_id, unit, attempt):
        self.contexts.append((run_id, unit, attempt))

    def clear_unit_jobs(self, unit):
        self.cleared.append(unit)

    def start_variant(self, v):
        self.started.append(v.label)
        return {"server_version": "v", "pod_uid": "fresh-uid"}

    def run_storm(self, label, env):
        self.stormed.append(label)
        return report_text(int(env["STORM_CONNECTIONS"]))

    def broker_uid(self):
        return self.uid

    def collect_storm(self, label, attempt):
        self.collected.append(label)
        got = self.jobs.get(label)
        return got[1] if got and got[0] == attempt else None

    def delete_storm_jobs(self):
        self.deleted_all += 1

    def cleanup_jobs(self, run_id):
        self.cleaned.append(run_id)


PLAN = ab.Plan(variants=(ab.Variant("main", A, "x"), ab.Variant("fixed", B, "y")), blocks=1, sizes=(800,))


def stopped_unit(tmp_path, *, with_files=("warm", "idle"), pod_uid="u1"):
    """main-b1 ran warm and idle and its driver stopped while n800 was running (or after it): the state has it running."""
    store = state.MemoryStore()
    doc = state.plan_document(PLAN, name="n", out="o", target="local", stack_overlay="s", loadgen_overlay="l", settle_s=8.0, design={},
                              revision="r", now=1.0)
    rs = state.RunState.create(store, doc, revision="rev1")
    unit = next(u for u in ab.units(PLAN) if u.key == "main-b1")
    attempt = rs.begin(unit)
    if pod_uid is not None:
        rs.broker_up(unit, {"pod_uid": pod_uid})
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "main-b1.meta.json").write_text(json.dumps({"variant": "main", "pod_uid": pod_uid}))
    for ph in with_files:
        (raw / f"main-b1-{ph}.json").write_text(json.dumps({"run": "x", "runs": []}))
        (raw / f"main-b1-{ph}.txt").write_text("log")
    return store, rs, unit, attempt, raw


def run_loop(rs, cluster, raw, log=None):
    ab.run_blocks(PLAN, cluster, raw, sleep=lambda s: None, now=lambda: "t", log=(log or (lambda m: None)), progress=rs)


def test_a_unit_whose_last_storm_finished_while_the_driver_was_down_is_finished_from_the_cluster(tmp_path):
    store, rs, unit, attempt, raw = stopped_unit(tmp_path)
    cluster = Cluster(jobs={"main-b1-n800": (attempt, report_text(800))})
    said = []
    run_loop(rs, cluster, raw, said.append)
    assert cluster.started == ["fixed"]                                                  # main's broker was NOT restarted: only fixed's was
    assert cluster.stormed == ["fixed-b1-warm", "fixed-b1-idle", "fixed-b1-n800"] and cluster.collected == ["main-b1-n800"]
    assert json.loads((raw / "main-b1-n800.json").read_text())["runs"][0]["connections"] == 800
    assert rs.progress["units"]["main-b1"]["status"] == "done"
    assert any("finished from the cluster" in m and "n800" in m for m in said)


def test_a_unit_that_was_whole_but_not_marked_done_is_just_marked(tmp_path):
    store, rs, unit, attempt, raw = stopped_unit(tmp_path, with_files=("warm", "idle", "n800"))
    cluster = Cluster()
    said = []
    run_loop(rs, cluster, raw, said.append)
    assert cluster.collected == [] and rs.progress["units"]["main-b1"]["status"] == "done"
    assert any("every storm had been read already" in m for m in said)


@pytest.mark.parametrize("what,cluster_kw,state_kw", [
    ("the broker was replaced since", {"uid": "someone-restarted-it"}, {}),
    ("no storm of this attempt on the cluster", {"jobs": {}}, {}),
    ("a storm of an earlier attempt only", {"jobs": {"main-b1-n800": ("an-old-attempt", report_text(800))}}, {}),
    ("the driver stopped before the broker's identity was recorded", {"jobs": "ok"}, {"pod_uid": None}),
    ("the broker's identity is empty", {"jobs": "ok"}, {"pod_uid": ""}),
])
def test_a_unit_that_cannot_be_vouched_for_is_run_again_from_a_fresh_broker(tmp_path, what, cluster_kw, state_kw):
    store, rs, unit, attempt, raw = stopped_unit(tmp_path, **state_kw)
    kw = dict(cluster_kw)
    if kw.get("jobs") == "ok":
        kw["jobs"] = {"main-b1-n800": (attempt, report_text(800))}
    cluster = Cluster(**kw)
    run_loop(rs, cluster, raw)
    assert cluster.started[0] == "main", what                                            # restarted and measured again
    assert [s for s in cluster.stormed if s.startswith("main")] == ["main-b1-warm", "main-b1-idle", "main-b1-n800"]
    assert "main-b1" in cluster.cleared                                                  # and its earlier Jobs were deleted
    assert rs.progress["units"]["main-b1"]["status"] == "done" and rs.progress["units"]["main-b1"]["attempt"] != attempt


def test_a_log_with_no_report_is_not_taken_for_a_measurement(tmp_path):
    store, rs, unit, attempt, raw = stopped_unit(tmp_path)
    cluster = Cluster(jobs={"main-b1-n800": (attempt, "the pod's log was cut short\n")})
    run_loop(rs, cluster, raw)
    assert cluster.started[0] == "main"


def test_a_unit_that_is_not_running_in_the_state_is_never_rescued(tmp_path):
    store, rs, unit, attempt, raw = stopped_unit(tmp_path)
    rs.progress["units"].pop("main-b1")                                                  # the state knows nothing of it
    cluster = Cluster(jobs={"main-b1-n800": (attempt, report_text(800))})
    run_loop(rs, cluster, raw)
    assert cluster.started[0] == "main" and cluster.collected == []


def test_each_unit_tells_the_backend_which_run_unit_and_attempt_its_jobs_belong_to(tmp_path):
    store = state.MemoryStore()
    doc = state.plan_document(PLAN, name="n", out="o", target="local", stack_overlay="s", loadgen_overlay="l", settle_s=8.0, design={},
                              revision="r", now=1.0, run_id="run123")
    rs = state.RunState.create(store, doc, revision="rev1")
    cluster = Cluster()
    run_loop(rs, cluster, tmp_path / "raw")
    assert [(c[0], c[1]) for c in cluster.contexts] == [("run123", "main-b1"), ("run123", "fixed-b1")]
    attempts = [c[2] for c in cluster.contexts]
    assert len(set(attempts)) == 2 and all(re.match(r"^[0-9a-f]{8}$", a) for a in attempts)
    assert cluster.cleared == ["main-b1", "fixed-b1"]
    assert json.loads((tmp_path / "raw" / "main-b1.meta.json").read_text())["pod_uid"] == "fresh-uid"
    assert rs.progress["units"]["main-b1"]["pod_uid"] == "fresh-uid"


def test_a_run_without_progress_is_unchanged(tmp_path):
    cluster = Cluster()
    ab.run_blocks(PLAN, cluster, tmp_path / "raw", sleep=lambda s: None, now=lambda: "t", log=lambda m: None)
    assert cluster.cleared == [] and cluster.collected == [] and cluster.contexts[0][2] == ""      # no state: no attempts, nothing rescued


# -- the whole command --------------------------------------------------------------------------------------------------------------------
def parse(*argv):
    return cli.build_parser().parse_args(["--target", "local", *argv])


class Backend:
    """What runcmd builds: a class whose instances share one Cluster."""
    cluster = None

    def __init__(self, run, **kw):
        self.c = Backend.cluster

    def __enter__(self):
        return self

    def __exit__(self, *e):
        return None

    def prepare(self, variants, apply=True):
        Backend.cluster.prepare_apply = apply
        if apply:                                            # as on a real cluster: applying the first variant restarts the broker
            Backend.cluster.uid = "replaced-by-prepare"

    def describe(self):
        return {"nodes": []}

    def __getattr__(self, name):
        return getattr(self.c, name)


def test_a_new_run_clears_old_jobs_once_and_a_finished_run_cleans_up_its_own(tmp_path):
    Backend.cluster = Cluster()
    args = parse("run", "--a", A, "--b", B, "--a-label", "main", "--b-label", "fixed", "--blocks", "1", "--sizes", "800",
                 "--out", str(tmp_path / "out"), "--no-preflight")
    store = state.MemoryStore()
    assert runcmd.execute(Runner(), args, make_store=lambda t: store, backend_factory=Backend) == 0
    rs = state.RunState.load(store)
    assert Backend.cluster.deleted_all == 1 and Backend.cluster.cleaned == [rs.run_id]


def test_a_stopped_run_is_resumed_and_the_stopped_units_storm_is_read_not_repeated(tmp_path):
    args = parse("run", "--a", A, "--b", B, "--a-label", "main", "--b-label", "fixed", "--blocks", "1", "--sizes", "800",
                 "--out", str(tmp_path / "out"), "--no-preflight")
    store = state.MemoryStore()
    first = Cluster()

    def stop_during_n800(label, env):                        # the driver dies as main's n800 storm is running: the Job goes on without it
        if label == "main-b1-n800":
            raise SystemExit(143)
        return Cluster.run_storm(first, label, env)
    first.run_storm = stop_during_n800
    Backend.cluster = first
    with pytest.raises(SystemExit):
        runcmd.execute(Runner(), args, make_store=lambda t: store, backend_factory=Backend)
    rs = state.RunState.load(store)
    attempt = rs.progress["units"]["main-b1"]["attempt"]
    assert rs.progress["units"]["main-b1"]["status"] == "running" and rs.progress["units"]["main-b1"]["pod_uid"] == "fresh-uid"

    second = Cluster(uid="fresh-uid", jobs={"main-b1-n800": (attempt, report_text(800))})         # the Job finished while the driver was down
    Backend.cluster = second
    assert runcmd.execute(Runner(), parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")),
                          make_store=lambda t: store, backend_factory=Backend) == 0
    assert second.prepare_apply is False                                                          # a resume applies nothing up front...
    assert second.started == ["fixed"] and second.collected == ["main-b1-n800"]                  # ...so main's broker is still the one recorded
    assert second.deleted_all == 0 and second.cleaned == [rs.run_id]                              # a resume does not clear the run's Jobs
    assert state.RunState.load(store).is_complete and (tmp_path / "out" / "raw" / "DONE").exists()


def test_a_new_run_applies_the_first_variant_up_front_and_a_resume_does_not(tmp_path):
    Backend.cluster = Cluster()
    args = parse("run", "--a", A, "--b", B, "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "out"), "--no-preflight")
    assert runcmd.execute(Runner(), args, make_store=lambda t: state.MemoryStore(), backend_factory=Backend) == 0
    assert Backend.cluster.prepare_apply is True


def test_prepare_without_apply_only_loads_the_job_template(kubeconfig):
    run = Scripted(["rendered yaml", json.dumps({"kind": "Job", "metadata": {"name": "loadgen"}, "spec": {}})])
    b = kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None)
    b.prepare((ab.Variant("a", A, "x"),), apply=False)
    verbs = [c[3] for c in run.cmds]
    assert verbs == ["kustomize", "create"]                                        # no apply, no rollout status: the cluster is not touched
    assert b._job["kind"] == "Job"


def test_prepare_by_default_still_applies_and_waits_for_the_rollouts(kubeconfig):
    run = Scripted(["rendered yaml", json.dumps({"kind": "Job", "metadata": {"name": "loadgen"}, "spec": {}})])
    b = kube.KubeBackend(run, read_cpu=False, sleep=lambda s: None)
    b.prepare((ab.Variant("a", A, "x"),))
    assert [c[3] for c in run.cmds][:1] == ["apply"] and sum(1 for c in run.cmds if "rollout" in c) == 2
