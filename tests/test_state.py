# SPDX-License-Identifier: Apache-2.0
"""The run state in the cluster: stopping a run and starting it again, the lease, and adopting a run that has no state.
Nothing here reaches a cluster: the store is in memory and the backend is a fake (tests/conftest.py also makes any real kubectl call fail)."""
import json
import signal
import subprocess
from pathlib import Path

import pytest

from stormbench import ab, cli, report, runcmd, state
from stormbench.runner import Runner

A, B = "a" * 40, "b" * 40


def report_text(connections: int, runs: int = 2, admitted: int | None = None) -> str:
    one = {"connections": connections, "seconds": 0.2,
           "outcomes": {"admitted": connections if admitted is None else admitted, "refused": 0, "timeout": 0, "error": 0},
           "failures": {} if admitted is None else {"error: Connection refused (os error 111)": connections - admitted},
           "authMs": {"n": connections, "p50": 30.0, "p90": 40.0, "p99": 50.0, "max": 60.0}}
    return "console\n" + ab.REPORT_PREFIX + json.dumps({"run": "x", "runs": [one] * runs}) + "\n"


class FakeCluster:
    """What a backend does, recorded: which variants got a fresh broker, which storms ran, and which storm should fail."""
    def __init__(self, fail_on=None):
        self.started, self.stormed, self.fail_on = [], [], fail_on
        self.kwargs = []

    def factory(self):
        cluster = self

        class Backend:
            def __init__(self, run, **kw):
                cluster.kwargs.append(kw)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return None

            def prepare(self, variants, apply=True):
                cluster.prepared = [v.label for v in variants]
                cluster.applied_up_front = apply

            def describe(self):
                return {"nodes": []}

            def start_variant(self, v):
                cluster.started.append(v.label)
                return {"server_version": "v-test"}

            def run_storm(self, label, env):
                if label == cluster.fail_on:
                    raise ab.StormFailed(f"{label}: boom")
                cluster.stormed.append(label)
                return report_text(int(env["STORM_CONNECTIONS"]))
        return Backend


def parse(*argv):
    return cli.build_parser().parse_args(["--target", "local", *argv])


def run_args(tmp_path, *extra):
    return parse("run", "--a", A, "--b", B, "--a-label", "main", "--b-label", "fixed", "--blocks", "2", "--sizes", "800",
                 "--out", str(tmp_path / "out"), "--no-preflight", *extra)


def go(args, store, cluster):
    return runcmd.execute(Runner(), args, make_store=lambda target: store, backend_factory=cluster.factory())


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)


# -- the plan and its units ------------------------------------------------------------------------------------------------------------
def test_a_variant_survives_the_round_trip_through_its_recorded_form():
    from stormbench import variants
    from stormbench.config import REPO_ROOT
    for path in ("docs/variants.example.toml", "lock-variants.toml"):
        for v in variants.load(REPO_ROOT / path, "localhost/"):
            assert ab.Variant.from_dict(json.loads(json.dumps(v.describe()))) == v


def test_an_unknown_key_in_a_recorded_variant_is_refused():
    with pytest.raises(ValueError, match="different driver"):
        ab.Variant.from_dict({"label": "a", "image": "x", "teleport": True})


def test_units_are_the_schedule_grouped_by_block_and_variant_in_the_same_order():
    plan = ab.Plan(variants=(ab.Variant("main", A, "x"), ab.Variant("fixed", B, "y")), blocks=2, sizes=(800,))
    us = list(ab.units(plan))
    assert [u.key for u in us] == ["main-b1", "fixed-b1", "fixed-b2", "main-b2"]
    assert [st.label for u in us for st in u.steps] == [s.label for s in ab.schedule(plan)]
    assert [len(u.steps) for u in us] == [3] * 4                      # warm, idle, n800


# -- stopping and starting again -----------------------------------------------------------------------------------------------------
def test_a_stopped_run_is_resumed_from_its_last_finished_unit(tmp_path):
    store = state.MemoryStore()
    first = FakeCluster(fail_on="fixed-b1-n800")
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, first)
    rs = state.RunState.load(store)
    assert rs.done_units() == 1 and rs.progress["units"]["fixed-b1"]["status"] == "running" and not rs.is_complete
    assert rs.lease["owner"] == ""                                     # the failed driver handed the run back
    assert (tmp_path / "out" / "raw" / "fixed-b1-warm.json").exists()      # the interrupted unit's warm-up is on disk, and is replaced below

    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again) == 0
    # main-b1 was done and is skipped; the broker of the unfinished fixed-b1 is restarted and all its phases run again
    assert again.started == ["fixed", "fixed", "main"] and again.prepared == ["main", "fixed"]
    assert [s for s in again.stormed if s.startswith("fixed-b1")] == ["fixed-b1-warm", "fixed-b1-idle", "fixed-b1-n800"]
    assert not any(s.startswith("main-b1") for s in again.stormed)
    rs = state.RunState.load(store)
    assert rs.is_complete and rs.done_units() == 4
    out = tmp_path / "out"
    assert (out / "raw" / "DONE").exists() and (out / "summary.txt").exists()
    env = json.loads((out / "environment.json").read_text())
    assert len(env["resumes"]) == 1 and env["resumes"][0]["units_done"] == 1
    assert json.loads((out / "raw" / "fixed-b1.meta.json").read_text())["driver_revision"] == rs.revision


def test_the_plan_comes_from_the_state_so_the_original_command_is_not_needed(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="main-b2-warm"))
    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight"), store, again) == 1  # results/<name> is not where this test put them
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again) == 0
    assert again.kwargs[-1]["stack"] == "deploy/stack/overlays/local" and again.kwargs[-1]["settle_s"] == 8.0


@pytest.mark.parametrize("option", [["--blocks", "3"], ["--variants", "x.toml"], ["--a", A], ["--sizes", "400"], ["--name", "x"], ["--threads", "2"]])
def test_resume_takes_no_plan_option(option):
    with pytest.raises(ValueError, match="takes the plan from the run's state"):
        runcmd.execute(Runner(), parse("run", "--resume", *option), make_store=lambda t: state.MemoryStore())


def test_resume_with_nothing_recorded_says_so(tmp_path):
    with pytest.raises(state.StateError, match="nothing to resume"):
        go(parse("run", "--resume"), state.MemoryStore(), FakeCluster())


def test_resume_and_discard_contradict_each_other():
    with pytest.raises(ValueError, match="give one"):
        runcmd.execute(Runner(), parse("run", "--resume", "--discard-state"))


def test_a_new_run_is_refused_while_another_is_unfinished_and_can_replace_it_on_request(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    other = FakeCluster()
    args = parse("run", "--a", A, "--b", B, "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "second"), "--no-preflight")
    assert go(args, store, other) == 1 and other.started == []        # nothing touched
    assert "`bench run --resume` continues it" in capsys.readouterr().out
    args = parse("run", "--a", A, "--b", B, "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "second"), "--no-preflight", "--discard-state")
    assert go(args, store, other) == 0
    assert state.RunState.load(store).plan["out"] == "second"


def test_a_finished_run_does_not_stand_in_the_way_of_the_next(tmp_path):
    store = state.MemoryStore()
    assert go(run_args(tmp_path), store, FakeCluster()) == 0
    assert state.RunState.load(store).is_complete
    nxt = parse("run", "--a", A, "--b", B, "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "next"), "--no-preflight")
    assert go(nxt, store, FakeCluster()) == 0
    assert state.RunState.load(store).plan["out"] == "next"
    assert go(parse("run", "--resume", "--no-preflight"), store, FakeCluster()) == 1      # and a finished run is not resumed


def test_a_run_made_by_a_driver_that_does_things_differently_is_not_resumed_unless_told(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    doc = json.loads(store.data["plan.json"])
    doc["design"]["per_phase"]["n800"]["pause_ms"] = 99                  # as if the code now paused differently
    store.data["plan.json"] = json.dumps(doc)
    again = FakeCluster()
    resume = ("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out"))
    assert go(parse(*resume), store, again) == 1 and again.started == []
    assert "differs: per_phase" in capsys.readouterr().out
    assert go(parse(*resume, "--accept-design-change"), store, again) == 0
    assert json.loads((tmp_path / "out" / "environment.json").read_text())["resumes"][0]["design_changed"] is True


def test_the_results_must_be_where_the_state_says(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "elsewhere")), store, FakeCluster()) == 1
    assert "kept on the machine that ran them" in capsys.readouterr().out


def test_a_unit_recorded_done_whose_files_are_gone_is_run_again(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    (tmp_path / "out" / "raw" / "main-b1-n800.json").unlink()            # main-b1 is done in the state, but not here
    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again) == 0
    assert again.started[:2] == ["main", "fixed"] and "main-b1-n800" in again.stormed
    assert "main-b1 is recorded as done but main-b1-n800.json is missing" in capsys.readouterr().err


def test_a_target_mismatch_is_refused(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    with pytest.raises(ValueError, match="for --target local, this is --target eks"):
        runcmd.execute(Runner(), cli.build_parser().parse_args(["--target", "eks", "run", "--resume"]), make_store=lambda t: store)


# -- the lease --------------------------------------------------------------------------------------------------------------------------
def fresh_state(now):
    doc = {"schema": state.SCHEMA, "run_id": "r1", "name": "n", "out": "o", "target": "local", "created": "t", "driver_revision": "r", "blocks": 1,
           "sizes": [800], "threads": 4, "variants": [{"label": "a", "commit": "", "image": "x"}, {"label": "b", "commit": "", "image": "y"}],
           "design": {}, "settle_s": 8.0, "stack_overlay": "s", "loadgen_overlay": "l"}
    store = state.MemoryStore()
    return store, state.RunState.create(store, doc, now=now, revision="rev1")


def test_two_drivers_cannot_hold_one_run_until_the_first_has_been_quiet_for_five_minutes():
    clock = [1000.0]
    store, one = fresh_state(lambda: clock[0])
    one.acquire()
    two = state.RunState.load(store, now=lambda: clock[0], revision="rev2")
    with pytest.raises(state.LeaseHeld, match="wrote to this run 0 s ago"):
        two.acquire()
    two.acquire(takeover=True)                                         # the first was killed
    clock[0] += state.LEASE_S - 1
    one2 = state.RunState.load(store, now=lambda: clock[0])
    with pytest.raises(state.LeaseHeld):
        one2.acquire()
    clock[0] += 2
    one2.acquire()                                                     # two has been quiet for the whole lease: it is gone


def test_a_driver_that_ends_hands_the_run_back_so_the_next_starts_at_once():
    store, one = fresh_state(lambda: 1000.0)
    one.acquire()
    one.release()
    state.RunState.load(store, now=lambda: 1000.0).acquire()


def test_only_the_holder_can_release_the_lease():
    store, one = fresh_state(lambda: 1000.0)
    one.acquire()
    two = state.RunState.load(store, now=lambda: 1000.0)
    two.release()
    assert json.loads(store.data["lease.json"])["owner"] == one.token


def test_the_lease_is_held_while_blocks_run_and_released_after_a_failure_and_after_sigterm(tmp_path):
    store = state.MemoryStore()
    seen = []

    class Spy(FakeCluster):
        def factory(self):
            base = super().factory()

            class Backend(base):
                def run_storm(inner, label, env):
                    seen.append(json.loads(store.data["lease.json"])["owner"])
                    if label == "main-b1-idle":
                        raise SystemExit(143)                          # what the SIGTERM handler raises
                    return super().run_storm(label, env)
            return Backend
    before = signal.getsignal(signal.SIGTERM)
    with pytest.raises(SystemExit):
        go(run_args(tmp_path), store, Spy())
    assert seen[0] != "" and json.loads(store.data["lease.json"])["owner"] == ""
    assert signal.getsignal(signal.SIGTERM) == before                  # the handler was put back


def test_resume_refuses_a_run_another_driver_is_holding(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    other = state.RunState.load(store)
    other.acquire()                                                    # someone else is driving
    again = FakeCluster()
    resume = ("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out"))
    with pytest.raises(state.LeaseHeld):
        go(parse(*resume), store, again)
    assert again.started == []                                          # refused before any broker was restarted
    assert go(parse(*resume, "--takeover"), store, again) == 0


# -- the ConfigMap ----------------------------------------------------------------------------------------------------------------------
class Scripted(Runner):
    def __init__(self, outputs):
        super().__init__()
        self.cmds, self.stdin, self.outputs = [], [], list(outputs)

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


NO_NET = subprocess.CalledProcessError(1, ["kubectl"])


def test_the_configmap_is_read_created_patched_and_deleted_through_the_benchmarks_kubeconfig(kubeconfig):
    run = Scripted(["", json.dumps({"data": {"plan.json": "{}"}}), "", "", ""])
    store = state.KubeStore(run, "local", sleep=lambda s: None)
    assert store.read() is None and store.read() == {"plan.json": "{}"}
    store.create({"plan.json": "{}"})
    store.patch({"lease.json": "{\"owner\": \"x\"}"})
    store.delete()
    assert all(c[:5] == ["kubectl", "--kubeconfig", kubeconfig, "-n", "storm-bench"] for c in run.cmds)
    assert run.cmds[0][5:] == ["get", "configmap", "storm-run", "-o", "json", "--ignore-not-found"]
    manifest = json.loads(run.stdin[2])
    assert run.cmds[2][5:] == ["create", "-f", "-"] and manifest["metadata"]["name"] == "storm-run" and manifest["data"] == {"plan.json": "{}"}
    assert run.cmds[3][5:8] == ["patch", "configmap", "storm-run"] and "merge" in run.cmds[3]
    assert json.loads(run.cmds[3][run.cmds[3].index("-p") + 1]) == {"data": {"lease.json": "{\"owner\": \"x\"}"}}    # only the key that changed
    assert run.cmds[4][5:] == ["delete", "configmap", "storm-run", "--ignore-not-found"]


def test_a_dropped_connection_is_retried_for_reads_and_writes_but_creating_is_not(kubeconfig, capsys):
    run = Scripted([NO_NET, NO_NET, json.dumps({"data": {"a": "1"}}), NO_NET, ""])
    sleeps = []
    store = state.KubeStore(run, "local", sleep=sleeps.append, wait_s=3.0)
    assert store.read() == {"a": "1"}
    store.patch({"a": "2"})
    assert sleeps == [3.0, 3.0, 3.0] and "reading the run state failed" in capsys.readouterr().err
    with pytest.raises(subprocess.CalledProcessError):
        state.KubeStore(Scripted([NO_NET, ""]), "local", sleep=sleeps.append).create({"a": "1"})


def test_a_read_that_keeps_failing_gives_up(kubeconfig):
    store = state.KubeStore(Scripted([NO_NET] * 10), "local", retries=3, sleep=lambda s: None)
    with pytest.raises(subprocess.CalledProcessError):
        store.read()


# -- what is recorded ------------------------------------------------------------------------------------------------------------------
def test_a_state_of_another_schema_or_a_damaged_one_is_refused_with_the_way_out():
    store, _ = fresh_state(lambda: 1.0)
    doc = json.loads(store.data["plan.json"])
    doc["schema"] = 99
    store.data["plan.json"] = json.dumps(doc)
    with pytest.raises(state.StateError, match="--discard-state"):
        state.RunState.load(store)
    store.data["progress.json"] = "{not json"
    store.data["plan.json"] = json.dumps({**doc, "schema": state.SCHEMA})
    with pytest.raises(state.StateError, match="damaged"):
        state.RunState.load(store)


def test_status_says_how_far_the_run_got_and_who_is_driving(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    text = state.format_status(state.RunState.load(store))
    assert "1 of 4 units done" in text and "running: fixed-b1" in text and "bench run --resume" in text
    assert "COMPLETE" not in text
    go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, FakeCluster())
    done = state.format_status(state.RunState.load(store))
    assert "4 of 4 units done  (COMPLETE)" in done and "resumed " in done


def test_status_reports_units_run_by_different_driver_revisions(tmp_path):
    store, rs = fresh_state(lambda: 1.0)
    rs.progress["units"] = {"a-b1": {"status": "done", "driver_revision": "aaa"}, "b-b1": {"status": "done", "driver_revision": "bbb+dirty"}}
    assert "2 different driver revisions: aaa, bbb+dirty" in state.format_status(rs)


def test_a_stale_driver_is_said_to_have_stopped():
    clock = [1000.0]
    store, rs = fresh_state(lambda: clock[0])
    rs.acquire()
    clock[0] += state.LEASE_S + 10
    assert "stale: it has stopped" in state.format_status(rs)


def test_status_and_the_commands_parse():
    assert parse("status").cmd == "status"
    a = parse("run", "--resume", "--takeover", "--accept-design-change")
    assert a.resume and a.takeover and a.accept_design_change and a.blocks is None
    b = parse("adopt", "results/x", "--redo", "main-b4", "--redo", "nolock-b2", "--reason", "broker restarted by a test")
    assert b.redo == ["main-b4", "nolock-b2"] and b.dir == "results/x"


def test_status_prints_nothing_recorded(capsys):
    assert runcmd.execute_status(Runner(), parse("status"), make_store=lambda t: state.MemoryStore()) == 0
    assert "no run is recorded in the local cluster" in capsys.readouterr().out


def test_the_driver_revision_names_a_commit_or_unknown():
    rev = state.driver_revision()
    assert rev == "unknown" or rev.split("+")[0].isalnum()


# -- adopting a run that has no state --------------------------------------------------------------------------------------------------
def finished_run(tmp_path, spoil: str | None = None):
    """A complete run, as the earlier driver left it: results and environment.json but nothing in the cluster."""
    assert go(run_args(tmp_path), state.MemoryStore(), FakeCluster()) == 0
    raw = tmp_path / "out" / "raw"
    if spoil:
        (raw / f"{spoil}.json").write_text(json.dumps({"run": "x", "runs": [json.loads(report_text(1600, 1, admitted=800).split(ab.REPORT_PREFIX)[1])["runs"][0]]}))
    return tmp_path / "out"


def adopt_args(out, *extra):
    return parse("adopt", str(out), *extra)


def test_adopt_records_the_intact_units_as_done_and_sets_the_named_one_aside(tmp_path, capsys):
    out = finished_run(tmp_path, spoil="main-b2-n800")
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out, "--redo", "main-b2", "--reason", "the broker was restarted under it"), make_store=lambda t: store) == 0
    said = capsys.readouterr().out
    assert "main-b2-n800: 800 of 1600 connections admitted (error: Connection refused (os error 111) x800)" in said
    rs = state.RunState.load(store)
    assert sorted(rs.progress["units"]) == ["fixed-b1", "fixed-b2", "main-b1"] and rs.done_units() == 3 and not rs.is_complete
    assert not (out / "raw" / "main-b2-n800.json").exists() and not (out / "raw" / "DONE").exists()
    moved = [p for p in (out / "discarded").glob("main-b2-*/*")]
    assert any(p.name == "main-b2-n800.json" for p in moved)                   # kept, not deleted
    env = json.loads((out / "environment.json").read_text())["adopted"]
    assert env["redo"][0]["unit"] == "main-b2" and env["redo"][0]["reason"] == "the broker was restarted under it"
    assert env["redo"][0]["problems"] == ["main-b2-n800: 800 of 1600 connections admitted (error: Connection refused (os error 111) x800)"]
    assert "run --resume" in said

    # and --resume runs exactly that unit, then the run is whole again
    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(out)), store, again) == 0
    assert again.started == ["main"] and again.stormed == ["main-b2-warm", "main-b2-idle", "main-b2-n800"]
    assert state.RunState.load(store).is_complete and (out / "raw" / "DONE").exists()
    summary = (out / "summary.txt").read_text()
    assert "blocks found: [1, 2]" in summary and "admitted 3200/3200" in summary    # main-b2's redone n800 counts again: 2 blocks x 2 runs x 800
    assert len(json.loads((out / "environment.json").read_text())["resumes"]) == 1


def test_adopt_can_pick_out_the_failed_units_itself(tmp_path, capsys):
    out = finished_run(tmp_path, spoil="fixed-b1-n800")
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out, "--redo-failed"), make_store=lambda t: store) == 0
    assert sorted(state.RunState.load(store).progress["units"]) == ["fixed-b2", "main-b1", "main-b2"]


def test_adopt_only_says_what_looks_wrong_when_not_asked_to_redo_it(tmp_path, capsys):
    out = finished_run(tmp_path, spoil="fixed-b1-n800")
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 0
    said = capsys.readouterr().out
    assert "name the unit with --redo to run it again" in said
    assert state.RunState.load(store).done_units() == 4                        # nothing was moved or redone on its own


def test_adopt_a_dry_run_changes_nothing(tmp_path, capsys):
    out = finished_run(tmp_path, spoil="main-b2-n800")
    assert runcmd.execute_adopt(Runner(dry_run=True), adopt_args(out, "--redo", "main-b2")) == 0
    assert (out / "raw" / "main-b2-n800.json").exists() and not (out / "discarded").exists() and (out / "raw" / "DONE").exists()


def test_adopt_refuses_a_run_that_may_still_be_going(tmp_path, capsys):
    out = finished_run(tmp_path)
    (out / "raw" / "DONE").unlink()
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 1
    assert store.data is None and "second driver on one run" in capsys.readouterr().out
    assert runcmd.execute_adopt(Runner(), adopt_args(out, "--force"), make_store=lambda t: store) == 0


def test_adopt_refuses_an_unknown_unit_and_another_unfinished_run(tmp_path):
    out = finished_run(tmp_path)
    with pytest.raises(ValueError, match="no such unit: main-b9"):
        runcmd.execute_adopt(Runner(), adopt_args(out, "--redo", "main-b9"), make_store=lambda t: state.MemoryStore())
    busy = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(parse("run", "--a", A, "--b", B, "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "other"), "--no-preflight"), busy,
           FakeCluster(fail_on="b-b1-warm"))
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: busy) == 1
    assert runcmd.execute_adopt(Runner(), adopt_args(out, "--discard-state"), make_store=lambda t: busy) == 0


def test_a_unit_with_missing_files_is_set_aside_and_redone_without_being_asked(tmp_path):
    out = finished_run(tmp_path)
    (out / "raw" / "fixed-b2-idle.json").unlink()
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 0
    assert sorted(state.RunState.load(store).progress["units"]) == ["fixed-b1", "main-b1", "main-b2"]
    assert any((out / "discarded").glob("fixed-b2-*/fixed-b2-warm.json"))


def test_unit_problems_name_the_reason_in_the_data(tmp_path):
    out = finished_run(tmp_path, spoil="main-b1-n800")
    plan = ab.Plan(variants=(ab.Variant("main", A, "x"), ab.Variant("fixed", B, "y")), blocks=2, sizes=(800,))
    u = next(u for u in ab.units(plan) if u.key == "main-b1")
    assert state.unit_problems(out / "raw", u) == ["main-b1-n800: 800 of 1600 connections admitted (error: Connection refused (os error 111) x800)"]
    assert state.unit_problems(out / "raw", next(u for u in ab.units(plan) if u.key == "fixed-b1")) == []


# -- the run id: the key between the state in the cluster and the results directory ---------------------------------------------------
def env_of(out):
    return json.loads((out / "environment.json").read_text())


def test_a_new_run_stamps_its_id_everywhere_the_state_and_the_results_must_agree(tmp_path):
    store = state.MemoryStore()
    assert go(run_args(tmp_path), store, FakeCluster()) == 0
    out = tmp_path / "out"
    rs = state.RunState.load(store)
    assert env_of(out)["run_id"] == rs.run_id and len(rs.run_id) == 12
    for unit in ("main-b1", "fixed-b1", "fixed-b2", "main-b2"):
        assert json.loads((out / "raw" / f"{unit}.meta.json").read_text())["run_id"] == rs.run_id
    first = (out / "summary.txt").read_text().splitlines()[0]
    assert f"run id {rs.run_id}" in first and "cluster kind" in first and "namespace storm-bench" in first and "started " in first
    assert env_of(out)["driver_revision"] == rs.plan["driver_revision"]


def test_the_chart_footer_carries_the_id(tmp_path):
    from stormbench import queue
    burst = {"connections": 100, "samples": [{"outcome": "admitted", "startMs": 0.0, "sentMs": 0.0, "doneMs": 10.0 * (k + 1)} for k in range(100)]}
    by_phase = {"n800": {"a": {1: [burst, burst]}, "b": {1: [burst, burst]}}}
    queue.write_charts(tmp_path, by_phase, ["a", "b"], "2026-11-21T09:00:00-08:00", run_id="abc123def456")
    assert "id abc123def456" in (tmp_path / "queue-n800.svg").read_text()


def test_resume_refuses_a_directory_that_belongs_to_another_run(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    env = env_of(tmp_path / "out")
    env["run_id"] = "someone-elses"
    (tmp_path / "out" / "environment.json").write_text(json.dumps(env))
    again = FakeCluster()
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again) == 1
    said = capsys.readouterr().out
    assert "belongs to a different run" in said and "someone-elses" in said and again.started == []


def test_a_directory_from_before_run_ids_is_accepted_and_gets_the_state_s_id(tmp_path, capsys):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    env = env_of(tmp_path / "out")
    del env["run_id"]                                                  # as an older driver would have left it
    (tmp_path / "out" / "environment.json").write_text(json.dumps(env))
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, FakeCluster()) == 0
    assert env_of(tmp_path / "out")["run_id"] == state.RunState.load(store).run_id
    assert "had no run id" in capsys.readouterr().out


def test_adopt_gives_a_run_without_an_id_one_and_keeps_one_it_has(tmp_path):
    out = finished_run(tmp_path)
    env = env_of(out)
    keep = env["run_id"]
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 0
    assert state.RunState.load(store).run_id == keep                     # it had one: the same key joins the new state
    del env["run_id"]
    (out / "environment.json").write_text(json.dumps(env))
    store2 = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store2) == 0
    fresh = state.RunState.load(store2).run_id
    assert env_of(out)["run_id"] == fresh and fresh != keep


# -- the variants file ------------------------------------------------------------------------------------------------------------------
VARIANTS = """# Why these two: a comment that the parsed record loses.
[[variant]]
label = "main"
image = "localhost/nats-server:main"
conf_file = "extra/main.conf"

[[variant]]
label = "fixed"
image = "localhost/nats-server:fixed"
"""


def variants_run(tmp_path, *extra, text=VARIANTS):
    src = tmp_path / "vars" / "my-variants.toml"
    (src.parent / "extra").mkdir(parents=True, exist_ok=True)
    src.write_text(text)
    (src.parent / "extra" / "main.conf").write_text("max_payload: 2MB\n")
    return src, parse("run", "--variants", str(src), "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "out"), "--no-preflight", *extra)


def test_the_variants_file_is_saved_verbatim_with_its_hash_and_its_conf_files(tmp_path):
    import hashlib
    src, args = variants_run(tmp_path)
    assert go(args, state.MemoryStore(), FakeCluster()) == 0
    out = tmp_path / "out"
    assert (out / "variants.toml").read_bytes() == src.read_bytes()                  # comments and all
    assert (out / "extra" / "main.conf").read_text() == "max_payload: 2MB\n"
    rec = env_of(out)["variants_file"]
    assert rec["sha256"] == hashlib.sha256(src.read_bytes()).hexdigest() and rec["conf_files"] == ["extra/main.conf"]
    assert rec["path"] == "my-variants.toml"                                          # outside the repository: the name only, no home directory
    assert len(__import__("stormbench").variants.load(out / "variants.toml", "localhost/")) == 2     # the copy loads by itself


def test_a_variants_file_inside_the_repository_is_recorded_by_its_repository_path(tmp_path):
    from stormbench.config import REPO_ROOT
    args = parse("run", "--variants", str(REPO_ROOT / "lock-variants.toml"), "--blocks", "1", "--sizes", "800", "--out", str(tmp_path / "out"),
                 "--no-preflight")
    assert go(args, state.MemoryStore(), FakeCluster()) == 0
    assert env_of(tmp_path / "out")["variants_file"]["path"] == "lock-variants.toml"


def test_a_conf_file_that_cannot_be_copied_safely_is_left_out_and_said(tmp_path):
    outside = VARIANTS.replace('"extra/main.conf"', '"../elsewhere.conf"')
    src, args = variants_run(tmp_path, text=outside)
    (tmp_path / "elsewhere.conf").write_text("x")                      # "../elsewhere.conf" from the variants file's directory
    assert go(args, state.MemoryStore(), FakeCluster()) == 0
    out = tmp_path / "out"
    assert not list(out.rglob("elsewhere.conf"))                        # a path that climbs out is refused, not copied in
    assert "was not copied" in env_of(out)["variants_file"]["notes"][0] and "conf_files" not in env_of(out)["variants_file"]


def test_a_run_from_two_commits_has_no_variants_file_to_save(tmp_path):
    assert go(run_args(tmp_path), state.MemoryStore(), FakeCluster()) == 0
    assert not (tmp_path / "out" / "variants.toml").exists() and "variants_file" not in env_of(tmp_path / "out")


def test_resume_leaves_the_saved_copy_alone(tmp_path):
    src, args = variants_run(tmp_path)
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(args, store, FakeCluster(fail_on="fixed-b1-warm"))
    before = (tmp_path / "out" / "variants.toml").read_bytes()
    src.write_text("# edited since\n" + VARIANTS)                                       # the original changes; the run goes on with the recorded plan
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, FakeCluster()) == 0
    assert (tmp_path / "out" / "variants.toml").read_bytes() == before


def test_adopt_can_attach_the_variants_file_and_will_not_overwrite_one(tmp_path):
    out = finished_run(tmp_path)
    src = tmp_path / "attached.toml"
    src.write_text("# the file that made the run\n")
    assert runcmd.execute_adopt(Runner(), adopt_args(out, "--variants-file", str(src)), make_store=lambda t: state.MemoryStore()) == 0
    assert (out / "variants.toml").read_text() == "# the file that made the run\n" and env_of(out)["variants_file"]["path"] == "attached.toml"
    with pytest.raises(ValueError, match="already has a variants.toml"):
        runcmd.execute_adopt(Runner(), adopt_args(out, "--variants-file", str(src)), make_store=lambda t: state.MemoryStore())


# -- when the driver gives up -----------------------------------------------------------------------------------------------------------
def test_a_driver_that_stops_says_the_run_is_recorded_and_how_to_continue(tmp_path, capsys):
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), state.MemoryStore(), FakeCluster(fail_on="fixed-b1-n800"))
    err = capsys.readouterr().err
    assert "the run is recorded in the cluster" in err and "bench --target local run --resume" in err


def test_the_retry_windows_are_ten_minutes():
    from stormbench import kube
    assert state.KubeStore(Runner(), "local").retries * state.KubeStore(Runner(), "local").wait_s == 600
    b = kube.KubeBackend(Runner(), read_cpu=False)
    assert b.read_retries * b.read_retry_wait_s == 600


# -- the namespace ----------------------------------------------------------------------------------------------------------------------
def test_the_plan_records_the_namespace_the_components_are_in(tmp_path):
    from stormbench import config
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    rs = state.RunState.load(store)
    assert rs.plan["namespace"] == config.NAMESPACE == "storm-bench"
    assert f"namespace {config.NAMESPACE}" in state.format_status(rs)


def test_adopt_records_it_too(tmp_path):
    from stormbench import config
    out = finished_run(tmp_path)
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 0
    assert state.RunState.load(store).plan["namespace"] == config.NAMESPACE


def test_resume_refuses_a_run_whose_components_are_in_another_namespace(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    doc = json.loads(store.data["plan.json"])
    doc["namespace"] = "somewhere-else"
    store.data["plan.json"] = json.dumps(doc)
    again = FakeCluster()
    with pytest.raises(ValueError, match="namespace 'somewhere-else', but this driver looks in 'storm-bench'"):
        go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, again)
    assert again.started == []


def test_a_state_from_before_the_namespace_was_recorded_still_resumes(tmp_path):
    store = state.MemoryStore()
    with pytest.raises(ab.StormFailed):
        go(run_args(tmp_path), store, FakeCluster(fail_on="fixed-b1-warm"))
    doc = json.loads(store.data["plan.json"])
    del doc["namespace"]                                                # as the run that was resumed on EKS has it
    store.data["plan.json"] = json.dumps(doc)
    assert "namespace" not in state.format_status(state.RunState.load(store))
    assert go(parse("run", "--resume", "--no-preflight", "--out", str(tmp_path / "out")), store, FakeCluster()) == 0


def test_the_namespace_is_defined_once_in_the_driver():
    import re
    from stormbench import config, kube, secretgen
    from stormbench.config import REPO_ROOT
    assert kube.NAMESPACE is config.NAMESPACE and secretgen.NAMESPACE is config.NAMESPACE and state.NAMESPACE is config.NAMESPACE
    for p in (REPO_ROOT / "driver" / "stormbench").glob("*.py"):
        assert not re.search(r'^NAMESPACE\s*=\s*"', p.read_text(), re.M) or p.name == "config.py", f"{p.name} defines its own namespace"


def test_the_manifests_say_the_same_namespace_as_the_driver():
    import re
    from stormbench import config
    from stormbench.config import REPO_ROOT
    found = {}
    for p in (REPO_ROOT / "deploy").rglob("*.yaml"):
        for m in re.finditer(r"namespace:\s*([A-Za-z0-9-]+)", p.read_text()):
            found.setdefault(m.group(1), []).append(p.name)
    assert found and set(found) == {config.NAMESPACE}, f"manifests use namespaces {found}, the driver uses {config.NAMESPACE}"
    ns_doc = (REPO_ROOT / "deploy" / "stack" / "base" / "namespace.yaml").read_text()
    assert f"name: {config.NAMESPACE}" in ns_doc                           # and the Namespace object itself is that one


def test_the_state_configmap_is_made_in_that_namespace(kubeconfig):
    from stormbench import config
    run = Scripted([""])
    state.KubeStore(run, "local").create({"plan.json": "{}"})
    assert json.loads(run.stdin[0])["metadata"]["namespace"] == config.NAMESPACE and "-n" in run.cmds[0] and config.NAMESPACE in run.cmds[0]
