# SPDX-License-Identifier: Apache-2.0
"""Every output says which kind of cluster, which namespace, which run and when it started, so that nobody has to guess."""
import json
import re
import xml.etree.ElementTree as ET

import pytest

from stormbench import ab, cli, config, qualify, queue, report, runcmd, state
from stormbench.runner import Runner
from test_state import FakeCluster, adopt_args, env_of, finished_run, go, run_args  # helpers of the state tests

@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)   # run_blocks pauses 2 s after each storm


QUALIFIED = re.compile(r"cluster (kind|eks) · namespace (\S+) · run id ([0-9a-f]{12}) · started (\S+)")


# -- the words ---------------------------------------------------------------------------------------------------------------------------
def test_local_is_kind_and_eks_is_eks():
    assert qualify.cluster_kind("local") == "kind" and qualify.cluster_kind("eks") == "eks"
    assert qualify.cluster_kind(None) == "not recorded"


def test_a_value_that_was_not_recorded_says_so_instead_of_being_guessed():
    q = qualify.from_env({"target": "eks", "date": "2026-11-21T11:44:36-08:00"})        # a run from before namespaces and ids were written
    assert q == {"cluster": "eks", "namespace": "not recorded", "run_id": "not recorded", "started": "2026-11-21T11:44:36-08:00"}
    assert "namespace not recorded" in qualify.line(q)
    assert qualify.from_env({})["cluster"] == "not recorded"


# -- a new run -------------------------------------------------------------------------------------------------------------------------
def test_a_new_run_qualifies_every_output_it_writes(tmp_path):
    store = state.MemoryStore()
    assert go(run_args(tmp_path), store, FakeCluster()) == 0
    out = tmp_path / "out"
    rs = state.RunState.load(store)
    env = env_of(out)
    # environment.json: the kind is written out, not left as "local"
    assert env["target"] == "local" and env["cluster_kind"] == "kind" and env["namespace"] == config.NAMESPACE
    assert env["run_id"] == rs.run_id and env["date"] == rs.plan["started"]
    # the first line of the summary
    m = QUALIFIED.search((out / "summary.txt").read_text().splitlines()[0])
    assert m and m.groups() == ("kind", config.NAMESPACE, rs.run_id, env["date"])
    # every unit's meta, and every storm's own report, so that a single copied file is still labelled
    want = {"run_id": rs.run_id, "cluster": "kind", "target": "local", "namespace": config.NAMESPACE, "started": env["date"]}
    for unit in ("main-b1", "fixed-b1", "fixed-b2", "main-b2"):
        meta = json.loads((out / "raw" / f"{unit}.meta.json").read_text())
        assert meta["qualifiers"] == {**want, "unit": unit}
        for phase in ("warm", "idle", "n800"):
            rep = json.loads((out / "raw" / f"{unit}-{phase}.json").read_text())
            assert rep["qualifiers"] == {**want, "unit": unit, "storm": f"{unit}-{phase}"} and "runs" in rep   # the generator's own keys are untouched


def test_the_eks_target_is_labelled_eks(tmp_path):
    args = cli.build_parser().parse_args(["--target", "eks", "run", "--a", "a" * 40, "--b", "b" * 40, "--image-prefix", "ghcr.io/o/", "--blocks", "1",
                                          "--sizes", "800", "--out", str(tmp_path / "out"), "--no-preflight"])
    assert runcmd.execute(Runner(), args, make_store=lambda t: state.MemoryStore(), backend_factory=FakeCluster().factory()) == 0
    assert env_of(tmp_path / "out")["cluster_kind"] == "eks"
    assert " cluster eks · " in (tmp_path / "out" / "summary.txt").read_text().splitlines()[0]


def test_the_state_records_the_kind_the_namespace_and_the_real_start(tmp_path):
    store = state.MemoryStore()
    assert go(run_args(tmp_path), store, FakeCluster()) == 0
    plan = state.RunState.load(store).plan
    assert plan["cluster_kind"] == "kind" and plan["namespace"] == config.NAMESPACE and plan["started"] == env_of(tmp_path / "out")["date"]


def test_status_shows_the_run_id_next_to_the_start(tmp_path):
    store = state.MemoryStore()
    assert go(run_args(tmp_path), store, FakeCluster()) == 0
    rs = state.RunState.load(store)
    text = state.format_status(rs)
    assert f"run id {rs.run_id}" in text and f"namespace {config.NAMESPACE}" in text and rs.plan["started"] in text


# -- the charts -------------------------------------------------------------------------------------------------------------------------
def burst():
    return {"connections": 100, "samples": [{"outcome": "admitted", "startMs": 0.0, "sentMs": 0.0, "doneMs": 10.0 * (k + 1)} for k in range(100)]}


def test_the_chart_footer_says_where_and_when_on_two_lines(tmp_path):
    by_phase = {"n800": {"a": {1: [burst(), burst()]}, "b": {1: [burst(), burst()]}}}
    queue.write_charts(tmp_path, by_phase, ["a", "b"], "2026-11-21T09:00:00-08:00", run_id="abc123def456", cluster="kind", namespace="storm-bench")
    svg = (tmp_path / "queue-n800.svg").read_text()
    text = list(ET.fromstring(svg).itertext())
    assert "cluster kind  ·  namespace storm-bench  ·  run id abc123def456" in text
    assert any(t.startswith("run started 2026-11-21T09:00:00-08:00  ·  chart generated ") for t in text)
    height = int(svg.split('height="')[1].split('"')[0])
    curves = {"a": queue.pooled_curves([burst()], 1000.0), "b": queue.pooled_curves([burst()], 1000.0)}
    bare = queue.chart("n800", curves, {"a": 1, "b": 1}, "d", "g")
    assert height > int(bare.split('height="')[1].split('"')[0])                              # the second footer line made room for itself


def test_the_footer_lines_and_the_axis_title_do_not_overprint_each_other():
    curves = {"a": queue.pooled_curves([burst()], 1000.0), "b": queue.pooled_curves([burst()], 1000.0)}
    svg = queue.chart("n800", curves, {"a": 1, "b": 1}, "2026-11-21T09:00:00-08:00", "2026-11-21T10:00:00-08:00", None, "abc123def456", "kind", "storm-bench")
    rows = {}
    for t in ET.fromstring(svg).iter("{http://www.w3.org/2000/svg}text"):
        text = "".join(t.itertext())
        if text.startswith("milliseconds since") or text.startswith("cluster ") or text.startswith("run started"):
            rows[text.split("  ")[0].split(" (")[0]] = float(t.get("y"))
    ys = sorted(rows.values())
    assert len(ys) == 3 and all(b - a >= 14 for a, b in zip(ys, ys[1:])), rows               # three lines, each at least a line apart
    assert max(ys) < float(svg.split('height="')[1].split('"')[0])                              # and all inside the chart


def test_the_report_gives_the_summary_and_the_chart_the_runs_own_qualifiers(tmp_path):
    out = tmp_path / "run"
    (out / "raw").mkdir(parents=True)
    env = {"target": "eks", "cluster_kind": "eks", "namespace": "storm-bench", "run_id": "aaaabbbbcccc", "date": "2026-11-21T09:00:00-08:00",
           "variants": [{"label": "a", "commit": "", "image": "x"}, {"label": "b", "commit": "", "image": "y"}],
           "design": {"phases": ["warm", "idle", "n800"]}}
    (out / "environment.json").write_text(json.dumps(env))
    for v in ("a", "b"):
        (out / "raw" / f"{v}-b1.meta.json").write_text("{}")
        one = {**burst(), "seconds": 1.0, "outcomes": {"admitted": 100, "refused": 0, "timeout": 0, "error": 0},
               "authMs": {"n": 100, "p50": 1.0, "p90": 1.0, "p99": 1.0, "max": 1.0}}
        (out / "raw" / f"{v}-b1-n800.json").write_text(json.dumps({"run": "x", "runs": [one, one]}))
    text = report.write(out)
    assert text.splitlines()[0] == "run: cluster eks · namespace storm-bench · run id aaaabbbbcccc · started 2026-11-21T09:00:00-08:00"
    svg = (out / "queue-n800.svg").read_text()
    assert "cluster eks" in svg and "namespace storm-bench" in svg and "run id aaaabbbbcccc" in svg and "run started 2026-11-21T09:00:00-08:00" in svg


# -- results made before this --------------------------------------------------------------------------------------------------------------
def test_an_older_directory_is_reported_with_what_it_has_and_not_recorded_for_what_it_lacks(tmp_path):
    out = finished_run(tmp_path)
    env = env_of(out)
    for k in ("cluster_kind", "namespace", "run_id"):
        env.pop(k, None)
    (out / "environment.json").write_text(json.dumps(env))
    first = report.summarize(out).splitlines()[0]
    assert "cluster kind" in first and "namespace not recorded" in first and "run id not recorded" in first and f"started {env['date']}" in first


def test_adopt_fills_in_what_an_older_run_lacks_and_says_it_did(tmp_path):
    out = finished_run(tmp_path)
    env = env_of(out)
    for k in ("cluster_kind", "namespace"):
        env.pop(k)
    (out / "environment.json").write_text(json.dumps(env))
    store = state.MemoryStore()
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: store) == 0
    after = env_of(out)
    assert after["cluster_kind"] == "kind" and after["namespace"] == config.NAMESPACE
    assert after["adopted"]["qualifiers_added"] == ["cluster_kind", "namespace"]              # said, not passed off as written by the old driver
    assert state.RunState.load(store).plan["started"] == env["date"]                           # the run's own start, not the time it was adopted


def test_adopt_leaves_alone_what_the_run_already_recorded(tmp_path):
    out = finished_run(tmp_path)
    assert runcmd.execute_adopt(Runner(), adopt_args(out), make_store=lambda t: state.MemoryStore()) == 0
    assert "qualifiers_added" not in env_of(out)["adopted"]


def test_a_unit_finished_from_the_cluster_is_labelled_like_any_other(tmp_path):
    from test_jobs import PLAN, Cluster, report_text, stopped_unit
    store, rs, unit, attempt, raw = stopped_unit(tmp_path)
    cluster = Cluster(jobs={"main-b1-n800": (attempt, report_text(800))})
    ab.run_blocks(PLAN, cluster, raw, sleep=lambda s: None, now=lambda: "t", log=lambda m: None, progress=rs)
    rep = json.loads((raw / "main-b1-n800.json").read_text())
    assert rep["qualifiers"]["unit"] == "main-b1" and rep["qualifiers"]["storm"] == "main-b1-n800" and rep["qualifiers"]["cluster"] == "kind"
