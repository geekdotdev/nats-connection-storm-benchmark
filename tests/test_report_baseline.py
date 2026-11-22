# SPDX-License-Identifier: Apache-2.0
"""`bench report --baseline LABEL`: the same data, measured against another variant than the run's first."""
import json

import pytest

from stormbench import cli, report


def make_run(tmp_path, p50s=(("a", 100.0), ("b", 80.0), ("c", 80.0)), blocks=4):
    out = tmp_path / "run"
    (out / "raw").mkdir(parents=True)
    env = {"target": "eks", "cluster_kind": "eks", "namespace": "storm-bench", "run_id": "aaaabbbbcccc", "date": "2026-11-21T09:00:00-08:00",
           "variants": [{"label": l, "commit": "", "image": f"img-{l}"} for l, _ in p50s], "design": {"phases": ["warm", "idle", "n800"]}}
    (out / "environment.json").write_text(json.dumps(env))
    for label, p50 in p50s:
        for b in range(1, blocks + 1):
            (out / "raw" / f"{label}-b{b}.meta.json").write_text("{}")
            wobble = (b % 2) * 1.0                      # the blocks differ a little, so a test has something to be significant over
            burst = {"connections": 100, "seconds": 1.0, "outcomes": {"admitted": 100, "refused": 0, "timeout": 0, "error": 0},
                     "authMs": {"n": 100, "p50": p50 + wobble, "p90": p50 + 5, "p99": p50 + 9, "max": p50 + 10}}
            for phase in ("idle", "n800"):
                (out / "raw" / f"{label}-b{b}-{phase}.json").write_text(json.dumps({"runs": [burst, burst]}))
    return out


def changes(text, phase="n800"):
    """{variant: percent} of the 'change X vs Y in the mean of block medians' lines of a phase."""
    on, found = False, {}
    for line in text.splitlines():
        if line == phase:
            on = True
        elif line in ("idle", "warm", "n800") and line != phase:
            on = False
        elif on and line.strip().startswith("change ") and "in the mean of block medians" in line:
            parts = line.split()
            found[(parts[1], parts[3])] = float(parts[parts.index("medians:") + 1].rstrip("%"))
    return found


def test_by_default_every_change_is_against_the_runs_first_variant(tmp_path):
    got = changes(report.summarize(make_run(tmp_path)))
    assert set(got) == {("b", "a"), ("c", "a")} and got[("b", "a")] < -15 and got[("c", "a")] < -15


def test_another_baseline_flips_the_comparison_and_says_it_was_chosen(tmp_path):
    out = make_run(tmp_path)
    text = report.summarize(out, baseline="b")
    got = changes(text)
    assert set(got) == {("a", "b"), ("c", "b")} and got[("a", "b")] > 15 and abs(got[("c", "b")]) < 1     # a is slower than b; c and b are alike
    assert "baseline b (chosen with --baseline; the run's own baseline is a, and summary.txt is against that)" in text
    assert "2 comparisons with it" in text and "there are more comparisons than these" in text            # the other baselines count as comparisons too


def test_an_unknown_baseline_names_the_variants_there_are(tmp_path):
    with pytest.raises(ValueError, match=r"'zzz' is not a variant of this run \(it has: a, b, c\)"):
        report.summarize(make_run(tmp_path), baseline="zzz")


def test_the_command_prints_for_another_baseline_and_leaves_the_stored_summary_alone(tmp_path, capsys):
    out = make_run(tmp_path)
    assert cli.main(["report", str(out)]) == 0
    stored = (out / "summary.txt").read_text()
    capsys.readouterr()
    assert cli.main(["report", str(out), "--baseline", "c"]) == 0
    printed = capsys.readouterr().out
    assert "baseline c (chosen with --baseline" in printed and ("a", "c") in changes(printed)
    assert (out / "summary.txt").read_text() == stored and "chosen with --baseline" not in stored       # summary.txt is still against the run's own baseline
    assert cli.main(["report", str(out), "--baseline", "nope"]) == 2


def test_a_two_variant_run_can_be_turned_around_too(tmp_path):
    got = changes(report.summarize(make_run(tmp_path, (("a", 100.0), ("b", 80.0))), baseline="b"))
    assert set(got) == {("a", "b")} and got[("a", "b")] > 15


# -- where a connection's time goes: setup (total minus auth) ----------------------------------------------------------------------------------
def make_split_run(tmp_path, setups=(("a", 200.0), ("b", 400.0)), auth=100.0, blocks=4, with_total=True):
    out = tmp_path / "split"
    (out / "raw").mkdir(parents=True)
    env = {"target": "eks", "cluster_kind": "eks", "namespace": "storm-bench", "run_id": "aaaabbbbcccc", "date": "2026-11-21T09:00:00-08:00",
           "variants": [{"label": l, "commit": "", "image": f"img-{l}"} for l, _ in setups], "design": {"phases": ["warm", "idle", "n800"]}}
    (out / "environment.json").write_text(json.dumps(env))
    for label, setup in setups:
        for b in range(1, blocks + 1):
            (out / "raw" / f"{label}-b{b}.meta.json").write_text("{}")
            burst = {"connections": 100, "seconds": 1.0, "outcomes": {"admitted": 100, "refused": 0, "timeout": 0, "error": 0},
                     "authMs": {"n": 100, "p50": auth + (b % 2), "p90": auth + 5, "p99": auth + 9, "max": auth + 10}}
            if with_total:
                burst["totalMs"] = {"n": 100, "p50": auth + (b % 2) + setup, "p90": 1, "p99": 1, "max": 1}
            for phase in ("idle", "n800"):
                (out / "raw" / f"{label}-b{b}-{phase}.json").write_text(json.dumps({"runs": [burst, burst]}))
    return out


def test_the_report_splits_a_connections_time_into_setup_and_auth(tmp_path):
    text = report.summarize(make_split_run(tmp_path))
    assert "where a connection's time goes" in text
    assert "a        total p50  300.5 ms = setup  200.0 ms (67%) + auth  100.5 ms" in text          # 200 ms of setup and 100 ms of auth, 67% setup
    assert "b        total p50  500.5 ms = setup  400.0 ms (80%) + auth  100.5 ms" in text
    assert "change b vs a in setup: +100.0%" in text and "change b vs a in total p50: +66.6%" in text   # auth did not change: the whole difference is setup
    assert "change b vs a in the mean of block medians: +0.0%" in text


def test_a_run_from_before_total_was_recorded_has_no_split_and_is_not_broken(tmp_path):
    text = report.summarize(make_split_run(tmp_path, with_total=False))
    assert "where a connection's time goes" not in text and "change b vs a in the mean of block medians" in text


def test_the_split_is_for_bursts_not_for_the_idle_phase(tmp_path):
    text = report.summarize(make_split_run(tmp_path))
    idle = text.split("\nn800")[0]
    assert "where a connection's time goes" not in idle and text.count("where a connection's time goes") == 1
