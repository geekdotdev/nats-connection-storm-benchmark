# SPDX-License-Identifier: Apache-2.0
"""The queue curves: derived from per-connection timings, checked against bursts whose shape is known by construction."""
import json
import xml.etree.ElementTree as ET

import pytest

from stormbench import ab, queue, report


def sample(sent, done, outcome="admitted", start=0.0):
    return {"outcome": outcome, "startMs": start, "sentMs": sent, "doneMs": done}


def fifo_burst(n=100, service_ms=10.0, arrive_ms=0.0):
    """n connections whose CONNECT goes out at arrive_ms and that are answered one after another, service_ms apart: a queue that
    holds n at the start and drains at 1000 / service_ms per second."""
    return {"connections": n, "samples": [sample(arrive_ms, arrive_ms + service_ms * (k + 1)) for k in range(n)]}


def test_a_fifo_queue_peaks_at_the_burst_size_and_drains_at_the_service_rate():
    f = queue.features(fifo_burst(100, 10.0))
    assert f["peak_depth"] == 100 and f["peak_at_ms"] == 0.0
    assert (f["t10_ms"], f["t50_ms"], f["t90_ms"], f["t100_ms"]) == (100.0, 500.0, 900.0, 1000.0)
    assert f["drain_per_s"] == pytest.approx(100.0)       # 80 connections in 0.8 s
    assert f["sent90_ms"] == 0.0 and f["admitted"] == 100


def test_arrivals_that_are_spread_out_make_a_shallow_queue():
    # one CONNECT every 10 ms, each answered 5 ms later: the broker keeps up, the queue never holds more than one
    burst = {"connections": 50, "samples": [sample(10.0 * k, 10.0 * k + 5.0) for k in range(50)]}
    f = queue.features(burst)
    assert f["peak_depth"] == 1
    assert f["sent90_ms"] == 440.0                         # arrival-limited: the last CONNECTs go out late
    assert f["t100_ms"] == 495.0


def test_waiting_is_sends_minus_answers_at_each_time():
    s = [sample(0.0, 30.0), sample(0.0, 60.0), sample(20.0, 90.0)]
    assert queue.waiting_at(s, [0.0, 10.0, 20.0, 30.0, 60.0, 90.0, 100.0]) == [2, 2, 3, 2, 1, 0, 0]
    assert queue.admitted_share_at(s, 3, [0.0, 30.0, 60.0, 90.0]) == [0.0, 1 / 3, 2 / 3, 1.0]


def test_a_failed_connection_leaves_the_queue_but_is_not_admitted():
    samples = [sample(0.0, 10.0 * (k + 1)) for k in range(18)] + [sample(0.0, 500.0, "timeout"), sample(0.0, 500.0, "refused")]
    f = queue.features({"connections": 20, "samples": samples})
    assert f["admitted"] == 18
    assert f["t90_ms"] == 180.0 and f["t100_ms"] is None   # 100 % was never reached
    assert queue.waiting_at(samples, [499.0, 500.0]) == [2, 0]


def test_a_connection_that_never_sent_connect_is_not_in_the_queue():
    samples = [sample(0.0, 10.0 * (k + 1)) for k in range(9)] + [{"outcome": "error", "startMs": 0.0, "sentMs": None, "doneMs": 3000.0}]
    f = queue.features({"connections": 10, "samples": samples})
    assert f["peak_depth"] == 9 and f["t100_ms"] is None
    assert queue.waiting_at(samples, [1000.0]) == [0]


def test_a_burst_without_samples_has_no_curve_and_does_not_crash():
    assert queue.features({"connections": 800}) is None
    assert queue.features({"connections": 800, "samples": []}) is None
    assert queue.features({"connections": 1, "samples": [sample(0, 2)]}) is None   # the idle phase: one at a time is not a queue
    assert queue.pooled_curves([{"connections": 800}], 1000.0) is None
    assert queue.summarize({"a": {1: [{"connections": 800}]}, "b": {1: [{"connections": 800}]}}, ["a", "b"], report.perm_p) == []


def test_the_pooled_curve_takes_the_median_and_the_band_across_bursts():
    bursts = [fifo_burst(100, s) for s in (5.0, 10.0, 20.0)]
    c = queue.pooled_curves(bursts, 2000.0)
    assert c["bursts"] == 3 and c["connections"] == 100
    t = c["times"]
    i = min(range(len(t)), key=lambda j: abs(t[j] - 400.0))
    # at ~400 ms the three queues hold about 20, 60 and 80 connections: the middle one is the median
    assert c["depth"]["low"][i] <= c["depth"]["median"][i] <= c["depth"]["high"][i]
    assert c["depth"]["median"][0] == 100 and c["depth"]["median"][-1] == 0
    assert c["share"]["median"][0] == 0.0 and c["share"]["median"][-1] == 1.0
    assert all(a <= b + 1e-9 for a, b in zip(c["share"]["median"], c["share"]["median"][1:]))   # a drain curve never goes back


def test_the_block_is_the_unit_of_replication():
    # block 1 drains slowly, block 2 fast: the feature of a block is the median over its bursts, and the test sees two values
    slow = {1: [fifo_burst(100, 20.0)] * 3, 2: [fifo_burst(100, 20.0)] * 3}
    fast = {1: [fifo_burst(100, 5.0)] * 3, 2: [fifo_burst(100, 5.0)] * 3}
    lines = queue.summarize({"slow": slow, "fast": fast}, ["slow", "fast"], report.perm_p)
    text = "\n".join(lines)
    assert "peak queue (connections)" in text and "drain rate 10-90%" in text
    assert "90% admitted by (ms): fast vs slow -75.0%" in text and "(n=2 v 2 blocks)" in text
    assert "drain rate 10-90% (connections/s): fast vs slow +300.0%" in text


# -- the chart ---------------------------------------------------------------------------------------------------------
def curves_for(*labels):
    out = {}
    for i, label in enumerate(labels):
        out[label] = queue.pooled_curves([fifo_burst(100, 10.0 + 5 * i), fifo_burst(100, 12.0 + 5 * i)], 2000.0)
    return out


def test_the_chart_is_well_formed_svg_with_a_legend_and_direct_labels():
    svg = queue.chart("n800", curves_for("single", "cluster"), {"single": 8, "cluster": 8})
    root = ET.fromstring(svg)                             # parses: no stray markup
    text = " ".join(root.itertext())
    assert text.count("single (2 bursts, 8 blocks)") == 1 and "cluster (2 bursts, 8 blocks)" in text   # the legend
    assert 1 <= svg.count(">single</text>") <= 2 and 1 <= svg.count(">cluster</text>") <= 2           # direct labels, where there is room for them
    assert "connections waiting for an answer" in text and "share of the burst admitted" in text
    assert "prefers-color-scheme:dark" in svg and "fill=\"var(--bg)\"" in svg                        # a dark theme and its own surface
    assert svg.count("<polyline") == 4 and svg.count("<polygon") == 4                               # a line and a band per variant per panel


def test_the_chart_is_stamped_with_the_run_date_and_the_time_it_was_drawn():
    svg = queue.chart("n800", curves_for("single", "cluster"), {"single": 8, "cluster": 8}, "2026-11-21T09:18:15-08:00", "2026-11-21T11:50:02-08:00")
    ET.fromstring(svg)
    assert "run started 2026-11-21T09:18:15-08:00" in svg and "chart generated 2026-11-21T11:50:02-08:00" in svg
    bare = queue.chart("n800", curves_for("single", "cluster"), {"single": 8, "cluster": 8})
    assert "chart generated" not in bare and "run started" not in bare              # nothing is invented when no stamp is given


def test_the_legend_wraps_instead_of_running_off_the_chart_and_the_chart_grows_to_fit():
    labels = ["main", "nolock", "mux_main", "mux_nolock"]
    four = queue.chart("n800", curves_for(*labels), {l: 8 for l in labels})
    two = queue.chart("n800", curves_for("main", "nolock"), {"main": 8, "nolock": 8})
    text = " ".join(ET.fromstring(four).itertext())
    assert all(f"{l} (2 bursts, 8 blocks)" in text for l in labels)                    # none is cut off at the right edge
    height = lambda svg: int(svg.split('height="')[1].split('"')[0])
    assert height(four) > height(two)                                                  # a second legend row made room for itself


def test_a_label_with_no_room_inside_the_panel_is_left_to_the_legend():
    labels = ["main", "nolock", "mux_main", "mux_nolock"]
    svg = queue.chart("n800", curves_for(*labels), {l: 8 for l in labels})
    for l in labels:
        assert svg.count(f">{l}<") <= 2 + 1 and f"{l} (2 bursts" in svg                # never more than a label per panel and the legend


def test_one_stalled_burst_does_not_squeeze_every_curve_into_the_left_of_the_chart(tmp_path):
    normal = [fifo_burst(100, 10.0) for _ in range(19)]                               # each ends at 1000 ms
    stalled = [fifo_burst(100, 30.0)]                                                  # ends at 3000 ms
    by_phase = {"n800": {"a": {1: normal + stalled}, "b": {1: [fifo_burst(100, 12.0) for _ in range(20)]}}}
    queue.write_charts(tmp_path, by_phase, ["a", "b"], "2026-11-21T09:00:00-08:00")
    svg = (tmp_path / "queue-n800.svg").read_text()
    text = " ".join(ET.fromstring(svg).itertext())
    assert "1 of 40 bursts ran longer and are cut off" in text
    end = float(text.split("the axis ends at ")[1].split(" ms")[0])
    assert 1000 < end < 3000                                                           # it runs past the usual burst, not to the stall


def test_when_no_burst_is_cut_off_the_chart_does_not_mention_it(tmp_path):
    by_phase = {"n800": {"a": {1: [fifo_burst(100, 10.0) for _ in range(5)]}, "b": {1: [fifo_burst(100, 12.0) for _ in range(5)]}}}
    queue.write_charts(tmp_path, by_phase, ["a", "b"])
    assert "cut off" not in (tmp_path / "queue-n800.svg").read_text()


def test_variant_labels_cannot_inject_markup_into_the_chart():
    svg = queue.chart("n800", curves_for("a<b>", "c&d"), {"a<b>": 1, "c&d": 1})
    ET.fromstring(svg)
    assert "a<b>" not in svg and "a&lt;b&gt;" in svg


def test_the_chart_never_draws_more_lines_than_the_palette_has_colours():
    labels = [f"v{i}" for i in range(10)]
    svg = queue.chart("n800", curves_for(*labels), {l: 1 for l in labels})
    assert svg.count("<polyline") == 2 * queue.MAX_SERIES


# -- the plan and the report ---------------------------------------------------------------------------------------------
def test_samples_are_asked_for_in_the_burst_phases_but_not_in_warm_up():
    plan = ab.Plan(variants=(ab.Variant("a", "a" * 40, "x"), ab.Variant("b", "b" * 40, "y")), blocks=1, sizes=(800,))
    envs = {ph.name: plan.storm_env(ph, "l") for ph in plan.phases}
    assert envs["n800"]["STORM_SAMPLES"] == "1" and envs["idle"]["STORM_SAMPLES"] == "1"
    assert "STORM_SAMPLES" not in envs["warm"]


def write_run(tmp_path, with_samples=True):
    raw = tmp_path / "raw"
    raw.mkdir()
    (tmp_path / "environment.json").write_text(json.dumps({"variants": [{"label": "single", "commit": "a" * 40, "image": "x"}, {"label": "cluster", "commit": "b" * 40, "image": "y"}],
                                                         "date": "2026-11-21T09:00:00-08:00", "design": {"phases": ["warm", "idle", "n800"]}}))
    for blk in (1, 2):
        for label, service in (("single", 10.0), ("cluster", 6.0)):
            (raw / f"{label}-b{blk}.meta.json").write_text("{}")
            bursts = []
            for _ in range(3):
                b = fifo_burst(100, service)
                b |= {"seconds": 1.0, "outcomes": {"admitted": 100, "refused": 0, "timeout": 0, "error": 0},
                      "authMs": {"n": 100, "p50": service * 50, "p90": 1, "p99": 1, "max": 1}}
                if not with_samples:
                    del b["samples"]
                bursts.append(b)
            (raw / f"{label}-b{blk}-n800.json").write_text(json.dumps({"run": "x", "runs": bursts}))
    return tmp_path


def test_the_report_adds_the_queue_and_writes_the_chart(tmp_path):
    text = report.write(write_run(tmp_path))
    assert "the queue (CONNECT sent, no answer yet)" in text and "peak queue (connections)" in text
    assert (tmp_path / "summary.txt").read_text() == text + "\n"
    svg = tmp_path / "queue-n800.svg"
    ET.fromstring(svg.read_text())
    assert not (tmp_path / "queue-idle.svg").exists()                         # one connection at a time is not a queue
    stamped = svg.read_text()
    assert "run started 2026-11-21T09:00:00-08:00" in stamped and "chart generated 20" in stamped


def test_results_without_samples_still_report_and_say_why(tmp_path):
    text = report.write(write_run(tmp_path, with_samples=False))
    assert "no per-connection samples" in text and "median auth p50 per block" in text
    assert not list(tmp_path.glob("queue-*.svg"))


def test_a_chart_from_an_earlier_report_does_not_outlive_its_data(tmp_path):
    run = write_run(tmp_path)
    report.write(run)
    assert (run / "queue-n800.svg").exists()
    for p in (run / "raw").glob("*-n800.json"):
        d = json.loads(p.read_text())
        for b in d["runs"]:
            del b["samples"]
        p.write_text(json.dumps(d))
    report.write(run)
    assert not (run / "queue-n800.svg").exists()
