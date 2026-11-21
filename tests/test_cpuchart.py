# SPDX-License-Identifier: Apache-2.0
"""CPU over time from the series Prometheus kept."""
import json
import xml.etree.ElementTree as ET

import pytest

from stormbench import cpuchart, report


def series(metric, comp, values, pod="p"):
    return {"metric": {"__name__": metric, "component": comp, "pod": pod, "container": comp}, "values": [[t, str(v)] for t, v in values]}


def export(folder, **metrics):
    folder.mkdir(parents=True, exist_ok=True)
    for name, result in metrics.items():
        (folder / f"{name}.json").write_text(json.dumps({"resultType": "matrix", "result": result}))
    return folder


def test_a_rate_is_the_increase_over_the_seconds_between_samples():
    assert cpuchart.increases([[10, "1.0"], [11, "1.5"], [13, "2.5"]]) == [(11.0, 0.5, 1.0), (13.0, 1.0, 2.0)]


def test_a_restarted_counter_counts_what_it_has_since_restarting_not_a_negative_reading():
    assert cpuchart.increases([[10, "4.9"], [11, "0.02"]]) == [(11.0, 0.02, 1.0)]


def test_cores_are_summed_over_the_series_of_a_component(tmp_path):
    f = export(tmp_path, storm_cpu_usage_seconds_total=[
        series("storm_cpu_usage_seconds_total", "loadgen", [(0, 0), (1, 0.5), (2, 1.0)], pod="g1"),
        series("storm_cpu_usage_seconds_total", "loadgen", [(0, 0), (1, 0.25), (2, 0.25)], pod="g2"),
        series("storm_cpu_usage_seconds_total", "broker", [(0, 5), (1, 6), (2, 6)])])
    got = cpuchart.cores(f)
    assert got["loadgen"] == [(1.0, 0.75), (2.0, 0.5)] and got["broker"] == [(1.0, 1.0), (2.0, 0.0)]


def test_throttled_share_is_throttled_periods_over_periods_and_absent_without_a_limit(tmp_path):
    f = export(tmp_path,
               storm_cpu_periods_total=[series("storm_cpu_periods_total", "callout", [(0, 0), (1, 10), (2, 20)]),
                                        series("storm_cpu_periods_total", "broker", [(0, 0), (1, 0)])],
               storm_cpu_throttled_periods_total=[series("storm_cpu_throttled_periods_total", "callout", [(0, 0), (1, 5), (2, 5)]),
                                                  series("storm_cpu_throttled_periods_total", "broker", [(0, 0), (1, 0)])])
    assert cpuchart.throttled_share(f) == {"callout": [(1.0, 0.5), (2.0, 0.0)]}      # the broker has no periods: it has no limit, so no share


def test_a_line_is_cut_where_the_component_was_absent():
    pts = [(0.0, 1.0), (1.0, 1.0), (2.0, 1.0), (30.0, 2.0), (31.0, 2.0)]
    assert [len(p) for p in cpuchart.segments(pts)] == [3, 2]
    assert cpuchart.segments([]) == []


def test_the_chart_is_valid_xml_labelled_and_shows_the_bursts(tmp_path):
    use = {"broker": [(100.0, 0.5), (101.0, 1.5)], "callout": [(100.0, 0.1), (101.0, 0.2)]}
    svg = cpuchart.chart(use, {"callout": [(100.0, 0.0), (101.0, 0.4)]}, [(100.2, 100.8)], run_date="d", generated="g", run_id="abc123def456", cluster="eks", namespace="storm-bench")
    root = ET.fromstring(svg)
    text = list(root.itertext())
    assert "cluster eks  ·  namespace storm-bench  ·  run id abc123def456" in text
    assert len(root.findall(".//{http://www.w3.org/2000/svg}polyline")) == 3                    # two components' cores and the callout's throttling
    assert len(root.findall(".//{http://www.w3.org/2000/svg}rect")) == 1 + 2                    # the background and the burst's column in each panel


def test_the_footer_and_axis_title_leave_room_for_each_other():
    svg = cpuchart.chart({"broker": [(0.0, 1.0), (60.0, 1.0)]}, {}, [], run_id="x", cluster="kind", namespace="n")
    ys = sorted(float(t.get("y")) for t in ET.fromstring(svg).iter("{http://www.w3.org/2000/svg}text")
                if "".join(t.itertext()).startswith(("seconds since", "cluster ", "run started", "chart generated")) or "".join(t.itertext()).startswith("cluster"))
    assert all(b - a >= 14 for a, b in zip(ys, ys[1:])), ys


def test_without_exported_series_there_is_no_chart_and_a_stale_one_is_removed(tmp_path):
    (tmp_path / "cpu.svg").write_text("old")
    assert cpuchart.write(tmp_path) is None and not (tmp_path / "cpu.svg").exists()


def test_the_bursts_that_recorded_when_they_ran_are_shaded(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "a-b1-n200.json").write_text(json.dumps({"runs": [{"startedAtMs": 1000, "endedAtMs": 1500}, {"samples": []}]}))
    (raw / "a-b1.meta.json").write_text("{}")
    assert cpuchart.burst_windows(tmp_path) == [(1.0, 1.5)]
