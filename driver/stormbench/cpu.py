# SPDX-License-Identifier: Apache-2.0
"""CPU use and throttling, per storm, for the broker, the callout and the generator.

Why: the queue curves show that connections wait, not where. Whether a component was near its CPU limit, or throttled by it, is the
first thing to know about contention, and nothing else in a result says it.

How it is read. The counters are the container's own cgroup (v2): `cpu.stat` has `usage_usec`, `nr_periods`, `nr_throttled` and
`throttled_usec`, and `cpu.max` the limit. They are cumulative, so the use during a storm is the difference between two readings.
- The broker's and the callout's are read by `kubectl exec ... cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/cpu.max` just before the
  storm's Job is created and just after it has finished (kube.py). That window is the whole storm: its bursts, the pauses between them
  and the closing of its connections. Dividing by the connections gives the CPU a connection cost; dividing by the bursts' own seconds
  gives how many cores were busy while they ran (a little over, since some work falls outside the bursts).
- The generator reads its own cgroup around each burst, in the process (components/loadgen), so each burst carries its own CPU use and
  throttling.

A reading that cannot be made (cgroup v1, a container without `cat`, a pod that restarted in between) leaves that component out of
that storm, and the report shows what it has. Existing results have none of this.
"""
from __future__ import annotations

import statistics as st

COUNTERS = ("usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec")


def parse(text: str) -> dict | None:
    """A container's cumulative CPU counters and limit from the text of `cpu.stat` followed by `cpu.max`; None if it is not cgroup v2
    (no usage_usec). `limit_cores` is None when there is no limit ("max")."""
    got: dict = {}
    limit = None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        key, val = parts
        if key in COUNTERS and val.isdigit():
            got[key] = int(val)
        elif key == "max" or (key.isdigit() and val.isdigit()):   # cpu.max is "<quota or max> <period>": the one line whose first word is not a name
            period = int(val) if val.isdigit() else 0
            if key != "max" and period > 0:
                limit = int(key) / period
    if "usage_usec" not in got:
        return None
    return {**{k: got.get(k, 0) for k in COUNTERS}, "limit_cores": limit}


def diff(before: dict | None, after: dict | None) -> dict | None:
    """The use between two readings of one container, or None when there is not a sound one: either reading missing, or a counter that
    went backwards, which means the container restarted and its counters began again."""
    if not before or not after:
        return None
    out = {k: after[k] - before[k] for k in COUNTERS}
    if any(v < 0 for v in out.values()):
        return None
    return {**out, "limit_cores": after.get("limit_cores")}


def snapshot_diff(before: dict, after: dict) -> dict:
    """{component: {pod: delta}} from two snapshots {component: {pod: reading}}; a pod or component with no sound difference is left out."""
    out: dict = {}
    for comp, pods in after.items():
        for pod, reading in pods.items():
            d = diff(before.get(comp, {}).get(pod), reading)
            if d:
                out.setdefault(comp, {})[pod] = d
    return out


# ---- the report ---------------------------------------------------------------------------------------------------------------------------

def _share(throttled: float, periods: float) -> float | None:
    return 100.0 * throttled / periods if periods else None


def _probe_stats(bursts: list[dict]) -> dict:
    """Per component, the median over the bursts of what the probes read for each burst (the generator asks them at the start and the end
    of every burst, so these are the burst's own CPU, not diluted by the pauses between bursts). The generator's own pod is left to its
    own cgroup reading."""
    per: dict[str, list[dict]] = {}
    for b in bursts:
        by_comp: dict[str, list[dict]] = {}
        for c in (b.get("probeCpu") or {}).get("containers", []):
            if c.get("component") and c["component"] != "loadgen":
                by_comp.setdefault(c["component"], []).append(c)
        for comp, cs in by_comp.items():
            usage = sum(c["usageUsec"] for c in cs) / 1e6
            limits = [c.get("limitCores") for c in cs]
            per.setdefault(comp, []).append({
                "ms_per_connection": 1000 * usage / b["connections"] if b.get("connections") else None,
                "cores_busy": usage / b["seconds"] if b.get("seconds") else None,
                "throttled_pct": _share(sum(c["throttledPeriods"] for c in cs), sum(c["periods"] for c in cs)),
                "throttled_s": sum(c["throttledUsec"] for c in cs) / 1e6,
                "limit_cores": sum(limits) if limits and all(l is not None for l in limits) else None})
    out: dict = {}
    for comp, rows in per.items():
        def med(key):
            vals = [r[key] for r in rows if r[key] is not None]
            return st.median(vals) if vals else None
        out[comp] = {k: med(k) for k in ("ms_per_connection", "cores_busy", "throttled_pct", "throttled_s")} | {"limit_cores": rows[0]["limit_cores"], "source": "probe"}
    return out


def storm_stats(storm: dict) -> dict:
    """What one storm's file says about CPU, per component, as numbers a report can average. `storm` is the stored report: the
    generator's own (one burst, or {"runs": [...]}) with the driver's `cpu` key for the broker and callout."""
    bursts = storm["runs"] if "runs" in storm else [storm]
    connections = sum(b.get("connections", 0) for b in bursts)
    burst_s = sum(b.get("seconds", 0.0) for b in bursts)
    out: dict = {}
    probed = _probe_stats(bursts)
    for comp, pods in (storm.get("cpu") or {}).get("components", {}).items():
        if comp in probed:
            continue   # the probes' per-burst readings are exact where this one is a whole storm: they win
        usage = sum(p["usage_usec"] for p in pods.values()) / 1e6
        limits = [p.get("limit_cores") for p in pods.values()]
        out[comp] = {"ms_per_connection": 1000 * usage / connections if connections else None,
                     "cores_busy": usage / burst_s if burst_s else None,
                     "throttled_pct": _share(sum(p["nr_throttled"] for p in pods.values()), sum(p["nr_periods"] for p in pods.values())),
                     "throttled_s": sum(p["throttled_usec"] for p in pods.values()) / 1e6,
                     "limit_cores": sum(limits) if limits and all(l is not None for l in limits) else None}
    out.update(probed)
    gen = [(b["cgroupCpu"], b) for b in bursts if b.get("cgroupCpu")]
    if gen:   # per burst: its own CPU over its own duration
        def per(f):
            vals = [f(c, b) for c, b in gen]
            vals = [v for v in vals if v is not None]
            return st.median(vals) if vals else None
        out["generator"] = {"ms_per_connection": per(lambda c, b: 1000 * c["usageUsec"] / 1e6 / b["connections"] if b.get("connections") else None),
                            "cores_busy": per(lambda c, b: c["usageUsec"] / 1e6 / b["seconds"] if b.get("seconds") else None),
                            "throttled_pct": per(lambda c, b: _share(c["throttledPeriods"], c["periods"])),
                            "throttled_s": per(lambda c, b: c["throttledUsec"] / 1e6),
                            "limit_cores": gen[0][0].get("limitCores")}
    return out


ROWS = (("ms_per_connection", "CPU ms per connection", "{:.2f}"), ("cores_busy", "cores busy while bursting", "{:.2f}"),
        ("throttled_pct", "periods throttled %", "{:.1f}"), ("throttled_s", "time throttled (s)", "{:.2f}"))
COMPONENTS = ("broker", "callout", "generator")


def summarize(storms: dict[str, dict[int, list[dict]]], labels: list[str]) -> list[str]:
    """Text lines for one phase. storms[label][block] is that block's stored storm reports. Empty when nothing has CPU data, so results
    made before it was recorded are reported as they always were. Each value is the mean over blocks of the block's median storm."""
    per: dict[str, dict[str, dict[str, list[float]]]] = {l: {} for l in labels}
    limits: dict[str, float | None] = {}
    for label in labels:
        for blk in storms[label].values():
            stats = [storm_stats(s) for s in blk]
            for comp in COMPONENTS:
                one = [s[comp] for s in stats if comp in s]
                if not one:
                    continue
                limits.setdefault(comp, one[0]["limit_cores"])
                for key, _, _ in ROWS:
                    vals = [o[key] for o in one if o[key] is not None]
                    if vals:
                        per[label].setdefault(comp, {}).setdefault(key, []).append(st.median(vals))
    if not any(per[l] for l in labels):
        return []
    width = max(max(len(l) for l in labels), 9)
    sources = {s for l in labels for blk in storms[l].values() for stm in blk for s in (v.get("source", "exec") for k, v in storm_stats(stm).items() if k != "generator")}
    how = ("each burst: the probes' readings for the broker and callout, the generator's own for itself" if sources == {"probe"} else
           "broker and callout: the whole storm, read from their cgroups by the driver; generator: each burst" if "probe" not in sources else
           "broker and callout: per burst from the probes where there are probes, else the whole storm; generator: each burst")
    out = [f"  CPU ({how}); mean over blocks of each block's median:",
           "    " + " " * 44 + "  ".join(f"{l:>{width}}" for l in labels)]
    for comp in COMPONENTS:
        if not any(comp in per[l] for l in labels):
            continue
        lim = limits.get(comp)
        for key, title, fmt in ROWS:
            cells = []
            for l in labels:
                v = per[l].get(comp, {}).get(key)
                cells.append(f"{fmt.format(st.mean(v)):>{width}}" if v else f"{'-':>{width}}")
            head = f"{comp} {title}" + (f" (limit {lim:g})" if key == "cores_busy" and lim else "")
            out.append(f"    {head:<44}" + "  ".join(cells))
    out.append("    (no p values: CPU is shown to read alongside the queue, not as another comparison)")
    return out
