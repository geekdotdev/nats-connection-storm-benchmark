# SPDX-License-Identifier: Apache-2.0
"""CPU over time, drawn from the series Prometheus kept (out/prometheus/*.json): the cores each component used every second, and the
share of its CFS periods in which it was throttled, with the bursts shaded so that the CPU reads against the load that caused it.

The probe exports counters; a rate is the difference between neighbouring samples over the time between them. A counter that goes
down (a container restarted: the broker is replaced for every variant's turn) contributes the value it restarted from, which is
the CPU it used since the restart, and is not a negative reading. Several series of one component (a pod replaced during the run,
one load generator per storm) are summed: only one of them runs at a time, so the sum is the component's use.
"""
from __future__ import annotations

import html
import json
import math
from pathlib import Path

from .queue import DARK, DASH, LIGHT, _fmt, _nice_step

COMPONENTS = ("broker", "callout", "loadgen")   # the slot of a component is its place here, so a component keeps its colour between charts
MAX_BURSTS_SHADED = 400
MAX_GAP_S = 3.0           # the scrape interval is 1 s: a longer silence is the component being absent, not a quiet second


def _load(folder: Path, metric: str) -> list[dict]:
    path = folder / f"{metric}.json"
    return json.loads(path.read_text())["result"] if path.exists() else []


def increases(values: list[list]) -> list[tuple[float, float, float]]:
    """(time, increase, seconds since the previous sample) for every sample after the first of one counter's series."""
    out = []
    for (t0, v0), (t1, v1) in zip(values, values[1:]):
        t0, t1, v0, v1 = float(t0), float(t1), float(v0), float(v1)
        if t1 > t0:
            out.append((t1, v1 - v0 if v1 >= v0 else v1, t1 - t0))   # a restart: what it counted since starting
    return out


def by_component(folder: Path, metric: str) -> dict[str, dict[float, tuple[float, float]]]:
    """component -> time -> (the increase of the counter, the seconds it covers), summed over the component's series."""
    out: dict[str, dict[float, tuple[float, float]]] = {}
    for series in _load(folder, metric):
        comp = series["metric"].get("component", "")
        into = out.setdefault(comp, {})
        for t, inc, dt_ in increases(series["values"]):
            a, b = into.get(t, (0.0, 0.0))
            into[t] = (a + inc, max(b, dt_))
    return out


def cores(folder: Path) -> dict[str, list[tuple[float, float]]]:
    """component -> [(time, cores used over the second before it)]."""
    return {c: sorted((t, inc / span) for t, (inc, span) in pts.items() if span > 0)
            for c, pts in by_component(folder, "storm_cpu_usage_seconds_total").items()}


def throttled_share(folder: Path) -> dict[str, list[tuple[float, float]]]:
    """component -> [(time, share of the CFS periods since the last sample in which the container was throttled)], only where it ran under a limit."""
    periods = by_component(folder, "storm_cpu_periods_total")
    out: dict[str, list[tuple[float, float]]] = {}
    for comp, pts in by_component(folder, "storm_cpu_throttled_periods_total").items():
        rows = [(t, th / periods[comp][t][0]) for t, (th, _) in pts.items() if periods.get(comp, {}).get(t, (0.0, 0.0))[0] > 0]
        if rows:
            out[comp] = sorted(rows)
    return out


def segments(points: list[tuple[float, float]], gap: float = MAX_GAP_S) -> list[list[tuple[float, float]]]:
    """The points cut wherever there is no sample for longer than `gap` seconds, so that a line is not drawn across the time a component
    was not there (the broker between variants, a load generator between storms) as if it had been measured."""
    out: list[list[tuple[float, float]]] = []
    for pt in points:
        if out and pt[0] - out[-1][-1][0] <= gap:
            out[-1].append(pt)
        else:
            out.append([pt])
    return out


def burst_windows(run_dir: Path) -> list[tuple[float, float]]:
    """(start, end) in epoch seconds of every burst that recorded when it ran."""
    out = []
    for path in sorted((run_dir / "raw").glob("*.json")):
        if path.name.endswith(".meta.json"):
            continue
        try:
            runs = json.loads(path.read_text()).get("runs", [])
        except (ValueError, OSError):
            continue
        out += [(b["startedAtMs"] / 1000, b["endedAtMs"] / 1000) for b in runs if isinstance(b, dict) and b.get("startedAtMs") and b.get("endedAtMs")]
    return sorted(out)[:MAX_BURSTS_SHADED]


def chart(use: dict[str, list[tuple[float, float]]], throttled: dict[str, list[tuple[float, float]]], bursts: list[tuple[float, float]],
          *, run_date: str = "", generated: str = "", run_id: str = "", cluster: str = "", namespace: str = "") -> str:
    comps = [c for c in COMPONENTS if c in use] + [c for c in use if c not in COMPONENTS]
    t0 = min(t for pts in use.values() for t, _ in pts)
    t1 = max(t for pts in use.values() for t, _ in pts)
    span = max(t1 - t0, 1.0)
    W, ML, MR, PH, GAP, TOP = 780, 62, 24, 170, 54, 112
    H = TOP + 2 * PH + GAP + 82
    xstep = _nice_step(span, 8)
    xend = math.ceil(span / xstep) * xstep

    def sx(t: float) -> float:
        return ML + (W - ML - MR) * (t - t0) / xend

    css = "".join(f"--s{i}:{LIGHT[i]};" for i in range(len(LIGHT)))
    cssd = "".join(f"--s{i}:{DARK[i]};" for i in range(len(DARK)))
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d" font-family="system-ui, sans-serif" font-size="12">',
         '<title id="t">CPU use over the run, from Prometheus</title>',
         '<desc id="d">The cores each component used, second by second, and the share of its CPU scheduling periods in which it was throttled, with the bursts shaded.</desc>',
         f'<style>:root{{color-scheme:light dark;--bg:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--grid:#e3e2dd;--band:#9a9992;{css}}}'
         f'@media (prefers-color-scheme:dark){{:root{{--bg:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#34332f;--band:#8a897f;{cssd}}}}}'
         '.t{fill:var(--ink)}.t2{fill:var(--ink2)}.g{stroke:var(--grid);stroke-width:1}.ax{stroke:var(--ink2);stroke-width:1}</style>',
         '<rect width="100%" height="100%" fill="var(--bg)"/>',
         f'<text class="t" x="{ML}" y="24" font-size="15" font-weight="600">CPU over the run</text>',
         f'<text class="t2" x="{ML}" y="44">Read from the probes once a second and kept in Prometheus. The shaded columns are the bursts.</text>',
         f'<text class="t2" x="{ML}" y="60">Throttling is the share of 100 ms scheduling periods in which a container had used its limit and was stopped until the next.</text>']
    lx = ML
    for i, c in enumerate(comps):
        o.append(f'<line x1="{lx}" y1="77" x2="{lx + 22}" y2="77" stroke="var(--s{i})" stroke-width="2"' + (f' stroke-dasharray="{DASH[i]}"' if DASH[i] else "") + "/>")
        o.append(f'<text class="t2" x="{lx + 28}" y="81">{html.escape(c)}</text>')
        lx += 28 + 8 * len(c) + 24

    def panel(top: int, series: dict[str, list[tuple[float, float]]], ymax: float, ytitle: str, pct: bool) -> None:
        ystep = _nice_step(ymax, 4)
        yend = math.ceil(ymax / ystep) * ystep

        def sy(v: float) -> float:
            return top + PH - PH * v / yend

        for lo, hi in bursts:
            a, b = max(sx(lo), ML), min(sx(hi), W - MR)
            if b > a:
                o.append(f'<rect x="{a:.1f}" y="{top}" width="{max(b - a, 1):.1f}" height="{PH}" fill="var(--band)" fill-opacity="0.18"/>')
        v = 0.0
        while v <= yend + 1e-9:
            o.append(f'<line class="g" x1="{ML}" y1="{sy(v):.1f}" x2="{W - MR}" y2="{sy(v):.1f}"/>')
            o.append(f'<text class="t2" x="{ML - 8}" y="{sy(v) + 4:.1f}" text-anchor="end">{_fmt(round(v * 100, 6)) + "%" if pct else _fmt(round(v, 6))}</text>')
            v += ystep
        t = 0.0
        while t <= xend + 1e-9:
            o.append(f'<text class="t2" x="{sx(t0 + t):.1f}" y="{top + PH + 16}" text-anchor="middle">{_fmt(t)}</text>')
            t += xstep
        o.append(f'<line class="ax" x1="{ML}" y1="{top + PH}" x2="{W - MR}" y2="{top + PH}"/>')
        o.append(f'<text class="t2" x="{ML}" y="{top - 12}">{html.escape(ytitle)}</text>')
        for i, c in enumerate(comps):
            for part in segments(series.get(c, [])):
                line = " ".join(f"{sx(t):.1f},{sy(min(v, yend)):.1f}" for t, v in part)
                o.append(f'<polyline points="{line}" fill="none" stroke="var(--s{i})" stroke-width="1.5" stroke-linejoin="round"' + (f' stroke-dasharray="{DASH[i]}"' if DASH[i] else "") + f'><title>{html.escape(c)}</title></polyline>')

    panel(TOP, use, max((v for pts in use.values() for _, v in pts), default=1.0) or 1.0, "cores used (CPU seconds per second)", False)
    panel(TOP + PH + GAP, throttled, 1.0, "share of scheduling periods throttled", True)
    if not throttled:
        o.append(f'<text class="t2" x="{ML + 8}" y="{TOP + PH + GAP + 20}">no container reported a CPU limit, so nothing could be throttled</text>')
    o.append(f'<text class="t2" x="{(ML + W - MR) / 2:.0f}" y="{H - 46}" text-anchor="middle">seconds since the first sample</text>')
    where = [f"cluster {cluster}" if cluster else "", f"namespace {namespace}" if namespace else "", f"run id {run_id}" if run_id else ""]
    stamps = [f"run started {run_date}" if run_date else "", f"chart generated {generated}" if generated else ""]
    if any(where):
        o.append(f'<text class="t2" x="{ML}" y="{H - 24}" font-size="11">{html.escape("  ·  ".join(x for x in where if x))}</text>')
    if any(stamps):
        o.append(f'<text class="t2" x="{ML}" y="{H - 8}" font-size="11">{html.escape("  ·  ".join(x for x in stamps if x))}</text>')
    o.append("</svg>")
    return "\n".join(o) + "\n"


def write(run_dir: Path, *, run_date: str = "", generated: str = "", run_id: str = "", cluster: str = "", namespace: str = "") -> Path | None:
    """cpu.svg, when the run exported Prometheus series that have samples; None otherwise (and any stale cpu.svg is removed)."""
    target = run_dir / "cpu.svg"
    target.unlink(missing_ok=True)
    folder = run_dir / "prometheus"
    use = cores(folder) if folder.is_dir() else {}
    if not use:
        return None
    target.write_text(chart(use, throttled_share(folder), burst_windows(run_dir), run_date=run_date, generated=generated, run_id=run_id,
                            cluster=cluster, namespace=namespace))
    return target
