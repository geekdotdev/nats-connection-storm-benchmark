# SPDX-License-Identifier: Apache-2.0
"""The shape of a burst: how a queue of connections builds and drains, from the generator's per-connection timings.

One number per burst (the median auth time) hides the story of a synchronized burst: the connections arrive together, the broker
and the callout answer them one after another at some rate, so the k-th connection waits about k divided by that rate. The
median is one point on that ramp, and it mixes how deep the queue got with how fast it drained. This module keeps the whole
curve.

From the samples (the generator's STORM_SAMPLES=1: for every connection, when it started, when its CONNECT went out and when
it was answered, all in milliseconds since the run began) it derives, over time:

  waiting(t)   connections whose CONNECT has been sent and that have no answer yet: the queue, as the client sees it
  admitted(t)  the share of the burst admitted by t, the drain curve

and, per burst: the peak queue depth and when it happens, the times by which 10, 50, 90 and 100 % of the connections were
admitted, the drain rate (connections per second between 10 and 90 %), and when 90 % of the CONNECTs were out (how long
the arrivals took: a burst is not a step, since opening a TLS WebSocket takes time too).

The client sees one queue, not two: waiting covers the broker's authorization and the callout's own queue and request round
trip. Splitting them needs timings from the callout; this does not do that.

Standard library only. Results made before the samples were recorded have none; every function here says so and returns
nothing for them instead of guessing.
"""
from __future__ import annotations

import bisect
import datetime as dt
import html
import math
import statistics as st
from pathlib import Path

MIN_BURST = 10            # below this many connections a burst is not a queue (the idle phase opens them one at a time)
FRACTIONS = (10, 50, 90, 100)
GRID_POINTS = 250         # points per drawn curve
BAND = (10, 90)           # percentiles across bursts, for the band around the median curve
MAX_SERIES = 8            # the palette's categorical slots: a ninth line would be a generated hue


def has_samples(burst: dict) -> bool:
    return bool(burst.get("samples")) and burst.get("connections", 0) >= MIN_BURST


def _answered(samples: list[dict]) -> list[dict]:
    return [s for s in samples if s.get("sentMs") is not None]


def waiting_at(samples: list[dict], times: list[float]) -> list[int]:
    """The queue depth at each time: CONNECTs sent by then minus answers in by then, counting only connections that sent one."""
    sent = _answered(samples)
    sent_at = sorted(s["sentMs"] for s in sent)
    done_at = sorted(s["doneMs"] for s in sent)
    return [bisect.bisect_right(sent_at, t) - bisect.bisect_right(done_at, t) for t in times]


def admitted_share_at(samples: list[dict], connections: int, times: list[float]) -> list[float]:
    done_at = sorted(s["doneMs"] for s in samples if s["outcome"] == "admitted")
    return [bisect.bisect_right(done_at, t) / connections for t in times]


def features(burst: dict) -> dict | None:
    """The curve summary of one burst, or None when it has no samples. Times are ms since the run began; a time is None when
    that share of the burst was never admitted."""
    if not has_samples(burst):
        return None
    samples, n = burst["samples"], burst["connections"]
    sent = _answered(samples)
    # Walk the sends and answers in time order; at a tie the send counts first, so a zero-length wait still shows as depth 1.
    events = sorted([(s["sentMs"], 1) for s in sent] + [(s["doneMs"], -1) for s in sent], key=lambda e: (e[0], -e[1]))
    depth = peak = 0
    peak_at = 0.0
    for t, d in events:
        depth += d
        if depth > peak:
            peak, peak_at = depth, t
    done = sorted(s["doneMs"] for s in samples if s["outcome"] == "admitted")

    def reached(pct: int) -> float | None:
        k = math.ceil(pct / 100 * n)
        return done[k - 1] if 1 <= k <= len(done) else None

    t = {pct: reached(pct) for pct in FRACTIONS}
    rate = None
    if t[10] is not None and t[90] is not None and t[90] > t[10]:
        rate = (math.ceil(0.9 * n) - math.ceil(0.1 * n)) / ((t[90] - t[10]) / 1000.0)
    sent_ms = sorted(s["sentMs"] for s in sent)
    k90 = math.ceil(0.9 * n)
    return {"connections": n, "peak_depth": peak, "peak_at_ms": peak_at,
            **{f"t{pct}_ms": t[pct] for pct in FRACTIONS},
            "drain_per_s": rate,
            "sent90_ms": sent_ms[k90 - 1] if len(sent_ms) >= k90 else None,
            "admitted": len(done)}


# ---- across bursts and blocks ----------------------------------------------------------------------------------------------------------------------------------------------------

FEATURES = (("peak_depth", "peak queue (connections)", "{:.0f}"), ("peak_at_ms", "peak at (ms)", "{:.0f}"),
            ("sent90_ms", "90% of CONNECTs sent by (ms)", "{:.0f}"),
            ("t10_ms", "10% admitted by (ms)", "{:.0f}"), ("t50_ms", "50% admitted by (ms)", "{:.0f}"),
            ("t90_ms", "90% admitted by (ms)", "{:.0f}"), ("t100_ms", "all admitted by (ms)", "{:.0f}"),
            ("drain_per_s", "drain rate 10-90% (connections/s)", "{:.0f}"))
COMPARED = ("peak_depth", "t90_ms", "drain_per_s")  # the p values are computed for these three only, to keep the comparisons few


def block_value(bursts: list[dict], name: str) -> float | None:
    """One block's value of a feature: the median over its bursts (the block is the unit of replication, as for the medians)."""
    vals = [f[name] for b in bursts if (f := features(b)) and f[name] is not None]
    return st.median(vals) if vals else None


def summarize(raw_bursts: dict[str, dict[int, list[dict]]], labels: list[str], perm_p) -> list[str]:
    """Text lines for one phase. raw_bursts[label][block] is that block's list of bursts. Empty when nothing has samples."""
    per_block: dict[str, dict[str, list[float]]] = {}
    for label in labels:
        per_block[label] = {name: [v for blk in raw_bursts[label].values() if (v := block_value(blk, name)) is not None] for name, _, _ in FEATURES}
    if not any(per_block[label]["peak_depth"] for label in labels):
        return []
    out = ["  the queue (CONNECT sent, no answer yet), per burst; mean over blocks of each block's median:"]
    width = max(len(label) for label in labels)
    out.append("    " + " " * 38 + "  ".join(f"{label:>{max(width, 9)}}" for label in labels))
    for name, title, fmt in FEATURES:
        cells = []
        for label in labels:
            v = per_block[label][name]
            cells.append(f"{fmt.format(st.mean(v)):>{max(width, 9)}}" if v else f"{'-':>{max(width, 9)}}")
        out.append(f"    {title:<38}" + "  ".join(cells))
    base = labels[0]
    for label in labels[1:]:
        for name in COMPARED:
            a, b = per_block[base][name], per_block[label][name]
            title = next(t for n, t, _ in FEATURES if n == name)
            if not a or not b or st.mean(a) == 0:
                continue
            out.append(f"    {title}: {label} vs {base} {100 * (st.mean(b) - st.mean(a)) / st.mean(a):+.1f}%   exact permutation p={perm_p(a, b):.5f}  (n={len(a)} v {len(b)} blocks)")
    out.append("    (three features are tested, so read each p as one comparison among three per variant; the curves are in queue-<phase>.svg)")
    return out


# ---- the pooled curves -----------------------------------------------------------------------------------------------------------------------------------------------------------

def _pct(sorted_vals: list[float], p: float) -> float:
    i = max(1, min(len(sorted_vals), math.ceil(p / 100 * len(sorted_vals)))) - 1
    return sorted_vals[i]


def pooled_curves(bursts: list[dict], xmax: float) -> dict | None:
    """The queue depth and admitted share on a common time grid, over every burst of a variant: the median across bursts at each
    time and the BAND percentiles around it. None when no burst has samples."""
    use = [b for b in bursts if has_samples(b)]
    if not use:
        return None
    times = [xmax * i / (GRID_POINTS - 1) for i in range(GRID_POINTS)]
    depth = [waiting_at(b["samples"], times) for b in use]
    share = [admitted_share_at(b["samples"], b["connections"], times) for b in use]

    def across(rows: list[list[float]]) -> dict:
        cols = [sorted(r[i] for r in rows) for i in range(GRID_POINTS)]
        return {"median": [st.median(c) for c in cols], "low": [_pct(c, BAND[0]) for c in cols], "high": [_pct(c, BAND[1]) for c in cols]}

    return {"times": times, "bursts": len(use), "connections": use[0]["connections"], "depth": across(depth), "share": across(share)}


def end_of(bursts: list[dict]) -> float:
    """When the last answer of any burst came in."""
    return max((s["doneMs"] for b in bursts if has_samples(b) for s in b["samples"]), default=0.0)


# ---- the chart: plain SVG, no dependencies ---------------------------------------------------------------------------------------------------------------------------

# The reference categorical palette (dataviz skill, references/palette.md), in slot order: a line takes its slot by the variant's position.
LIGHT = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948")
DARK = ("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c", "#9085e9", "#e66767")
DASH = ("", "7 4", "2 3", "10 3 2 3", "", "7 4", "2 3", "10 3 2 3")  # a second cue beside colour, for colour-vision differences and print


def _nice_step(span: float, ticks: int) -> float:
    raw = span / max(ticks, 1)
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for m in (1, 2, 2.5, 5, 10):
        if m * mag >= raw:
            return m * mag
    return 10 * mag


def _fmt(v: float) -> str:
    return f"{v:g}"


def chart(phase: str, curves: dict[str, dict], blocks: dict[str, int], run_date: str = "", generated: str = "",
          clipped: tuple[int, int] | None = None, run_id: str = "", cluster: str = "", namespace: str = "") -> str:
    """One SVG for a phase: the queue depth above and the admitted share below, one line per variant with its band. curves maps a
    variant label to pooled_curves(); blocks says how many blocks each variant ran. The footer carries run_date (when the run
    was made, from its environment.json) and generated (when this chart was drawn: the same data can be drawn again later).
    `clipped` is (bursts that ran past the end of the axis, bursts): the axis is cut short of the slowest bursts so that one stall does
    not squeeze every curve into the left of the chart, and the chart says so."""
    labels = list(curves)[:MAX_SERIES]
    xmax = max(c["times"][-1] for c in curves.values())
    ymax_depth = max(c["depth"]["high"][i] for c in curves.values() for i in range(GRID_POINTS)) or 1
    W, ML, MR, PH, GAP = 780, 62, 24, 190, 54
    # the legend, in as many rows as it takes to stay inside the chart
    legend, lx, row = [], ML, 0
    for i, label in enumerate(labels):
        text = f"{label} ({curves[label]['bursts']} bursts, {blocks.get(label, '?')} blocks)"
        width = 28 + 7 * len(text) + 18
        if lx + width > W - MR and lx > ML:
            lx, row = ML, row + 1
        legend.append((i, text, lx, row))
        lx += width
    TOP = 112 + 18 * row
    H = TOP + 2 * PH + GAP + 66 + (16 if cluster or namespace else 0)
    xstep = _nice_step(xmax, 8)
    xend = math.ceil(xmax / xstep) * xstep

    def sx(t: float) -> float:
        return ML + (W - ML - MR) * t / xend

    css = "".join(f"--s{i}:{LIGHT[i]};" for i in range(len(LIGHT)))
    cssd = "".join(f"--s{i}:{DARK[i]};" for i in range(len(DARK)))
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d" font-family="system-ui, sans-serif" font-size="12">',
         f'<title id="t">Queue of connections during a synchronized burst, {html.escape(phase)}</title>',
         '<desc id="d">For each variant, the median over bursts of the number of connections whose CONNECT is sent and not yet answered, and of the share of the burst admitted, against time since the burst began, with the 10th to 90th percentile band across bursts.</desc>',
         f'<style>:root{{color-scheme:light dark;--bg:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--grid:#e3e2dd;{css}}}'
         f'@media (prefers-color-scheme:dark){{:root{{--bg:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#34332f;{cssd}}}}}'
         '.t{fill:var(--ink)}.t2{fill:var(--ink2)}.g{stroke:var(--grid);stroke-width:1}.ax{stroke:var(--ink2);stroke-width:1}</style>',
         '<rect width="100%" height="100%" fill="var(--bg)"/>',
         f'<text class="t" x="{ML}" y="24" font-size="15" font-weight="600">A synchronized burst of {next(iter(curves.values()))["connections"]} connections: the queue building and draining</text>',
         f'<text class="t2" x="{ML}" y="44">Median over bursts; the band is the 10th to 90th percentile across bursts. Time is since the burst began.</text>',
         f'<text class="t2" x="{ML}" y="60">The queue is the client\'s view: CONNECT sent, no answer yet (the broker\'s authorization and the callout together).</text>']
    # legend: always present for two or more series
    for i, text, lx, row_ in legend:
        y = 77 + 18 * row_
        o.append(f'<line x1="{lx}" y1="{y}" x2="{lx + 22}" y2="{y}" stroke="var(--s{i})" stroke-width="2"' + (f' stroke-dasharray="{DASH[i]}"' if DASH[i] else "") + "/>")
        o.append(f'<text class="t2" x="{lx + 28}" y="{y + 4}">{html.escape(text)}</text>')

    def panel(top: int, key: str, ymax: float, ytitle: str, pct: bool) -> None:
        ystep = _nice_step(ymax, 4)
        yend = math.ceil(ymax / ystep) * ystep

        def sy(v: float) -> float:
            return top + PH - PH * v / yend

        v = 0.0
        while v <= yend + 1e-9:
            o.append(f'<line class="g" x1="{ML}" y1="{sy(v):.1f}" x2="{W - MR}" y2="{sy(v):.1f}"/>')
            o.append(f'<text class="t2" x="{ML - 8}" y="{sy(v) + 4:.1f}" text-anchor="end">{_fmt(v * 100) + "%" if pct else _fmt(v)}</text>')
            v += ystep
        t = 0.0
        while t <= xend + 1e-9:
            o.append(f'<text class="t2" x="{sx(t):.1f}" y="{top + PH + 16}" text-anchor="middle">{_fmt(t)}</text>')
            t += xstep
        o.append(f'<line class="ax" x1="{ML}" y1="{top + PH}" x2="{W - MR}" y2="{top + PH}"/>')
        o.append(f'<text class="t2" x="{ML}" y="{top - 12}">{html.escape(ytitle)}</text>')
        ends = []
        for i, label in enumerate(labels):
            c = curves[label]
            ts, band = c["times"], c[key]
            up = " ".join(f"{sx(ts[j]):.1f},{sy(band['high'][j]):.1f}" for j in range(GRID_POINTS))
            down = " ".join(f"{sx(ts[j]):.1f},{sy(band['low'][j]):.1f}" for j in range(GRID_POINTS - 1, -1, -1))
            o.append(f'<polygon points="{up} {down}" fill="var(--s{i})" fill-opacity="0.16" stroke="none"/>')
            line = " ".join(f"{sx(ts[j]):.1f},{sy(band['median'][j]):.1f}" for j in range(GRID_POINTS))
            o.append(f'<polyline points="{line}" fill="none" stroke="var(--s{i})" stroke-width="2" stroke-linejoin="round"' + (f' stroke-dasharray="{DASH[i]}"' if DASH[i] else "") + f'><title>{html.escape(label)}</title></polyline>')
            if key == "depth":   # the label sits above the band at the peak of the median curve
                j = max(range(GRID_POINTS), key=lambda k: band["median"][k])
                ends.append((sx(ts[j]), sy(band["high"][j]) - 7, "middle", label))
            else:                # the label sits where the median passes 50 %, on alternate sides so two do not overprint
                j = next((k for k in range(GRID_POINTS) if band["median"][k] >= 0.5), GRID_POINTS - 1)
                left = i % 2 == 0
                ends.append((sx(ts[j]) + (-10 if left else 10), sy(band["median"][j]) + 4, "end" if left else "start", label))
        placed: list[tuple[float, float, float]] = []   # the x range and y of each label so far

        def span(x: float, anchor: str, label: str) -> tuple[float, float]:
            w = 7.5 * len(label)
            return (x - w, x) if anchor == "end" else (x, x + w) if anchor == "start" else (x - w / 2, x + w / 2)

        for x, y, anchor, label in sorted(ends, key=lambda e: e[1]):
            x0, x1 = span(x, anchor, label)
            while any(x0 < b1 and b0 < x1 and abs(y - py) < 14 for b0, b1, py in placed):
                y -= 14                                  # nudge a label up until it clears the ones already placed
            if y < top:
                continue                                 # no room inside the panel: the legend names the line instead
            placed.append((x0, x1, y))
            o.append(f'<text class="t" x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-weight="600">{html.escape(label)}</text>')

    panel(TOP, "depth", float(ymax_depth), "connections waiting for an answer", False)
    panel(TOP + PH + GAP, "share", 1.0, "share of the burst admitted", True)
    cut = f" (the axis ends at {_fmt(xend)} ms; {clipped[0]} of {clipped[1]} bursts ran longer and are cut off)" if clipped and clipped[0] else ""
    o.append(f'<text class="t2" x="{(ML + W - MR) / 2:.0f}" y="{H - (46 if cluster or namespace else 30)}" text-anchor="middle">milliseconds since the burst began{html.escape(cut)}</text>')
    where = [f"cluster {cluster}" if cluster else "", f"namespace {namespace}" if namespace else "", f"run id {run_id}" if run_id else ""]
    stamps = [f"run started {run_date}" if run_date else "", f"chart generated {generated}" if generated else ""]
    if any(where):
        o.append(f'<text class="t2" x="{ML}" y="{H - 24}" font-size="11">{html.escape("  ·  ".join(x for x in where if x))}</text>')
    if any(stamps):
        o.append(f'<text class="t2" x="{ML}" y="{H - 8}" font-size="11">{html.escape("  ·  ".join(x for x in stamps if x))}</text>')
    o.append("</svg>")
    return "\n".join(o) + "\n"


def write_charts(run_dir: Path, raw_bursts: dict[str, dict[str, dict[int, list[dict]]]], labels: list[str], run_date: str = "",
                 now: dt.datetime | None = None, run_id: str = "", cluster: str = "", namespace: str = "") -> list[Path]:
    """queue-<phase>.svg for every phase whose bursts have samples. raw_bursts[phase][label][block] is a list of bursts. Each chart
    is stamped with the run's date and the time it was drawn (`now`, in the local time zone, unless given)."""
    generated = (now or dt.datetime.now().astimezone()).isoformat(timespec="seconds")
    written = []
    for phase, by_label in raw_bursts.items():
        curves, blocks = {}, {}
        every = [b for label in labels for blk in by_label[label].values() for b in blk if has_samples(b)]
        ends = sorted(max(s["doneMs"] for s in b["samples"]) for b in every)
        if not ends or ends[-1] <= 0:
            continue
        # The axis runs a little past the 90th percentile of the bursts' end times, not to the slowest: one stalled burst (a whole
        # second or more behind the rest) would otherwise squeeze every curve into the left of the chart. The chart says how many it cut off.
        xmax = 1.15 * _pct(ends, 90)
        step = _nice_step(xmax, 8)
        xmax = math.ceil(xmax / step) * step  # the axis ends on a tick
        clipped = (sum(1 for e in ends if e > xmax), len(ends))
        for label in labels:
            c = pooled_curves([b for blk in by_label[label].values() for b in blk], xmax)
            if c:
                curves[label] = c
                blocks[label] = sum(1 for blk in by_label[label].values() if any(has_samples(b) for b in blk))
        if curves:
            path = run_dir / f"queue-{phase}.svg"
            path.write_text(chart(phase, curves, blocks, run_date, generated, clipped, run_id, cluster, namespace))
            written.append(path)
    return written
