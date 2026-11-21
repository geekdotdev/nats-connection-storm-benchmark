# SPDX-License-Identifier: Apache-2.0
"""Summarise a run directory: for each phase, the median auth time (CONNECT to PONG) of each burst, for each variant.

The first variant is the baseline; every other variant is compared with it. The unit of replication is the block (one freshly
restarted broker), so each comparison is an exact two-sided permutation test on the per-block medians of the two variants,
not on the bursts. With more than two variants there are several comparisons and no correction is made for that: read each
p value as one comparison among that many. Standard library only. Reads older results too (environment.json "arms").
"""
from __future__ import annotations

import itertools
import json
import re
import statistics as st
from pathlib import Path

from . import queue
from .ab import runs_of


def perm_p(a: list[float], b: list[float]) -> float:
    """Exact two-sided permutation test on the difference of means. Enumerates every split, so keep blocks small (8 v 8 is 12,870)."""
    allv = a + b
    n = len(a)
    obs = abs(st.mean(a) - st.mean(b))
    hits = tot = 0
    for idx in itertools.combinations(range(len(allv)), n):
        chosen = set(idx)
        ga = [allv[i] for i in idx]
        gb = [allv[i] for i in range(len(allv)) if i not in chosen]
        tot += 1
        hits += abs(st.mean(ga) - st.mean(gb)) >= obs - 1e-12
    return hits / tot


def _bursts(raw: Path, label: str, block: int, phase: str) -> list[dict]:
    p = raw / f"{label}-b{block}-{phase}.json"
    return runs_of(json.loads(p.read_text())) if p.exists() else []


def _load(run_dir: Path) -> tuple[dict, list[dict], list[str], Path, list[int], list[str]]:
    env = json.loads((run_dir / "environment.json").read_text())
    described = env.get("variants") or env["arms"]  # runs before 2026-11-18 call them arms
    labels = [a["label"] for a in described]
    base = labels[0]
    raw = run_dir / "raw"
    blocks = sorted({int(m.group(1)) for p in raw.glob("*.meta.json") if (m := re.search(r"-b(\d+)\.meta\.json$", p.name))})
    phases = [p for p in env["design"]["phases"] if p not in ("warm",)]
    return env, described, labels, raw, blocks, phases


def bursts_by_phase(run_dir: Path) -> tuple[list[str], dict[str, dict[str, dict[int, list[dict]]]]]:
    """Every burst of the run: phase -> variant label -> block -> the bursts, in order."""
    _, _, labels, raw, blocks, phases = _load(run_dir)
    return labels, {phase: {label: {b: _bursts(raw, label, b, phase) for b in blocks} for label in labels} for phase in phases}


def write(run_dir: Path) -> str:
    """The summary text, written to summary.txt, and the queue charts (queue-<phase>.svg) next to it; returns the text."""
    text = summarize(run_dir)
    (run_dir / "summary.txt").write_text(text + "\n")
    labels, by_phase = bursts_by_phase(run_dir)
    for stale in run_dir.glob("queue-*.svg"):
        stale.unlink()  # a chart from an earlier report of the same directory must not outlive its data
    queue.write_charts(run_dir, by_phase, labels)
    return text


def summarize(run_dir: Path) -> str:
    env, described, labels, raw, blocks, phases = _load(run_dir)
    base = labels[0]
    out = [f"blocks found: {blocks}"]
    for a in described:
        extras = {k: v for k, v in a.items() if k not in ("label", "commit", "image")}
        what = f"commit {a['commit'][:12]}, " if a.get("commit") else ""
        out.append(f"{a['label']}: {what}image {a['image']}" + (f"   settings {json.dumps(extras, sort_keys=True)}" if extras else ""))
    if len(labels) > 2:
        out.append(f"baseline {base}; {len(labels) - 1} comparisons with it, not corrected for the number of them")
    out.append("")
    for phase in phases:
        res: dict[str, tuple[list[float], list[float], int, int]] = {}
        for label in labels:
            medians, secs, adm, tot = [], [], 0, 0
            for b in blocks:
                bursts = _bursts(raw, label, b, phase)
                if not bursts:
                    continue
                medians.append(st.median(r["authMs"]["p50"] for r in bursts if r["authMs"]["p50"] is not None))
                secs += [r["seconds"] for r in bursts]
                adm += sum(r["outcomes"]["admitted"] for r in bursts)
                tot += sum(r["connections"] for r in bursts)
            res[label] = (medians, secs, adm, tot)
        out.append(phase)
        if any(not res[label][0] for label in labels):
            out.append("  (missing data)\n")
            continue
        for label in labels:
            vals, secs, adm, tot = res[label]
            out.append(f"  {label:<8} median auth p50 per block (ms): {[round(x, 1) for x in vals]}  mean {st.mean(vals):.1f}   admitted {adm}/{tot}   burst length median {st.median(secs):.2f}s")
        am, asecs = res[base][0], res[base][1]
        for label in labels[1:]:
            bm, bsecs = res[label][0], res[label][1]
            ma, mb = st.mean(am), st.mean(bm)
            out.append(f"  change {label} vs {base} in the mean of block medians: {100 * (mb - ma) / ma:+.1f}%   exact permutation p={perm_p(am, bm):.5f}  (n={len(am)} v {len(bm)} blocks)")
            if phase != "idle":
                out.append(f"  burst length (throughput proxy) change {label} vs {base}: {100 * (st.median(bsecs) - st.median(asecs)) / st.median(asecs):+.1f}%")
        q = queue.summarize({label: {b: _bursts(raw, label, b, phase) for b in blocks} for label in labels}, labels, perm_p)
        out.extend(q if q else (["  (no per-connection samples in this phase, so no queue curve; runs before the samples were recorded have none)"] if phase != "idle" else []))
        out.append("")
    return "\n".join(out)
