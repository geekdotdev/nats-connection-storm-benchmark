# SPDX-License-Identifier: Apache-2.0
"""A run's notes: results/<run>/NOTES.md.

The facts about a run are already in its environment.json (run id, start, cluster, variants and their image digests, the design, where the
CPU came from, whether monitoring or profiling was on, units resumed or run again). They are written into NOTES.md by the driver when the
run ends, between two markers, so that nobody types them twice and they cannot disagree with the data.

What is not in environment.json is the part only a person (or an agent) knows: why the run was made, what it showed, and what to be
careful of. Those are three headings left for them to fill in. NOTES.md is never overwritten once it exists: the hand-written part is
the point of it; `refresh` replaces only what is between the markers.

results/INDEX.md has one line per run and is kept by hand (it is the log; the notes are the entries).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from . import qualify

BEGIN, END = "<!-- generated from environment.json: do not edit between the markers -->", "<!-- end generated -->"
SECTIONS = ("Purpose", "What it showed", "What perturbs it")
TODO = "_Not written yet._"


def _short(image: str) -> str:
    m = re.search(r"/([^/@:]+)@sha256:([0-9a-f]{12})", image)
    return f"{m.group(1)}@sha256:{m.group(2)}…" if m else image


def _auth(auth) -> str:
    """The authentication a run used, in words (older runs have none, and some record a bare name)."""
    if not isinstance(auth, dict):
        return f"auth {auth}"
    return f"auth {auth.get('mode', '?')} mode" + (f", {auth['resolver']} resolver" if auth.get("resolver") else "")


def facts(directory: str, env: dict) -> str:
    """The generated block, from the run's environment.json."""
    q = qualify.from_env(env)
    design = env.get("design", {})
    lines = [BEGIN, "",
             f"- **Run:** {qualify.line(q)}",
             f"- **Target:** {env.get('target') or qualify.NOT_RECORDED}" + (f", {_auth(env['auth'])}" if env.get("auth") else ""),
             f"- **Design:** {design.get('blocks', '?')} blocks, sizes {design.get('sizes', '?')}, phases {', '.join(design.get('phases', [])) or '?'}"]
    if env.get("variants_file"):
        vf = env["variants_file"]
        lines.append(f"- **Variants file:** {vf.get('path')} (sha256 {str(vf.get('sha256'))[:12]}…, copy in `variants.toml`)")
    cpu = env.get("cpu_source")
    lines.append(f"- **CPU source:** {cpu if cpu else 'not recorded (before it was)'}"
                 + (f"; monitoring: probe and Prometheus, scrape every {env['monitoring'].get('scrape_interval_s')} s" if env.get("monitoring") else ""))
    if env.get("profiling"):
        lines.append(f"- **Profiled:** {env['profiling'].get('block_profiles', '?')} snapshots; {env['profiling'].get('note', 'timings are perturbed')}")
    if env.get("resumes"):
        lines.append("- **Stopped and resumed:** " + "; ".join(f"{r['time']} with {r['units_done'] if isinstance(r['units_done'], int) else len(r['units_done'])} units done"
                                                               for r in env["resumes"]))
    redo = [r for r in (env.get("adopted") or {}).get("redo", []) if r.get("files")]   # an entry with no files was a unit that had not run yet
    if redo:
        lines.append("- **Units run again** (earlier files in `discarded/`): " + ", ".join(r["unit"] for r in redo))
    if env.get("backfilled"):
        lines.append("- **Backfilled afterwards:** " + ", ".join(env["backfilled"].get("keys", {})) + " (see `environment.json`)")
    if env.get("variants"):
        lines += ["", "| Variant | Commit | Image |", "|---|---|---|"]
        lines += [f"| `{v['label']}` | {v.get('commit', '')[:12] or '-'} | {_short(v['image'])} |" for v in env["variants"]]
    lines += ["", END]
    return "\n".join(lines)


def skeleton(directory: str, env: dict) -> str:
    body = "\n\n".join(f"## {s}\n\n{TODO}" for s in SECTIONS)
    return f"# {directory}\n\n{facts(directory, env)}\n\n{body}\n"


def ensure(out: Path) -> Path | None:
    """Write out/NOTES.md if there is none. Never overwrites: what a person wrote there is the point of the file."""
    path = out / "NOTES.md"
    env_path = out / "environment.json"
    if path.exists() or not env_path.is_file():
        return None
    path.write_text(skeleton(out.name, json.loads(env_path.read_text())))
    return path


def refresh(out: Path) -> bool:
    """Replace only the generated block of an existing NOTES.md (after the environment changed: a run resumed, a unit run again)."""
    path = out / "NOTES.md"
    if not path.is_file():
        return False
    text = path.read_text()
    if BEGIN not in text or END not in text:
        return False
    head, rest = text.split(BEGIN, 1)
    tail = rest.split(END, 1)[1]
    path.write_text(head + facts(out.name, json.loads((out / "environment.json").read_text())) + tail)
    return True


def unwritten(path: Path) -> list[str]:
    """The headings of a NOTES.md that still say nothing."""
    text = path.read_text()
    return [s for s in SECTIONS if re.search(rf"## {re.escape(s)}\n\n{re.escape(TODO)}", text)]
