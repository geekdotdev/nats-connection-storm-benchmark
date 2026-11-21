# Results

One directory per run, named `<description>-<date>`, holding:

- `environment.json`: both commits and the image digests, runner or node types, kernel, toolchain versions, the storm settings, and the design (blocks, order, pauses);
- `raw/`: the load generator's per-burst output for every block;
- `summary.txt`: the report, regenerated from `raw/` by `uv run bench report`.

Nothing here may contain credentials, hostnames of private systems or personal details. Compare numbers within a run only; absolute numbers from different machines or runs are not comparable.
