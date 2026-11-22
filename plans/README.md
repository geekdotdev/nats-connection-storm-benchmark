# Plans

Runs that are planned, running or finished, written down **before** the data exists: the hypothesis, what the data should look like if it is right, and what would show it is wrong. A plan is not an instruction and is not loaded by default; open one when you are about to do or interpret that run. [../results/INDEX.md](../results/INDEX.md) is the log of runs that ended; a finished plan links to its run, and the run's `NOTES.md` links back.

| Plan | Status | Run |
|---|---|---|
| [relaxed-acc-conns](relaxed-acc-conns.md) | done | [eks-relaxed-acc-conns-2026-11-21](../results/cfedersp/eks-relaxed-acc-conns-2026-11-21/NOTES.md) |
| [nolock-advantage-mechanism](nolock-advantage-mechanism.md) | dropped | |
| [mux-uncapped-callout](mux-uncapped-callout.md) | planned | |
| [broker-cpu-dose-response](broker-cpu-dose-response.md) | done | [eks-broker-cpu-2026-11-22](../results/cfedersp/eks-broker-cpu-2026-11-22/NOTES.md) |
| [tls-cost](tls-cost.md) | done | [eks-notls-2026-11-22](../results/cfedersp/eks-notls-2026-11-22/NOTES.md) |
| [callout-replicas](callout-replicas.md) | done | [eks-callout-replicas-2026-11-22](../results/cfedersp/eks-callout-replicas-2026-11-22/NOTES.md) |
| [bad-faith-storm](bad-faith-storm.md) | running | [eks-badfaith-sanity-2026-11-22](../results/cfedersp/eks-badfaith-sanity-2026-11-22/NOTES.md) |

**Status** is one of `planned`, `running`, `done` or `dropped` (say why). **Rules:** write the prediction and the falsifier before the run, and do not edit them afterwards: add a dated note under *Outcome*. A plan lists what needs approval first (image pushes to GHCR, EKS time) and a cost estimate; nothing in a plan is approval. Name the plan after the question, not the answer. When a run ends, set its plan to `done`, fill *Outcome* and link the run here. A test (`tests/test_plans.py`) checks that every plan has the headings below, a valid status, and a link to an existing run when it is running or done.

Required headings: **Question**, **Hypothesis**, **Prediction**, **Falsifier**, **Design**, **Needs approval**, **Outcome**.
