#!/usr/bin/env python3
"""Summarize a run-drift-exp.sh output tree.

Reads <out>/<event>-r<n>/<group>/<id>/new/estimates.json and prints, per
(event, id):

  * cross-run CV — |std of run means / mean of run means|, the "inter-run
    drift" figure (the number the methodology question is about);
  * within-run CV — mean over runs of std_dev/mean, criterion's own
    per-run scatter (what a single invocation's CI reports).
"""

import json
import sys
from pathlib import Path

import statistics as st


def cv(values: list[float]) -> float:
    mean = st.mean(values)
    return st.stdev(values) / abs(mean) * 100 if mean else float("nan")


def main() -> None:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/counter-bench-drift")
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    # data[(event, id)] = list of per-run means, list of per-run within CVs
    data: dict[tuple[str, str], tuple[list[float], list[float]]] = {}
    for run_dir in sorted(out.glob("*-r*")):
        event = run_dir.name.rsplit("-r", 1)[0]
        for estimates in run_dir.glob("*/*/new/estimates.json"):
            # Full group/id path — ids repeat across groups (e.g. both
            # perf_sync_cpu_heavy and perf_sync_lightweight have a
            # `rayon_par_iter`), and conflating them bi-modals the CV.
            bench_id = str(estimates.parent.parent.relative_to(run_dir))
            est = json.loads(estimates.read_text())
            mean = est["mean"]["point_estimate"]
            within = est["std_dev"]["point_estimate"] / mean * 100
            means, withins = data.setdefault((event, bench_id), ([], []))
            means.append(mean)
            withins.append(within)

    events = sorted({e for e, _ in data}, key=lambda e: (e != "walltime", e))
    print(f"{out} — {runs} runs per measurement kind\n")
    print(f"{'event':<14}{'bench id':<40}{'cross-run CV':>13}{'within-run CV':>15}")
    for event in events:
        for (ev, bench_id), (means, withins) in sorted(data.items()):
            if ev != event:
                continue
            print(f"{event:<14}{bench_id:<40}{cv(means):>12.2f}%{st.mean(withins):>14.2f}%")
        print()


if __name__ == "__main__":
    main()
