#!/usr/bin/env python3
"""Aggregate interleaved A/B(/C) criterion rounds into a drift-resistant verdict.

Reads an outdir produced by bench_ab.sh:

    outdir/round-<n>/<label>/            <- CRITERION_HOME for that (round, side)
        <group>/<bench>/<size>/new/estimates.json

Only `new/estimates.json` files are ever read — the round-2 lesson was that
criterion's accumulated `base/`/`change/` dirs silently compare against stale
runs and produce phantom regressions.

Per id we take each round's median estimate, then the median across rounds
per side. The verdict heuristic uses the spread of per-round medians as the
noise scale: a delta is only called a real change when it clearly exceeds
the larger side's round-to-round spread.
"""

from __future__ import annotations

import argparse
import itertools
import json
import statistics
import sys
from pathlib import Path

NOISE_PCT = 2.0


def load_side(round_dirs: list[Path], label: str) -> dict[str, dict[int, float]]:
    """id -> {round_number: median_estimate_ns}"""
    per_id: dict[str, dict[int, float]] = {}
    for rd in round_dirs:
        num = int(rd.name.removeprefix("round-"))
        home = rd / label
        if not home.is_dir():
            continue
        for est in home.rglob("new/estimates.json"):
            rel = est.relative_to(home).parent.parent  # strip trailing /new
            with est.open() as f:
                data = json.load(f)
            median_ns = data["median"]["point_estimate"]
            per_id.setdefault(rel.as_posix(), {})[num] = median_ns
    return per_id


def fmt_ns(ns: float) -> str:
    for unit, div in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= div:
            return f"{ns / div:.4g} {unit}"
    return f"{ns:.4g} ns"


def pct(a: float, b: float) -> float:
    return (b - a) / a * 100.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir", type=Path)
    ap.add_argument(
        "--pairs",
        nargs="*",
        default=None,
        help="side pairs as a:b (default: first side vs every other)",
    )
    ap.add_argument(
        "--fail-on-regression",
        type=float,
        default=None,
        metavar="PCT",
        help="exit 1 if any stable regression worse than PCT%%",
    )
    args = ap.parse_args()

    round_dirs = sorted(
        (d for d in args.outdir.glob("round-*") if d.is_dir()),
        key=lambda d: int(d.name.removeprefix("round-")),
    )
    if not round_dirs:
        print(f"no round-* dirs under {args.outdir}", file=sys.stderr)
        return 1

    labels: list[str] = []
    for rd in round_dirs:
        for child in sorted(rd.iterdir()):
            if child.is_dir() and child.name not in labels:
                labels.append(child.name)
    if len(labels) < 2:
        print("need at least two sides", file=sys.stderr)
        return 1

    sides = {label: load_side(round_dirs, label) for label in labels}

    if args.pairs:
        pairs = [tuple(p.split(":", 1)) for p in args.pairs]
    else:
        pairs = [(labels[0], other) for other in labels[1:]]

    any_regression = False
    for a_label, b_label in pairs:
        a, b = sides[a_label], sides[b_label]
        common = sorted(set(a) & set(b))
        only_a = set(a) - set(b)
        only_b = set(b) - set(a)
        if only_a or only_b:
            print(f"# note: ids only on one side ignored "
                  f"(only {a_label}: {sorted(only_a)}, only {b_label}: {sorted(only_b)})")

        rows = []
        for ident in common:
            ra = [a[ident][k] for k in sorted(a[ident])]
            rb = [b[ident][k] for k in sorted(b[ident])]
            med_a, med_b = statistics.median(ra), statistics.median(rb)
            delta = pct(med_a, med_b)
            spread_a = (max(ra) - min(ra)) / statistics.median(ra) * 100
            spread_b = (max(rb) - min(rb)) / statistics.median(rb) * 100
            noise = max(spread_a, spread_b, NOISE_PCT)
            # stable = the delta clearly exceeds the observed round spread and
            # every round pair leans the same way beyond noise
            round_ratios = [pct(x, y) for x, y in zip(ra, rb)]
            stable = abs(delta) > noise and all(
                abs(rr) > noise and (rr > 0) == (delta > 0) for rr in round_ratios
            )
            rows.append((ident, ra, rb, med_a, med_b, delta, spread_a, spread_b,
                         noise, stable))

        rows.sort(key=lambda r: -abs(r[5]))
        print(f"\n## {a_label} → {b_label}  "
              f"({len(round_dirs)} interleaved rounds, median-of-round-medians; "
              f"Δ>0 = {b_label} slower)\n")
        print("| id | rounds(a) | rounds(b) | med a | med b | Δ% | spread a/b % | verdict |")
        print("|---|---|---|---|---|---|---|---|")
        for ident, ra, rb, med_a, med_b, delta, sa, sb, noise, stable in rows:
            verdict = (
                ("REGRESSION" if delta > 0 else "improvement") + ("*stable*" if stable else "")
                if abs(delta) > noise else "noise"
            )
            if stable and delta > 0 and args.fail_on_regression is not None \
                    and delta > args.fail_on_regression:
                any_regression = True
            print(f"| {ident} | {'/'.join(fmt_ns(x) for x in ra)} "
                  f"| {'/'.join(fmt_ns(x) for x in rb)} "
                  f"| {fmt_ns(med_a)} | {fmt_ns(med_b)} | {delta:+.1f} "
                  f"| {sa:.1f}/{sb:.1f} | {verdict} |")

        tsv = args.outdir / f"compare-{a_label}-{b_label}.tsv"
        with tsv.open("w") as f:
            f.write("id\tmed_a_ns\tmed_b_ns\tdelta_pct\tspread_a_pct\tspread_b_pct\t"
                    "rounds_a_ns\trounds_b_ns\n")
            for ident, ra, rb, med_a, med_b, delta, sa, sb, _, _ in rows:
                f.write(f"{ident}\t{med_a}\t{med_b}\t{delta:.3f}\t{sa:.2f}\t{sb:.2f}\t"
                        f"{'\t'.join(map(str, ra))}\t{'\t'.join(map(str, rb))}\n")
        print(f"\n(wrote {tsv})")

    if any_regression:
        print("\nFAIL: stable regression beyond threshold")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
