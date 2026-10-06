#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Render the horizontal cross-library benchmark (benches/horizontal.rs) as
SVG charts for the README.

Throughput (items per second), higher is better — the inverse of the wall
time the harness measures, but with an intuitive direction and explicit
units per panel. Bar panels draw min–max whiskers across the interleaved
rounds.

Panels whose values span decades across the batch sweep (cpu_balanced:
1.8 → 2070 M items/s) are plotted relative to a baseline lib (× rayon) on a
linear axis instead. Lesson: a log-y line chart made the 4–5× youpipe-vs-
rayon gaps read as near-parity (log compresses ratios into small offsets
and every polyline converges at the large-n end), while absolute linear
bars degenerated into invisible nubs for the small-n groups. Ratios per
group keep every gap readable as a bar-length ratio, and the baseline's
absolute throughput rides as a second line under each batch-size tick —
any bar's absolute value is ratio × that number (absolute values also
stay in the sibling panels and results.json).

Usage:
    uv run perf/plot-horizontal.py [results.json] [outdir]

Defaults: results.json = perf/horizontal/results.json, outdir = docs/src/assets.

NixOS note: uv's isolated env cannot find libstdc++.so.6 (matplotlib's
numpy import fails). Prefix with the nix-store gcc lib, e.g.
    LD_LIBRARY_PATH=/nix/store/<hash>-gcc-<ver>-lib/lib uv run ...
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("svg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter

# ── presentation config ──────────────────────────────────────────────────

INK = "#1f2430"
MUTED = "#64748b"
GRID = "#e2e8f0"
ERR = "#475569"

# Draw order doubles as legend order; youpipe first, its variants adjacent.
LIB_STYLE: dict[str, dict] = {
    "youpipe":                 {"color": "#0d9488"},
    "youpipe (Unbalanced)":    {"color": "#0f766e"},
    "youpipe (default)":       {"color": "#7edcd0"},
    "youpipe (512 thr)":       {"color": "#0d9488"},
    "youpipe (32 thr)":        {"color": "#a7e8e0"},
    "rayon":                   {"color": "#ea580c"},
    "tokio":                   {"color": "#4f46e5"},
    "futures":                 {"color": "#16a34a"},
    "std threads":             {"color": "#94a3b8"},
}

mpl.rcParams.update({
    "svg.fonttype": "none",  # keep text as text: small files, crisp zooming
    "font.family": "sans-serif",
    "font.size": 8.6,
    "text.color": INK,
    "axes.edgecolor": "#94a3b8",
    "axes.labelcolor": INK,
    "axes.linewidth": 0.9,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "xtick.labelsize": 8.2,
    "ytick.labelsize": 8.2,
})

# ── data loading ─────────────────────────────────────────────────────────

def load(path: Path) -> tuple[dict, dict[tuple[str, str, int], list[float]]]:
    """Return (meta, {(scenario, lib, n): per-round times in ns})."""
    doc = json.loads(path.read_text())
    data = {k: v for r in doc["results"] for k, v in
            [((r["scenario"], r["lib"], r["n"]), r["rounds_ns"])]}
    return doc["meta"], data


def lib_order(data: dict, scenario: str) -> list[str]:
    libs = {lib for sc, lib, _ in data if sc == scenario}
    ordered = [lib for lib in LIB_STYLE if lib in libs]
    return ordered + sorted(libs - set(LIB_STYLE))


def sizes_of(data: dict, scenario: str) -> list[int]:
    return sorted({n for sc, _, n in data if sc == scenario})

# ── formatting helpers ───────────────────────────────────────────────────

def fmt_val(v: float) -> str:
    if v >= 100:
        return f"{v:.0f}"
    if v >= 10:
        return f"{v:.3g}"  # integral ticks print bare: "20" not "20.0"
    return f"{v:.2g}"


def fmt_ratio(v: float) -> str:
    return f"{v:.2g}×" if v < 10 else f"{v:.0f}×"  # 4.1× · 0.97× · 1×


def fmt_tp(v: float) -> str:
    """Compact absolute items/s: 1.8M/s · 159M/s · 2.07G/s."""
    for div, suf in ((1e9, "G"), (1e6, "M"), (1e3, "K")):
        if v >= div:
            return f"{v / div:.3g}{suf}/s"
    return f"{v:.3g}/s"


def fmt_n(n: int) -> str:
    if n >= 1_000_000:
        return f"{n // 1_000_000}M"
    if n >= 1000:
        return f"{n // 1000}K"
    return str(n)

# ── panels ───────────────────────────────────────────────────────────────

def _header(ax, title: str, note: str) -> None:
    ax.set_title(title, loc="left", fontsize=10.5, fontweight="bold", pad=15)
    ax.text(0.0, 1.015, note, transform=ax.transAxes, fontsize=8.0,
            color=MUTED, va="bottom")


def _decorate(ax, unit: str) -> None:
    ax.set_ylabel(unit, fontsize=8.6)
    ax.yaxis.grid(True, color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_val(v)))
    ax.yaxis.set_minor_locator(FixedLocator([]))


def _legend(ax, ncols: int, drop: float = 0.16) -> None:
    # `drop` must sit the row clear of the "batch size" xlabel and, on
    # ratio panels, the second tick line (constrained layout reserves the
    # extra room by shrinking the axes).
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -drop), ncols=ncols,
              frameon=False, fontsize=8.2, handlelength=1.3,
              columnspacing=1.1, handletextpad=0.45, borderaxespad=0)


def _throughputs(data: dict, sc: str, lib: str, n: int, scale: float) -> tuple[float, float, float]:
    """(median, min, max) throughput in the panel's unit, from per-round times."""
    tps = [n / ns * 1e9 * scale for ns in data[(sc, lib, n)]]  # ns → items/s
    ordered = sorted(tps)
    mid = ordered[len(ordered) // 2]
    return mid, ordered[0], ordered[-1]


def bar_panel(ax, data: dict, sc: str, title: str, note: str,
              unit: str, scale: float, baseline: str | None = None) -> None:
    """Grouped bars per batch size; value label above every bar.

    With `baseline`, bars and whiskers are divided by that lib's median per
    group and labels print as ratios (the baseline itself pins every group
    at 1×). The baseline's absolute throughput rides as a second line under
    each batch-size tick, turning any ratio back into an absolute value
    (ratio × that number). It must not go above the baseline's own bar: at
    near-parity groups the neighboring ratio labels sit at the same height
    and wide absolute labels collide with them.
    """
    libs = lib_order(data, sc)
    sizes = sizes_of(data, sc)
    xs = [i * (len(libs) * 0.9 + 0.6) for i in range(len(sizes))]
    bw = 0.82
    _header(ax, title, note)

    base_med = ({n: _throughputs(data, sc, baseline, n, scale)[0]
                 for n in sizes} if baseline else None)

    top = 0.0
    for i, lib in enumerate(libs):
        color = LIB_STYLE.get(lib, {"color": "#888"})["color"]
        meds, los, his, offs = [], [], [], []
        for gi, n in enumerate(sizes):
            med, lo, hi = _throughputs(data, sc, lib, n, scale)
            if base_med is not None:
                med, lo, hi = (v / base_med[n] for v in (med, lo, hi))
            meds.append(med)
            los.append(lo)
            his.append(hi)
            offs.append(xs[gi] + (i - (len(libs) - 1) / 2) * bw)
        ax.bar(offs, meds, bw * 0.9, color=color, label=lib, zorder=3)
        yerr = [[m - l for m, l in zip(meds, los)],
                [h - m for m, h in zip(meds, his)]]
        ax.errorbar(offs, meds, yerr=yerr, fmt="none", ecolor=ERR,
                    elinewidth=1.0, capsize=2, zorder=4)
        fmt_label = fmt_ratio if base_med is not None else fmt_val
        for x, m, h in zip(offs, meds, his):
            ax.text(x, h, fmt_label(m), ha="center", va="bottom",
                    fontsize=7.4, color=INK, zorder=6)
        top = max(top, *his)

    ax.set_ylim(0, top * 1.16)
    ticks = [fmt_n(n) for n in sizes]
    if base_med is not None:
        # base_med carries the panel scale; /scale recovers items/s.
        ticks = [f"{t}\n({fmt_tp(base_med[n] / scale)})"
                 for t, n in zip(ticks, sizes)]
    ax.set_xticks(xs, ticks)
    ax.set_xlabel("batch size", fontsize=8.0, color=MUTED)
    _decorate(ax, unit)
    _legend(ax, len(libs), 0.20 if base_med is not None else 0.16)

# ── chart definitions ────────────────────────────────────────────────────
# (file, figure title, [(kind, scenario, panel title, note, unit, scale,
#                        baseline)])

CHART_DEFS = [
    ("bench-cpu.svg", "CPU pipelines", 9.7, [
        ("bar", "cpu_balanced", "Balanced CPU map",
         "uniform cost · ~100 ns per item · lower tick row: (rayon items/s)",
         "× rayon", 1.0, "rayon"),
        ("bar", "cpu_unbalanced", "Skewed CPU map",
         "10% of items cost 1000×", "M items/s", 1e-6, None),
    ]),
    ("bench-io.svg", "IO pipelines", 9.7, [
        ("bar", "io_async", "Async IO",
         "sleep 1 ms + 8 ms tail · 512 in flight", "K items/s", 1e-3, None),
        ("bar", "io_blocking", "Blocking IO",
         "thread-sleeping waits · 1 ms + 8 ms tail", "K items/s", 1e-3, None),
    ]),
    ("bench-real.svg", "Realistic mixed sync + async pipelines", 11.0, [
        ("bar", "mixed_cpu_io", "Mixed CPU + async IO",
         "CPU stage → async IO stage", "K items/s", 1e-3, None),
        ("bar", "real_doc", "Document pipeline",
         "fetch → parse → save · log-normal sizes", "K docs/s", 1e-3, None),
        ("bar", "real_web", "Web pipeline",
         "HTTP GET → parse → sum · loopback server", "K req/s", 1e-3, None),
    ]),
]

PANEL_KIND = {"bar": bar_panel}


def main() -> None:
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("perf/horizontal/results.json")
    outdir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("docs/src/assets")
    outdir.mkdir(parents=True, exist_ok=True)
    meta, data = load(results)

    rounds = meta.get("rounds", "?")
    sub = (f'{meta.get("cpus", "?")} pinned cores · {rounds} interleaved round'
           f'{"s" if rounds != 1 else ""}, median · bar whiskers = min–max across rounds · '
           f'{meta.get("timestamp", "")[:10]}')

    for fname, ftitle, fig_w, panels in CHART_DEFS:
        panels = [p for p in panels if p[1] in {k[0] for k in data}]
        if not panels:
            continue
        fig, axs = plt.subplots(1, len(panels), figsize=(fig_w, 4.15),
                                layout="constrained")
        axs = axs if len(panels) > 1 else [axs]
        for ax, (kind, sc, ptitle, note, unit, scale, baseline) in zip(axs, panels):
            PANEL_KIND[kind](ax, data, sc, ptitle, note, unit, scale, baseline)
        fig.suptitle(f"{ftitle} — throughput, higher is better",
                     fontsize=12.0, fontweight="bold")
        fig.supxlabel(sub, fontsize=7.6, color=MUTED)
        out = outdir / fname
        fig.savefig(out)
        plt.close(fig)
        print(f"wrote {out} ({out.stat().st_size / 1024:.1f} KiB)")


if __name__ == "__main__":
    main()
