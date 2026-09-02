#!/usr/bin/env python3
"""Render the horizontal cross-library benchmark (benches/horizontal.rs) as
SVG charts for the README.

Usage:
    python3 perf/plot-horizontal.py [results.json] [outdir]

Defaults: results.json = perf/horizontal/results.json, outdir = docs/assets.

Zero third-party dependencies: the SVG is emitted by hand so the charts stay
small and crisp on both light and dark GitHub themes.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

# ── presentation config ──────────────────────────────────────────────────

FONT = "-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
INK = "#1f2430"     # primary text
MUTED = "#64748b"   # secondary text
GRID = "#e2e8f0"
AXIS = "#94a3b8"

# Draw order doubles as legend order; youpipe first, its variants adjacent.
LIB_STYLE: dict[str, dict] = {
    "youpipe":              {"color": "#0d9488", "width": 2.8, "dash": None},
    "youpipe (Unbalanced)": {"color": "#0d9488", "width": 2.8, "dash": None},
    "youpipe (default)":    {"color": "#5eead4", "width": 2.2, "dash": "6 4"},
    "youpipe (32 thr)":     {"color": "#5eead4", "width": 2.2, "dash": None},
    "youpipe (512 thr)":    {"color": "#0d9488", "width": 2.8, "dash": None},
    "rayon":                {"color": "#ea580c", "width": 2.0, "dash": None},
    "tokio":                {"color": "#4f46e5", "width": 2.0, "dash": None},
    "futures":              {"color": "#16a34a", "width": 2.0, "dash": None},
    "std threads":          {"color": "#94a3b8", "width": 2.0, "dash": None},
}

PANEL_W, PANEL_H = 436, 300
PANEL_GAP = 30


# ── data loading ─────────────────────────────────────────────────────────

def load(path: Path) -> tuple[dict, dict[tuple[str, str, int], float]]:
    """Return (meta, {(scenario, lib, n): median_ns})."""
    doc = json.loads(path.read_text())
    data = {}
    for r in doc["results"]:
        data[(r["scenario"], r["lib"], r["n"])] = r["median_ns"]
    return doc["meta"], data


def lib_order(data: dict, scenario: str) -> list[str]:
    libs = {lib for sc, lib, _ in data if sc == scenario}
    ordered = [lib for lib in LIB_STYLE if lib in libs]
    ordered += sorted(libs - set(LIB_STYLE))
    return ordered


# ── formatting helpers ───────────────────────────────────────────────────

def fmt_ms(ns: float) -> str:
    ms = ns / 1e6
    if ms >= 100:
        return f"{ms:.0f}"
    if ms >= 10:
        return f"{ms:.1f}"
    if ms >= 1:
        return f"{ms:.2f}"
    return f"{ms:.2f}"


def fmt_n(n: int) -> str:
    if n >= 1_000_000:
        return f"{n // 1_000_000}M"
    if n >= 1000:
        return f"{n // 1000}K"
    return str(n)


def nice_ticks(lo: float, hi: float, count: int = 4) -> list[float]:
    span = hi - lo
    if span <= 0:
        return [lo]
    step = 10 ** math.floor(math.log10(span / count))
    for mult in (1, 2, 2.5, 5, 10):
        if mult * step >= span / count:
            step *= mult
            break
    start = math.ceil(lo / step) * step
    return [start + i * step for i in range(int(math.ceil((hi - start) / step)) + 1)]


def log_ticks(lo_dec: float, hi_dec: float) -> list[float]:
    """1-2-5 ticks between decades lo_dec..hi_dec (decades of milliseconds)."""
    ticks = []
    for d in range(math.floor(lo_dec), math.ceil(hi_dec) + 1):
        for m in (1, 2, 5):
            v = m * 10.0**d
            if lo_dec - 1e-9 <= math.log10(v) <= hi_dec + 1e-9:
                ticks.append(v)
    return ticks


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── SVG primitives ───────────────────────────────────────────────────────

def text(x: float, y: float, s: str, size: float, color: str, anchor: str = "middle",
         weight: int = 400) -> str:
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-family="{FONT}" font-size="{size}" '
            f'fill="{color}" text-anchor="{anchor}" font-weight="{weight}">{esc(s)}</text>')


def _swatch(lib: str, cx: float, cy: float, mode: str) -> tuple[str, float]:
    """Legend swatch for one lib; returns (svg, width consumed)."""
    st = LIB_STYLE.get(lib, {"color": "#888", "width": 2, "dash": None})
    if mode == "line":
        dash = f' stroke-dasharray="{st["dash"]}"' if st["dash"] else ""
        return (f'<line x1="{cx:.1f}" y1="{cy:.1f}" x2="{cx + 21:.1f}" y2="{cy:.1f}" '
                f'stroke="{st["color"]}" stroke-width="{st["width"] + 0.6}"{dash} '
                f'stroke-linecap="round"/>', 26)
    return (f'<rect x="{cx:.1f}" y="{cy - 6.5:.1f}" width="13" height="13" rx="3" '
            f'fill="{st["color"]}"/>', 18)


def legend(x: float, y: float, libs: list[str], mode: str, max_w: float,
           size: float = 11.0) -> list[str]:
    """Horizontal legend, greedily wrapped to `max_w`. Returns text rows."""
    def item_w(lib: str) -> float:
        sw = 26 if mode == "line" else 18
        return sw + 6.2 * len(lib) * (size / 11.0) + 18

    rows: list[list[str]] = [[]]
    cur_w = 0.0
    for lib in libs:
        w = item_w(lib)
        if rows[-1] and cur_w + w > max_w:
            rows.append([])
            cur_w = 0.0
        rows[-1].append(lib)
        cur_w += w

    out, line_h = [], 19
    for r, row in enumerate(rows):
        cx, cy = x, y + r * line_h
        for lib in row:
            sw, sw_w = _swatch(lib, cx, cy, mode)
            out.append(sw)
            cx += sw_w
            out.append(text(cx, cy + 4.1, lib, size, INK, anchor="start"))
            cx += item_w(lib) - sw_w - 18 + 18  # text + gap
    return out


# ── panels ───────────────────────────────────────────────────────────────

def _header(title: str, note: str, libs: list[str], mode: str,
            left: float, plot_w: float) -> list[str]:
    g = [text(left, 19, title, 14.5, INK, anchor="start", weight=600),
         text(left, 36, note, 10.8, MUTED, anchor="start")]
    g += legend(left, 53, libs, mode, max_w=plot_w)
    return g


def line_panel(title: str, note: str, sizes: list[int], libs: list[str],
               data: dict) -> str:
    """Time on a log ms axis; one polyline per library."""
    top, bottom, left, right = 78, 34, 52, 14
    pw, ph = PANEL_W - left - right, PANEL_H - top - bottom
    n_libs = len(libs)
    n_rows = 2 if n_libs > 3 else 1
    top += (n_rows - 1) * 19
    ph = PANEL_H - top - bottom

    dec = lambda ns: math.log10(ns / 1e6)
    vals = [data[(title, lib, n)] for lib in libs for n in sizes]
    lo = math.floor(min(map(dec, vals)) * 2) / 2
    hi = math.ceil(max(map(dec, vals)) * 2) / 2

    def ymap(ns: float) -> float:
        return top + (1 - (dec(ns) - lo) / (hi - lo)) * ph

    def xpos(i: int) -> float:
        return left + (i / (len(sizes) - 1)) * pw

    g = _header(title, note, libs, "line", left, pw)

    for t in log_ticks(lo, hi):
        yy = (1 - (math.log10(t) - lo) / (hi - lo)) * ph
        g.append(f'<line x1="{left}" y1="{top + yy:.1f}" x2="{left + pw}" '
                 f'y2="{top + yy:.1f}" stroke="{GRID}" stroke-width="1"/>')
        g.append(text(left - 8, top + yy + 3.5, fmt_ms(t * 1e6), 10.5, MUTED, anchor="end"))
    g.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + ph}" '
             f'stroke="{AXIS}" stroke-width="1.2"/>')
    g.append(f'<line x1="{left}" y1="{top + ph}" x2="{left + pw}" y2="{top + ph}" '
             f'stroke="{AXIS}" stroke-width="1.2"/>')
    for i, n in enumerate(sizes):
        g.append(text(xpos(i), top + ph + 20, fmt_n(n), 11, MUTED))

    for lib in libs:
        st = LIB_STYLE.get(lib, {"color": "#888", "width": 2, "dash": None})
        pts = [f"{xpos(i):.1f},{ymap(data[(title, lib, n)]):.1f}"
               for i, n in enumerate(sizes)]
        dash = f' stroke-dasharray="{st["dash"]}"' if st["dash"] else ""
        g.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{st["color"]}" '
                 f'stroke-width="{st["width"]}"{dash} stroke-linejoin="round" '
                 f'stroke-linecap="round"/>')
        for i, n in enumerate(sizes):
            g.append(f'<circle cx="{xpos(i):.1f}" cy="{ymap(data[(title, lib, n)]):.1f}" '
                     f'r="3.4" fill="{st["color"]}" stroke="#ffffff" stroke-width="1.4"/>')
    return f'<g transform="translate(0,0)">{"".join(g)}</g>'


def bar_panel(title: str, note: str, sizes: list[int], libs: list[str],
              data: dict) -> str:
    """Time on a linear axis from zero; value label above every bar."""
    top, bottom, left, right = 78, 34, 52, 14
    pw = PANEL_W - left - right
    n_libs = len(libs)
    n_rows = 2 if n_libs > 3 else 1
    top += (n_rows - 1) * 19
    ph = PANEL_H - top - bottom

    hi = max(data[(title, lib, n)] for lib in libs for n in sizes) / 1e6 * 1.2

    def ymap(ns: float) -> float:
        return top + (1 - (ns / 1e6) / hi) * ph

    group_w = pw / len(sizes)
    bar_w = min(30.0, group_w / len(libs) * 0.62)

    g = _header(title, note, libs, "bar", left, pw)

    for t in nice_ticks(0, hi, 4):
        if t == 0:
            continue
        yy = top + (1 - t / hi) * ph
        g.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{left + pw}" y2="{yy:.1f}" '
                 f'stroke="{GRID}" stroke-width="1"/>')
        g.append(text(left - 8, yy + 3.5, fmt_ms(t * 1e6), 10.5, MUTED, anchor="end"))
    g.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + ph}" '
             f'stroke="{AXIS}" stroke-width="1.2"/>')
    g.append(f'<line x1="{left}" y1="{top + ph}" x2="{left + pw}" y2="{top + ph}" '
             f'stroke="{AXIS}" stroke-width="1.2"/>')

    for gi, n in enumerate(sizes):
        cx = left + group_w * (gi + 0.5)
        total = bar_w * len(libs) + 4 * (len(libs) - 1)
        bx = cx - total / 2
        for lib in libs:
            ns = data[(title, lib, n)]
            bh = max(2.0, top + ph - ymap(ns))
            st = LIB_STYLE.get(lib, {"color": "#888"})
            g.append(f'<rect x="{bx:.1f}" y="{top + ph - bh:.1f}" width="{bar_w:.1f}" '
                     f'height="{bh:.1f}" rx="3" fill="{st["color"]}" fill-opacity="0.92"/>')
            g.append(text(bx + bar_w / 2, top + ph - bh - 4.5, fmt_ms(ns), 9.6, INK))
            bx += bar_w + 4
        g.append(text(cx, top + ph + 20, fmt_n(n), 11, MUTED))
    return f'<g transform="translate(0,0)">{"".join(g)}</g>'


# ── figure assembly ──────────────────────────────────────────────────────

HEADER_H = 58
ROW_GAP = 18


def figure(name: str, rows: list[list[str]], meta: dict) -> str:
    """rows: grid of pre-rendered panels (each row = one horizontal line)."""
    n_cols = max(len(r) for r in rows)
    total_w = 8 + n_cols * PANEL_W + (n_cols - 1) * PANEL_GAP
    total_h = HEADER_H + len(rows) * PANEL_H + (len(rows) - 1) * ROW_GAP + 12
    rounds = meta.get("rounds", "?")
    sub = (f'{meta.get("cpus", "?")} pinned cores · {meta.get("hostname", "")} · '
           f'{rounds} interleaved round{"s" if rounds != 1 else ""}, median · '
           f'{meta.get("timestamp", "")[:10]}')
    body = [text(total_w / 2, 24, name, 15.5, INK, weight=700),
            text(total_w / 2, 42, sub, 11, MUTED)]
    for r, row in enumerate(rows):
        y = HEADER_H + r * (PANEL_H + ROW_GAP)
        for i, p in enumerate(row):
            tx = 8 + i * (PANEL_W + PANEL_GAP)
            body.append(p.replace('transform="translate(0,0)"',
                                  f'transform="translate({tx:.0f},{y})"', 1))
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_w}" height="{total_h}" '
            f'viewBox="0 0 {total_w} {total_h}" role="img">'
            f'<rect width="{total_w}" height="{total_h}" fill="#ffffff"/>'
            f'{"".join(body)}</svg>')


# ── chart definitions ────────────────────────────────────────────────────

CHART_DEFS: list[tuple[str, str, list[list[tuple[str, str, str]]]]] = [
    ("bench-cpu.svg", "CPU pipelines", [
        [("line", "cpu_balanced", "Balanced CPU map · 100 ns/item · lower is better"),
         ("bar", "cpu_unbalanced", "Skewed CPU · 10% of items cost 1000× · lower is better")],
    ]),
    ("bench-io.svg", "IO pipelines", [
        [("line", "io_async", "Async IO · 512 in flight · 1/8 ms tail · lower is better"),
         ("bar", "io_blocking", "Blocking IO · 1/8 ms tail · lower is better")],
    ]),
    ("bench-real.svg", "Mixed sync + async pipelines (realistic workloads)", [
        [("bar", "mixed_cpu_io", "Sync CPU stage + async IO stage · lower is better"),
         ("bar", "real_doc", "Docs: fetch → parse → save · heavy-tailed sizes")],
        [("bar", "real_web", "HTTP fetch → parse → aggregate · loopback server")],
    ]),
]


def main() -> None:
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("perf/horizontal/results.json")
    outdir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("docs/assets")
    outdir.mkdir(parents=True, exist_ok=True)
    meta, data = load(results)
    scenarios = {k[0] for k in data}

    def sizes_of(sc: str) -> list[int]:
        return sorted({n for s, _, n in data if s == sc})

    for fname, ftitle, row_defs in CHART_DEFS:
        rows = []
        for row_defs_row in row_defs:
            row = []
            for kind, sc, note in row_defs_row:
                if sc not in scenarios:
                    continue
                libs = lib_order(data, sc)
                sizes = sizes_of(sc)
                panel = line_panel(sc, note, sizes, libs, data) if kind == "line" \
                    else bar_panel(sc, note, sizes, libs, data)
                row.append(panel)
            if row:
                rows.append(row)
        if not rows:
            continue
        out = outdir / fname
        out.write_text(figure(ftitle, rows, meta))
        print(f"wrote {out} ({out.stat().st_size / 1024:.1f} KiB)")


if __name__ == "__main__":
    main()
