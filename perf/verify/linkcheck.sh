#!/usr/bin/env bash
# mdbook intra-book link check (todo #15): every relative markdown link
# target and every SUMMARY.md chapter path under docs/src must resolve to an
# existing file. External (http/https/mailto) and anchor-only (#fragment)
# targets are skipped — mdbook/rustdoc anchor slugs are not reconstructible
# from the source without the renderer (both slug styles are on record, e.g.
# `#youpipe_async_chain_floor-async-chain-capacity-floor-streaming`), and the
# 2026-10-20 one-off scan showed file targets are the actionable breakage
# class. Replaces that one-off scan. Wired into CI since 2026-10-26 (todo
# #15): first step of the Test workflow (PR gate, fails fast before any
# toolchain setup) and a pre-build step of the Docs workflow (Pages deploy).
#
# Usage: perf/verify/linkcheck.sh   (exit 0 = clean, exit 1 + list otherwise)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BOOK_DIR=docs/src
broken=0

check_link() { # $1 = linking md file, $2 = raw link target
    local from=$1 raw=$2 target=${2%%\#*} base
    case $raw in
        http://*|https://*|mailto:*) return 0 ;;
        \#*) return 0 ;; # same-page anchor
    esac
    # SUMMARY.md chapter paths are relative to the book root; every other
    # file's links are relative to the file's own directory.
    if [[ $from == "$BOOK_DIR/SUMMARY.md" ]]; then
        base=$BOOK_DIR
    else
        base=$(dirname "$from")
    fi
    if [[ ! -e $base/$target ]]; then
        printf 'broken link: %s -> %s\n' "$from" "$raw"
        broken=1
    fi
}

while IFS= read -r f; do
    # `](target)` — covers [text](t) and ![alt](t); no link in this book
    # spans lines or nests parens. grep exits 1 on no match; tolerate.
    while IFS= read -r raw; do
        check_link "$f" "$raw"
    done < <(grep -oE '\]\([^)]+\)' "$f" | sed -e 's/^](//' -e 's/)$//' || true)
done < <(find "$BOOK_DIR" -name '*.md' | sort)

if ((broken)); then
    echo "linkcheck: broken targets found (see above)"
else
    echo "linkcheck: all intra-book targets resolve"
fi
exit "$broken"
