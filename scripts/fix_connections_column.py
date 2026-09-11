#!/usr/bin/env python3
"""Correct the Connections column of older result files.

Until the cluster campaign (September 2026) the CSV exporter wrote the value
of the -connections flag into every row, including the read_performance and
update_performance scenarios, which run in a single synchronous worker in
every engine (internal/runner/*.go; cmd/benchmark/main.go exports
measuredConnections = 1 since then). Files written before that fix therefore
label single-client read and update measurements with the flag value.

This script rewrites Connections to 1 for read_performance and
update_performance rows and leaves every other value byte-identical. It is
idempotent and prints one line per changed file.

Usage: python3 scripts/fix_connections_column.py FILE.csv [...]
"""
import csv
import sys

SINGLE = {"read_performance", "update_performance"}


def fix(path: str) -> int:
    with open(path, newline="") as fh:
        rows = list(csv.reader(fh))
    if not rows or "Connections" not in rows[0] or "Scenario" not in rows[0]:
        return 0
    ci, si = rows[0].index("Connections"), rows[0].index("Scenario")
    changed = 0
    for row in rows[1:]:
        if len(row) > max(ci, si) and row[si] in SINGLE and row[ci] != "1":
            row[ci] = "1"
            changed += 1
    if changed:
        with open(path, "w", newline="") as fh:
            csv.writer(fh, lineterminator="\n").writerows(rows)
        print(f"{path}: {changed} rows")
    return changed


if __name__ == "__main__":
    total = sum(fix(p) for p in sys.argv[1:])
    print(f"total rows changed: {total}")
