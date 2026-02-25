#!/usr/bin/env python3
"""Convert benchmark CSV results to JSON for the GitHub Pages dashboard.

Scans results/*.csv (skipping *_raw.csv), parses statistical aggregates,
extracts metadata from filenames and CSV columns, and writes a single
docs/data/data.json consumed by the static dashboard.

Python 3 stdlib only -- no pip dependencies.
"""

import csv
import json
import os
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Paths -- relative to this script so it works from any working directory
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "docs", "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "data.json")

# ---------------------------------------------------------------------------
# Known databases (used only for filename extraction)
# ---------------------------------------------------------------------------
KNOWN_DATABASES = {"postgres", "mysql", "mongodb", "cassandra"}

# ---------------------------------------------------------------------------
# Canonical sort orders -- items are ordered logically, not alphabetically.
# Only values that actually appear in the data are emitted.
# ---------------------------------------------------------------------------
DATABASE_ORDER = ["postgres", "mysql", "mongodb", "cassandra"]
SCALE_ORDER = ["100k", "1m", "10m"]
KEY_TYPE_ORDER = [
    "SEQUENTIAL", "OBJECTID", "UUIDV1", "UUIDV4", "UUIDV7",
    "ULID", "ULID_MONOTONIC",
]
SCENARIO_ORDER = [
    "insert_performance", "read_performance", "update_performance",
    "mixed_insert_heavy", "mixed_read_update",
]

# ---------------------------------------------------------------------------
# Metric normalization -- unify scenario-specific variants into one name.
# Each scenario records its primary ops/sec under a different name; we
# collapse them so the dashboard can offer "throughput" across all scenarios.
# ---------------------------------------------------------------------------
METRIC_RENAMES = {
    "read_throughput": "throughput",
    "update_throughput": "throughput",
    "overall_throughput": "throughput",
}


def record_count_to_scale(count):
    """Map a numeric record count to a human-friendly scale label."""
    mapping = {
        100000: "100k",
        1000000: "1m",
        10000000: "10m",
        100000000: "100m",
    }
    return mapping.get(count, str(count))


def extract_database(filename):
    """Extract the database name from the leading segment of *filename*.

    Filenames follow the pattern ``{database}_{scale}_{scenario}_{Nconn}.csv``
    where *database* is one of the KNOWN_DATABASES.
    """
    base = os.path.splitext(filename)[0]  # strip .csv
    first_segment = base.split("_", 1)[0]
    if first_segment in KNOWN_DATABASES:
        return first_segment
    return None


def sorted_by_order(values, order):
    """Return *values* sorted according to *order*, appending unknowns at the end."""
    order_map = {v: i for i, v in enumerate(order)}
    return sorted(values, key=lambda v: order_map.get(v, len(order)))


def parse_csv_file(filepath):
    """Parse a single summary CSV and yield entry dicts."""
    filename = os.path.basename(filepath)
    database = extract_database(filename)
    if database is None:
        print(f"  WARN: cannot determine database from '{filename}', skipping")
        return

    with open(filepath, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Skip rows where Median == -1 (non-applicable metrics)
            try:
                median_val = float(row["Median"])
            except (ValueError, KeyError):
                continue
            if median_val == -1:
                continue

            try:
                record_count = int(row["RecordCount"])
                connections = int(row["Connections"])
            except (ValueError, KeyError):
                continue

            scale = record_count_to_scale(record_count)

            raw_metric = row["Metric"]
            metric = METRIC_RENAMES.get(raw_metric, raw_metric)

            yield {
                "database": database,
                "scale": scale,
                "scenario": row["Scenario"],
                "connections": connections,
                "keyType": row["KeyType"],
                "metric": metric,
                "median": round(float(row["Median"]), 2),
                "mean": round(float(row["Mean"]), 2),
                "stddev": round(float(row["StdDev"]), 2),
                "min": round(float(row["Min"]), 2),
                "max": round(float(row["Max"]), 2),
                "cv": round(float(row["CV_Percent"]), 2),
            }


def build_data():
    """Scan results/*.csv, aggregate entries, derive metadata."""
    entries = []
    files_processed = 0

    csv_files = sorted(
        f for f in os.listdir(RESULTS_DIR)
        if f.endswith(".csv") and not f.endswith("_raw.csv")
    )

    if not csv_files:
        print("No CSV files found in", RESULTS_DIR)
        return {"entries": [], "metadata": {}}

    print(f"Found {len(csv_files)} summary CSV file(s) in {RESULTS_DIR}")

    for fname in csv_files:
        filepath = os.path.join(RESULTS_DIR, fname)
        count_before = len(entries)
        entries.extend(parse_csv_file(filepath))
        added = len(entries) - count_before
        if added > 0:
            files_processed += 1
            print(f"  {fname}: {added} entries")
        else:
            print(f"  {fname}: 0 entries (all skipped or empty)")

    # Deduplicate by composite key (last writer wins).
    # This handles overlapping files like postgres_1m_all_8conn.csv and
    # postgres_1m_insert-performance_batch1_8conn.csv which both contain
    # insert_performance data for the same database/scale/connections.
    seen = {}
    for entry in entries:
        key = (entry["database"], entry["scale"], entry["scenario"],
               entry["connections"], entry["keyType"], entry["metric"])
        seen[key] = entry
    deduped = len(entries) - len(seen)
    if deduped > 0:
        print(f"\n  Deduplicated: removed {deduped} duplicate entries")
    entries = list(seen.values())

    # Derive metadata from actual data
    databases = sorted_by_order(
        list({e["database"] for e in entries}), DATABASE_ORDER
    )
    scales = sorted_by_order(
        list({e["scale"] for e in entries}), SCALE_ORDER
    )
    scenarios = sorted_by_order(
        list({e["scenario"] for e in entries}), SCENARIO_ORDER
    )
    key_types = sorted_by_order(
        list({e["keyType"] for e in entries}), KEY_TYPE_ORDER
    )
    metrics = sorted({e["metric"] for e in entries})  # alphabetical
    connections = sorted({e["connections"] for e in entries})  # numerical

    metadata = {
        "databases": databases,
        "scales": scales,
        "scenarios": scenarios,
        "keyTypes": key_types,
        "metrics": metrics,
        "connections": connections,
        "generated": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "entries": entries,
        "metadata": metadata,
        "files_processed": files_processed,
    }


def main():
    print("=" * 60)
    print("UUID Benchmark: CSV -> JSON conversion")
    print("=" * 60)
    print()

    if not os.path.isdir(RESULTS_DIR):
        print(f"ERROR: results directory not found: {RESULTS_DIR}")
        return

    data = build_data()
    files_processed = data.pop("files_processed", 0)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write JSON
    with open(OUTPUT_FILE, "w") as fh:
        json.dump(data, fh, separators=(",", ":"))

    # Summary
    meta = data["metadata"]
    print()
    print("-" * 60)
    print(f"Written: {OUTPUT_FILE}")
    print(f"  Entries:      {len(data['entries'])}")
    print(f"  Files:        {files_processed}")
    print(f"  Databases:    {meta['databases']}")
    print(f"  Scales:       {meta['scales']}")
    print(f"  Scenarios:    {meta['scenarios']}")
    print(f"  Key types:    {meta['keyTypes']}")
    print(f"  Metrics:      {len(meta['metrics'])} ({', '.join(meta['metrics'])})")
    print(f"  Connections:  {meta['connections']}")
    print(f"  Generated:    {meta['generated']}")
    print("-" * 60)


if __name__ == "__main__":
    main()
