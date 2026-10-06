"""Validated PostgreSQL insert structure at 1M and 10M for Section 5.

The split metric counts instance-wide WAL B-tree split records. Density and
fragmentation describe the primary index after loading; index_size_mb stores
MiB despite its historical name. No wrap-event timing is inferred here.
"""

import csv
import math

from part_one_insert import RESULTS_DEFAULT, TYPES

SCALES = (("1m", 1_000_000), ("10m", 10_000_000))
METRICS = ("page_splits", "avg_leaf_density", "fragmentation", "index_size_mb")


def load_structure(results=RESULTS_DEFAULT):
    """Return scale -> metric -> key -> five values; validate before plotting."""
    data = {}
    for scale, count in SCALES:
        candidates = [results / f"postgres_{scale}_{tag}_1conn_raw.csv"
                      for tag in ("insert-performance", "all")]
        existing = [path for path in candidates if path.exists()]
        if len(existing) != 1:
            raise ValueError(f"postgres/{scale}: expected one input, got {existing}")
        path = existing[0]
        groups = {metric: {} for metric in METRICS}
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            headers = reader.fieldnames or []
            if len(headers) != len(set(headers)):
                raise ValueError(f"{path}: duplicate column names")
            required = {"Scenario", "Metric", "KeyType", "RecordCount", "Connections"}
            if not required.issubset(headers):
                raise ValueError(f"{path}: missing metadata columns")
            if {c for c in headers if c.startswith("Run")} != {f"Run{i}" for i in range(1, 6)}:
                raise ValueError(f"{path}: expected exactly Run1–Run5")
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"{path}: row width differs from header")
                metric = row["Metric"]
                if row["Scenario"] != "insert_performance" or metric not in METRICS:
                    continue
                key = row["KeyType"]
                if key in groups[metric]:
                    raise ValueError(f"{path}: duplicate {metric}/{key}")
                if int(row["RecordCount"]) != count or int(row["Connections"]) != 1:
                    raise ValueError(f"{path}: unexpected scale or client count")
                values = [float(row[f"Run{i}"]) for i in range(1, 6)]
                if not all(math.isfinite(v) and v >= 0 for v in values):
                    raise ValueError(f"{path}: invalid {metric}/{key}")
                if metric in ("page_splits", "index_size_mb") and min(values) <= 0:
                    raise ValueError(f"{path}: nonpositive {metric}/{key}")
                if metric == "page_splits" and any(not v.is_integer() for v in values):
                    raise ValueError(f"{path}: noninteger split count")
                if metric in ("avg_leaf_density", "fragmentation") and max(values) > 100:
                    raise ValueError(f"{path}: percentage outside 0–100")
                groups[metric][key] = values
        for metric, runs in groups.items():
            if set(runs) != set(TYPES):
                raise ValueError(f"{path}: unexpected types for {metric}: {sorted(runs)}")
        data[scale] = groups
    return data
