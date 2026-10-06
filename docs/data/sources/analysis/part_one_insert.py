"""Validated part-one insert inputs shared by the figure and macro generator."""

import csv
import math
import statistics as st
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DEFAULT = ROOT.parent / "uuid-benchmark" / "results" / "laptop"
ENGINES = ("postgres", "mysql", "mongodb", "cassandra")
SCALES = (("100k", 100_000), ("1m", 1_000_000), ("10m", 10_000_000))
TYPES = ("SEQUENTIAL", "UUIDV1", "UUIDV7", "ULID", "ULID_MONOTONIC", "UUIDV4")


def key_types(engine):
    return TYPES + (("OBJECTID",) if engine == "mongodb" else ())


def load_inserts(results=RESULTS_DEFAULT):
    """Return (engine, scale) -> key -> five raw throughputs; reject ambiguity."""
    data = {}
    for engine in ENGINES:
        for scale, count in SCALES:
            candidates = [results / f"{engine}_{scale}_{tag}_1conn_raw.csv"
                          for tag in ("insert-performance", "all")]
            existing = [path for path in candidates if path.exists()]
            if len(existing) != 1:
                raise ValueError(f"{engine}/{scale}: expected one input, got {existing}")
            path = existing[0]
            with path.open(newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
                if len(headers) != len(set(headers)):
                    raise ValueError(f"{path}: duplicate column names")
                required = {"Scenario", "Metric", "KeyType", "RecordCount", "Connections"}
                if not required.issubset(headers):
                    raise ValueError(f"{path}: missing metadata columns")
                run_cols = [c for c in headers if c.startswith("Run")]
                if set(run_cols) != {f"Run{i}" for i in range(1, 6)}:
                    raise ValueError(f"{path}: expected exactly Run1–Run5")
                runs = {}
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"{path}: row width differs from header")
                    if (row["Scenario"], row["Metric"]) != ("insert_performance", "throughput"):
                        continue
                    key = row["KeyType"]
                    if key in runs:
                        raise ValueError(f"{path}: duplicate throughput row for {key}")
                    if int(row["RecordCount"]) != count or int(row["Connections"]) != 1:
                        raise ValueError(f"{path}: unexpected scale or client count")
                    values = [float(row[f"Run{i}"]) for i in range(1, 6)]
                    if not all(math.isfinite(v) and v > 0 for v in values):
                        raise ValueError(f"{path}: nonpositive/nonfinite throughput for {key}")
                    runs[key] = values
            if set(runs) != set(key_types(engine)):
                raise ValueError(f"{path}: unexpected key types {sorted(runs)}")
            data[engine, scale] = runs
    return data


def normalized(runs, key):
    """Median and observed extrema divided by the same sequential median."""
    baseline = st.median(runs["SEQUENTIAL"])
    values = runs[key]
    return st.median(values) / baseline, min(values) / baseline, max(values) / baseline
