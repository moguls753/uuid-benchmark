"""Validated single-client point-lookup inputs for Section 5.3.

read_iops is an exported process-window rate, not an I/O count per lookup.
Zero is accepted as an exported value, not proof of full cache residency.
"""

import csv
import math
import statistics as st

from part_one_insert import ENGINES, RESULTS_DEFAULT, SCALES, key_types

METRICS = ("read_throughput", "read_iops")
ENGINE_WORDS = {"postgres": "Pg", "mysql": "Mysql", "mongodb": "Mongo", "cassandra": "Cass"}
SCALE_WORDS = {"100k": "HundredK", "1m": "OneM", "10m": "TenM"}
KEY_WORDS = {"UUIDV1": "VOne", "UUIDV7": "VSeven", "ULID": "Ulid",
             "ULID_MONOTONIC": "UlidMono", "UUIDV4": "VFour", "OBJECTID": "ObjectId"}


def load_reads(results=RESULTS_DEFAULT):
    data = {}
    for engine in ENGINES:
        for scale, count in SCALES:
            candidates = [results / f"{engine}_{scale}_{tag}_1conn_raw.csv"
                          for tag in ("read-performance", "all")]
            existing = [path for path in candidates if path.exists()]
            if len(existing) != 1:
                raise ValueError(f"{engine}/{scale}: expected one read input, got {existing}")
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
                    if row["Scenario"] != "read_performance" or metric not in METRICS:
                        continue
                    key = row["KeyType"]
                    if key in groups[metric]:
                        raise ValueError(f"{path}: duplicate {metric}/{key}")
                    if int(row["RecordCount"]) != count or int(row["Connections"]) != 1:
                        raise ValueError(f"{path}: unexpected scale or client count")
                    values = [float(row[f"Run{i}"]) for i in range(1, 6)]
                    if not all(math.isfinite(v) and v >= 0 for v in values):
                        raise ValueError(f"{path}: invalid {metric}/{key}")
                    if metric == "read_throughput" and min(values) <= 0:
                        raise ValueError(f"{path}: nonpositive throughput/{key}")
                    groups[metric][key] = values
            for metric, runs in groups.items():
                if set(runs) != set(key_types(engine)):
                    raise ValueError(f"{path}: unexpected types for {metric}: {sorted(runs)}")
            data[engine, scale] = groups
    return data


def read_macros(data):
    """Return name/value pairs; ratios use unrounded per-scheme medians."""
    macros = []
    other_one_m = []
    for engine in ENGINES:
        for scale, _ in SCALES:
            groups = data[engine, scale]
            prefix = f"SingleRead{ENGINE_WORDS[engine]}{SCALE_WORDS[scale]}"
            baseline = st.median(groups["read_throughput"]["SEQUENTIAL"])
            macros.append((prefix + "Seq", f"{baseline:,.0f}"))
            for key in key_types(engine)[1:]:
                percent = 100 * st.median(groups["read_throughput"][key]) / baseline
                macros.append((prefix + KEY_WORDS[key], f"{percent:.1f}"))
                if engine != "postgres" and scale == "1m":
                    other_one_m.append(percent)
            rates = [v for values in groups["read_iops"].values() for v in values]
            macros.extend(((prefix + "IoMin", f"{min(rates):,.2f}"),
                           (prefix + "IoMax", f"{max(rates):,.2f}")))
    macros.extend((("SingleReadOtherOneMMin", f"{min(other_one_m):.1f}"),
                   ("SingleReadOtherOneMMax", f"{max(other_one_m):.1f}")))
    return macros
