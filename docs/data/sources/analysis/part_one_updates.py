"""Validated historical single-client U/RU inputs for Section 5.4.

RU RecordCount=1M is metadata, not the 500K preload target.
These comparisons describe attempted-operation throughput, within an engine.
Selected IH is loaded independently by part_one_ih.
"""

import csv
import math
import statistics as st

from part_one_insert import ENGINES, RESULTS_DEFAULT, key_types
from part_one_reads import ENGINE_WORDS, KEY_WORDS

# Endpoint, file scale, exported RecordCount, scenario, metric, candidate tags.
UPDATE_ENDPOINTS = (
    ("update_1m", "1m", 1_000_000, "update_performance", "update_throughput",
     ("update-performance", "all")),
    ("update_10m", "10m", 10_000_000, "update_performance", "update_throughput",
     ("update-performance", "all")),
    ("read_update", "1m", 1_000_000, "mixed_read_update", "overall_throughput",
     ("all",)),
)
ENDPOINTS = UPDATE_ENDPOINTS + (
    ("insert_heavy", "1m", 1_000_000, "mixed_insert_heavy", "overall_throughput",
     ("all",)),
)


def load_updates(results=RESULTS_DEFAULT):
    """Return (engine, endpoint) -> key -> five throughputs; reject ambiguity."""
    data = {}
    for engine in ENGINES:
        for endpoint, scale, count, scenario, metric, tags in UPDATE_ENDPOINTS:
            paths = [results / f"{engine}_{scale}_{tag}_1conn_raw.csv" for tag in tags]
            paths = [path for path in paths if path.exists()]
            if len(paths) != 1:
                raise ValueError(f"{engine}/{endpoint}: expected one input, got {paths}")
            path = paths[0]
            runs = {}
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
                    if (row["Scenario"], row["Metric"]) != (scenario, metric):
                        continue
                    key = row["KeyType"]
                    if key in runs:
                        raise ValueError(f"{path}: duplicate {metric}/{key}")
                    if int(row["RecordCount"]) != count or int(row["Connections"]) != 1:
                        raise ValueError(f"{path}: unexpected configuration metadata")
                    values = [float(row[f"Run{i}"]) for i in range(1, 6)]
                    if not all(math.isfinite(v) and v > 0 for v in values):
                        raise ValueError(f"{path}: nonpositive/nonfinite {metric}/{key}")
                    runs[key] = values
            if set(runs) != set(key_types(engine)):
                raise ValueError(f"{path}: unexpected types for {scenario}: {sorted(runs)}")
            data[engine, endpoint] = runs
    return data


def update_macro_prefix(engine, endpoint):
    word = ENGINE_WORDS[engine]
    return {"update_1m": f"SingleUpdate{word}OneM",
            "update_10m": f"SingleUpdate{word}TenM",
            "read_update": f"SingleReadUpdate{word}",
            "insert_heavy": f"SingleInsertHeavy{word}"}[endpoint]


def update_macros(data):
    """75 table values and four prose range endpoints, from unrounded medians."""
    macros = []
    ranges = {"update_1m": [], "read_update": []}
    for engine in ENGINES:
        for endpoint, *_ in UPDATE_ENDPOINTS:
            runs = data[engine, endpoint]
            prefix = update_macro_prefix(engine, endpoint)
            baseline = st.median(runs["SEQUENTIAL"])
            macros.append((prefix + "Seq", f"{baseline:,.0f}"))
            for key in key_types(engine)[1:]:
                percent = 100 * st.median(runs[key]) / baseline
                macros.append((prefix + KEY_WORDS[key], f"{percent:.1f}"))
                if endpoint in ranges:
                    ranges[endpoint].append(percent)
    for endpoint, name in (("update_1m", "SingleUpdateOneM"),
                           ("read_update", "SingleReadUpdate")):
        macros.extend(((name + "Min", f"{min(ranges[endpoint]):.1f}"),
                       (name + "Max", f"{max(ranges[endpoint]):.1f}")))
    return macros


def insert_heavy_macros(data):
    """Archival helper only; NOT used for the selected IH analysis."""
    macros = []
    other_ratios = []
    for engine in ENGINES:
        runs = data[engine, "insert_heavy"]
        prefix = update_macro_prefix(engine, "insert_heavy")
        baseline = st.median(runs["SEQUENTIAL"])
        macros.append((prefix + "Seq", f"{baseline:,.0f}"))
        for key in key_types(engine)[1:]:
            percent = 100 * st.median(runs[key]) / baseline
            macros.append((prefix + KEY_WORDS[key], f"{percent:.1f}"))
            if engine != "mongodb":
                other_ratios.append(percent)
    macros.extend((("SingleInsertHeavyOtherMin", f"{min(other_ratios):.1f}"),
                   ("SingleInsertHeavyOtherMax", f"{max(other_ratios):.1f}")))
    return macros
