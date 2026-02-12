#!/usr/bin/env python3
"""Generate thesis-quality PDF bar charts from UUID benchmark CSV data.

Usage:
    python scripts/plot.py <csv_file> [--output-dir DIR] [--scenario NAME] [--metric NAME]

Reads the stats CSV (Scenario, KeyType, Metric, Median, Mean, StdDev, ...)
and produces one grouped bar chart PDF per (scenario, metric) pair.
"""

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for PDF generation
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

# ---------------------------------------------------------------------------
# Style configuration
# ---------------------------------------------------------------------------

# Try serif fonts suitable for thesis (Palatino-compatible), fall back gracefully
_SERIF_CANDIDATES = ["TeX Gyre Pagella", "Palatino", "Palatino Linotype",
                     "DejaVu Serif", "serif"]
_available_fonts = {f.name for f in fm.fontManager.ttflist}

FONT_FAMILY = "serif"
FONT_SERIF = []
for candidate in _SERIF_CANDIDATES:
    if candidate in _available_fonts:
        FONT_SERIF.append(candidate)
if not FONT_SERIF:
    FONT_SERIF = ["DejaVu Serif"]

matplotlib.rcParams.update({
    "font.family": FONT_FAMILY,
    "font.serif": FONT_SERIF,
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
    "pdf.fonttype": 42,       # TrueType fonts in PDF (better for LaTeX)
    "ps.fonttype": 42,
})

# Consistent 6-color palette across all figures
KEY_TYPE_ORDER = ["SEQUENTIAL", "UUIDV1", "UUIDV4", "UUIDV7", "ULID", "ULID_MONOTONIC"]
KEY_TYPE_LABELS = {
    "SEQUENTIAL":    "SEQUENTIAL",
    "UUIDV1":        "UUIDv1",
    "UUIDV4":        "UUIDv4",
    "UUIDV7":        "UUIDv7",
    "ULID":          "ULID",
    "ULID_MONOTONIC": "ULID-M",
}
KEY_TYPE_COLORS = {
    "SEQUENTIAL":    "#4e79a7",
    "UUIDV1":        "#f28e2b",
    "UUIDV4":        "#e15759",
    "UUIDV7":        "#76b7b2",
    "ULID":          "#59a14f",
    "ULID_MONOTONIC": "#edc948",
}

FIGURE_SIZE = (7, 4)  # inches, fits \textwidth in standard LaTeX

# ---------------------------------------------------------------------------
# Metric / scenario display labels
# ---------------------------------------------------------------------------

METRIC_LABELS = {
    "throughput":          "Throughput (records/sec)",
    "read_throughput":     "Read Throughput (ops/sec)",
    "update_throughput":   "Update Throughput (ops/sec)",
    "overall_throughput":  "Overall Throughput (ops/sec)",
    "p50_latency_us":     "P50 Latency (\u00b5s)",
    "p95_latency_us":     "P95 Latency (\u00b5s)",
    "p99_latency_us":     "P99 Latency (\u00b5s)",
    "cache_hit_ratio":    "Cache Hit Ratio",
    "index_hit_ratio":    "Index Hit Ratio",
    "page_splits":        "Page Splits",
    "fragmentation":      "Fragmentation (%)",
    "avg_leaf_density":   "Avg Leaf Density (%)",
    "table_size_mb":      "Table Size (MB)",
    "index_size_mb":      "Index Size (MB)",
    "read_iops":          "Read IOPS",
    "write_iops":         "Write IOPS",
    "read_throughput_mb": "Read Throughput (MB/s)",
    "write_throughput_mb": "Write Throughput (MB/s)",
}

SCENARIO_TITLES = {
    "insert_performance":  "Insert Performance",
    "read_performance":    "Read Performance",
    "update_performance":  "Update Performance",
    "mixed_insert_heavy":  "Mixed Workload (70% Insert, 30% Read)",
    "mixed_read_update":   "Mixed Workload YCSB-A (50% Read, 50% Update)",
}


def metric_label(metric: str) -> str:
    return METRIC_LABELS.get(metric, metric.replace("_", " ").title())


def scenario_title(scenario: str) -> str:
    return SCENARIO_TITLES.get(scenario, scenario.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_grouped_bars(df_group: pd.DataFrame, scenario: str, metric: str,
                      output_dir: str) -> str:
    """Create a single grouped bar chart for one (scenario, metric) pair.

    Returns the path to the generated PDF.
    """
    # Filter to key types present in data, preserving canonical order
    present_keys = [k for k in KEY_TYPE_ORDER if k in df_group["KeyType"].values]
    if not present_keys:
        return ""

    # Build arrays aligned to present_keys
    medians = []
    stddevs = []
    for kt in present_keys:
        row = df_group[df_group["KeyType"] == kt].iloc[0]
        medians.append(row["Median"])
        stddevs.append(row["StdDev"])

    labels = [KEY_TYPE_LABELS.get(k, k) for k in present_keys]
    colors = [KEY_TYPE_COLORS.get(k, "#999999") for k in present_keys]

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    x = range(len(present_keys))
    bar_width = 0.6
    ax.bar(x, medians, width=bar_width, yerr=stddevs,
           color=colors, edgecolor="white", linewidth=0.5,
           capsize=3, error_kw={"linewidth": 0.8, "capthick": 0.8})

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel(metric_label(metric))
    ax.set_title(scenario_title(scenario))
    ax.yaxis.grid(True, linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    # Start y-axis at 0 unless all values are very close (e.g., cache_hit_ratio ~1.0)
    if min(medians) > 0 and max(medians) > 0:
        # If the smallest bar is more than 80% of the largest, zoom in
        if min(medians) / max(medians) > 0.8:
            margin = max(stddevs) * 2 if stddevs else min(medians) * 0.05
            ax.set_ylim(bottom=max(0, min(medians) - margin))
        else:
            ax.set_ylim(bottom=0)
    else:
        ax.set_ylim(bottom=0)

    plt.tight_layout()

    filename = f"{scenario}_{metric}.pdf"
    filepath = os.path.join(output_dir, filename)
    fig.savefig(filepath)
    plt.close(fig)
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate thesis-quality PDF bar charts from benchmark CSV.")
    parser.add_argument("csv_file", help="Path to stats CSV (results.csv)")
    parser.add_argument("--output-dir", default="plots",
                        help="Output directory for PDFs (default: plots/)")
    parser.add_argument("--scenario", default=None,
                        help="Filter to one scenario (e.g., insert_performance)")
    parser.add_argument("--metric", default=None,
                        help="Filter to one metric (e.g., p99_latency_us)")
    args = parser.parse_args()

    # Read CSV
    df = pd.read_csv(args.csv_file, skipinitialspace=True)

    # Validate expected columns
    required = {"Scenario", "KeyType", "Metric", "Median", "StdDev"}
    missing = required - set(df.columns)
    if missing:
        print(f"Error: CSV missing columns: {missing}", file=sys.stderr)
        sys.exit(1)

    # Drop rows where Median is NaN (metric could not be collected)
    df = df.dropna(subset=["Median"])
    # Fill NaN StdDev with 0 (single-run or uncollectable)
    df["StdDev"] = df["StdDev"].fillna(0)

    # Apply filters
    if args.scenario:
        df = df[df["Scenario"] == args.scenario]
        if df.empty:
            print(f"Error: no data for scenario '{args.scenario}'", file=sys.stderr)
            sys.exit(1)

    if args.metric:
        df = df[df["Metric"] == args.metric]
        if df.empty:
            print(f"Error: no data for metric '{args.metric}'", file=sys.stderr)
            sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    # Group by (Scenario, Metric) and generate one chart each
    groups = df.groupby(["Scenario", "Metric"])
    generated = []

    for (scenario, metric), group_df in groups:
        path = plot_grouped_bars(group_df, scenario, metric, args.output_dir)
        if path:
            generated.append(path)

    print(f"Generated {len(generated)} plots in {args.output_dir}/")
    for p in sorted(generated):
        print(f"  {p}")


if __name__ == "__main__":
    main()
