#!/usr/bin/env python3
"""Generate PDF/PGF bar charts and heatmaps from UUID benchmark CSV data.

Usage:
    # Single database (normalized + regular bars)
    python scripts/plot.py results.csv --output-dir plots/

    # Cross-database comparison (grouped bars + heatmaps)
    python scripts/plot.py --cross-db postgres:pg.csv mysql:my.csv --output-dir plots/

    # Filters
    python scripts/plot.py results.csv --scenario insert_performance --metric throughput
"""

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for PDF generation
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Style configuration -- LaTeX integration via text.usetex
# ---------------------------------------------------------------------------

matplotlib.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "font.serif": ["TeX Gyre Pagella"],
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
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})

# Consistent 6-color palette across all figures (Tableau-derived)
KEY_TYPE_ORDER = ["SEQUENTIAL", "UUIDV1", "UUIDV4", "UUIDV7", "ULID", "ULID_MONOTONIC"]
KEY_TYPE_LABELS = {
    "SEQUENTIAL":     "SEQUENTIAL",
    "UUIDV1":         "UUIDv1",
    "UUIDV4":         "UUIDv4",
    "UUIDV7":         "UUIDv7",
    "ULID":           "ULID",
    "ULID_MONOTONIC": "ULID-M",
}
KEY_TYPE_COLORS = {
    "SEQUENTIAL":     "#4e79a7",
    "UUIDV1":         "#f28e2b",
    "UUIDV4":         "#e15759",
    "UUIDV7":         "#76b7b2",
    "ULID":           "#59a14f",
    "ULID_MONOTONIC": "#edc948",
}

FIGURE_SIZE = (7, 4)
FIGURE_SIZE_WIDE = (9, 4)

# ---------------------------------------------------------------------------
# Metrics that get normalized to SEQUENTIAL = 100%
# ---------------------------------------------------------------------------

NORMALIZED_METRICS = {
    "throughput", "read_throughput", "update_throughput", "overall_throughput",
    "p50_latency_us", "p95_latency_us", "p99_latency_us",
}

LATENCY_METRICS = {"p50_latency_us", "p95_latency_us", "p99_latency_us"}

# ---------------------------------------------------------------------------
# Metric / scenario display labels (LaTeX-safe with text.usetex)
# ---------------------------------------------------------------------------

METRIC_LABELS = {
    "throughput":          "Throughput (records/sec)",
    "read_throughput":     "Read Throughput (ops/sec)",
    "update_throughput":   "Update Throughput (ops/sec)",
    "overall_throughput":  "Overall Throughput (ops/sec)",
    "p50_latency_us":      r"P50 Latency ($\mu$s)",
    "p95_latency_us":      r"P95 Latency ($\mu$s)",
    "p99_latency_us":      r"P99 Latency ($\mu$s)",
    "cache_hit_ratio":    "Cache Hit Ratio",
    "index_hit_ratio":    "Index Hit Ratio",
    "page_splits":        "Page Splits",
    "fragmentation":       r"Fragmentation (\%)",
    "avg_leaf_density":    r"Avg Leaf Density (\%)",
    "table_size_mb":      "Table Size (MB)",
    "index_size_mb":      "Index Size (MB)",
    "read_iops":          "Read IOPS",
    "write_iops":         "Write IOPS",
    "read_throughput_mb": "Read Throughput (MB/s)",
    "write_throughput_mb": "Write Throughput (MB/s)",
}

NORMALIZED_YLABEL = {
    "throughput":          r"Relative Performance (\%)",
    "read_throughput":     r"Relative Performance (\%)",
    "update_throughput":   r"Relative Performance (\%)",
    "overall_throughput":  r"Relative Performance (\%)",
    "p50_latency_us":      r"Relative Latency (\%)",
    "p95_latency_us":      r"Relative Latency (\%)",
    "p99_latency_us":      r"Relative Latency (\%)",
}

SCENARIO_TITLES = {
    "insert_performance":  "Insert Performance",
    "read_performance":    "Read Performance",
    "update_performance":  "Update Performance",
    "mixed_insert_heavy":  r"Mixed Workload (70\% Insert, 30\% Read)",
    "mixed_read_update":   r"Mixed Workload YCSB-A (50\% Read, 50\% Update)",
}


def metric_label(metric: str) -> str:
    return METRIC_LABELS.get(metric, metric.replace("_", " ").title())


def scenario_title(scenario: str) -> str:
    return SCENARIO_TITLES.get(scenario, scenario.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _apply_style(ax):
    """Remove top/right spines, lighter y-grid."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)


def _save(fig, filepath):
    """Save as PDF and PGF side by side."""
    fig.savefig(filepath)
    fig.savefig(filepath.replace(".pdf", ".pgf"))
    plt.close(fig)


def _extract_keyed_data(df_group):
    """Return (present_keys, medians_dict, stddevs_dict) from a group DataFrame."""
    present_keys = [k for k in KEY_TYPE_ORDER if k in df_group["KeyType"].values]
    medians = {}
    stddevs = {}
    for kt in present_keys:
        row = df_group[df_group["KeyType"] == kt].iloc[0]
        medians[kt] = row["Median"]
        stddevs[kt] = row["StdDev"]
    return present_keys, medians, stddevs


# ---------------------------------------------------------------------------
# Single-database plotting
# ---------------------------------------------------------------------------

def plot_bars(df_group: pd.DataFrame, scenario: str, metric: str,
              output_dir: str) -> str:
    """Regular bar chart for one (scenario, metric) pair."""
    present_keys, medians, stddevs = _extract_keyed_data(df_group)
    if not present_keys:
        return ""

    med_vals = [medians[k] for k in present_keys]
    std_vals = [stddevs[k] for k in present_keys]
    labels = [KEY_TYPE_LABELS.get(k, k) for k in present_keys]
    colors = [KEY_TYPE_COLORS.get(k, "#999999") for k in present_keys]

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    x = range(len(present_keys))
    ax.bar(x, med_vals, width=0.6, yerr=std_vals,
           color=colors, edgecolor="white", linewidth=0.5,
           capsize=3, error_kw={"linewidth": 0.8, "capthick": 0.8})

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel(metric_label(metric))
    ax.set_title(scenario_title(scenario))
    _apply_style(ax)

    if min(med_vals) > 0 and max(med_vals) > 0:
        if min(med_vals) / max(med_vals) > 0.8:
            margin = max(std_vals) * 2 if std_vals else min(med_vals) * 0.05
            ax.set_ylim(bottom=max(0, min(med_vals) - margin))
        else:
            ax.set_ylim(bottom=0)
    else:
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{scenario}_{metric}.pdf")
    _save(fig, filepath)
    return filepath


def plot_normalized_bars(df_group: pd.DataFrame, scenario: str, metric: str,
                         output_dir: str) -> str:
    """Normalized bar chart where SEQUENTIAL = 100%."""
    present_keys, medians, stddevs = _extract_keyed_data(df_group)
    if not present_keys:
        return ""

    # Need SEQUENTIAL as baseline
    if "SEQUENTIAL" not in medians or medians["SEQUENTIAL"] == 0:
        return plot_bars(df_group, scenario, metric, output_dir)

    baseline = medians["SEQUENTIAL"]

    # Exclude SEQUENTIAL from the bars (it's always 100% by definition)
    plot_keys = [k for k in present_keys if k != "SEQUENTIAL"]
    if not plot_keys:
        return ""

    norm_vals = [medians[k] / baseline * 100 for k in plot_keys]
    norm_stds = [stddevs[k] / baseline * 100 for k in plot_keys]
    labels = [KEY_TYPE_LABELS.get(k, k) for k in plot_keys]
    colors = [KEY_TYPE_COLORS.get(k, "#999999") for k in plot_keys]

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    x = range(len(plot_keys))
    ax.bar(x, norm_vals, width=0.6, yerr=norm_stds,
           color=colors, edgecolor="white", linewidth=0.5,
           capsize=3, error_kw={"linewidth": 0.8, "capthick": 0.8})

    # 100% baseline reference line
    ax.axhline(y=100, color="#4e79a7", linestyle="--", linewidth=1.0,
               alpha=0.7, label="Sequential baseline")

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel(NORMALIZED_YLABEL.get(metric, r"Relative (\%)"))
    ax.set_title(scenario_title(scenario))
    ax.legend(loc="best", framealpha=0.8)
    _apply_style(ax)

    # Y-axis: include 100% line with some padding
    all_vals = norm_vals + [100]
    all_err = norm_stds + [0]
    y_max = max(v + e for v, e in zip(all_vals, all_err)) * 1.1
    y_min = min(v - e for v, e in zip(all_vals, all_err))
    ax.set_ylim(bottom=max(0, y_min * 0.9), top=y_max)

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{scenario}_{metric}.pdf")
    _save(fig, filepath)
    return filepath


def plot_single_db(df, scenario, metric, output_dir):
    """Dispatch to normalized or regular bars based on metric."""
    if metric in NORMALIZED_METRICS:
        return plot_normalized_bars(df, scenario, metric, output_dir)
    return plot_bars(df, scenario, metric, output_dir)


# ---------------------------------------------------------------------------
# Cross-database plotting
# ---------------------------------------------------------------------------

def plot_cross_db_bars(cross_df: pd.DataFrame, scenario: str, metric: str,
                       db_labels: list, output_dir: str) -> str:
    """Grouped normalized bars across databases."""
    # Filter to this scenario+metric
    sdf = cross_df[(cross_df["Scenario"] == scenario) & (cross_df["Metric"] == metric)]
    if sdf.empty:
        return ""

    # Determine key types present across all databases (excluding SEQUENTIAL)
    all_key_types = [k for k in KEY_TYPE_ORDER
                     if k != "SEQUENTIAL" and k in sdf["KeyType"].values]
    if not all_key_types:
        return ""

    # Normalize each database to its own SEQUENTIAL baseline
    norm_data = {}  # {db: {kt: (norm_val, norm_std)}}
    for db in db_labels:
        db_df = sdf[sdf["Database"] == db]
        if db_df.empty:
            continue
        seq_row = db_df[db_df["KeyType"] == "SEQUENTIAL"]
        if seq_row.empty or seq_row.iloc[0]["Median"] == 0:
            # No SEQUENTIAL baseline -- use raw values
            for kt in all_key_types:
                kt_row = db_df[db_df["KeyType"] == kt]
                if not kt_row.empty:
                    norm_data.setdefault(db, {})[kt] = (
                        kt_row.iloc[0]["Median"], kt_row.iloc[0]["StdDev"])
            continue
        baseline = seq_row.iloc[0]["Median"]
        for kt in all_key_types:
            kt_row = db_df[db_df["KeyType"] == kt]
            if not kt_row.empty:
                norm_data.setdefault(db, {})[kt] = (
                    kt_row.iloc[0]["Median"] / baseline * 100,
                    kt_row.iloc[0]["StdDev"] / baseline * 100,
                )

    dbs_with_data = [db for db in db_labels if db in norm_data]
    if not dbs_with_data:
        return ""

    n_dbs = len(dbs_with_data)
    n_keys = len(all_key_types)
    bar_width = 0.8 / n_keys
    x = np.arange(n_dbs)

    fig, ax = plt.subplots(figsize=FIGURE_SIZE_WIDE)

    for i, kt in enumerate(all_key_types):
        vals = []
        errs = []
        for db in dbs_with_data:
            v, e = norm_data[db].get(kt, (0, 0))
            vals.append(v)
            errs.append(e)
        offset = (i - (n_keys - 1) / 2) * bar_width
        ax.bar(x + offset, vals, width=bar_width, yerr=errs,
               color=KEY_TYPE_COLORS.get(kt, "#999999"),
               edgecolor="white", linewidth=0.5,
               capsize=2, error_kw={"linewidth": 0.6, "capthick": 0.6},
               label=KEY_TYPE_LABELS.get(kt, kt))

    # 100% baseline
    has_normalized = any(
        sdf[(sdf["Database"] == db) & (sdf["KeyType"] == "SEQUENTIAL")].shape[0] > 0
        for db in dbs_with_data
    )
    if has_normalized:
        ax.axhline(y=100, color="#4e79a7", linestyle="--", linewidth=1.0,
                   alpha=0.7, label="Sequential baseline")

    ax.set_xticks(x)
    ax.set_xticklabels(dbs_with_data)
    ylabel = NORMALIZED_YLABEL.get(metric, metric_label(metric))
    ax.set_ylabel(ylabel)
    ax.set_title(scenario_title(scenario))
    ax.legend(loc="best", framealpha=0.8, ncol=min(n_keys, 4))
    _apply_style(ax)

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"crossdb_{scenario}_{metric}.pdf")
    _save(fig, filepath)
    return filepath


def plot_heatmap(cross_df: pd.DataFrame, scenario: str, metric: str,
                 db_labels: list, output_dir: str) -> str:
    """Heatmap: databases (rows) x key types (columns), normalized to SEQUENTIAL."""
    sdf = cross_df[(cross_df["Scenario"] == scenario) & (cross_df["Metric"] == metric)]
    if sdf.empty:
        return ""

    all_key_types = [k for k in KEY_TYPE_ORDER
                     if k != "SEQUENTIAL" and k in sdf["KeyType"].values]
    if not all_key_types:
        return ""

    # Build matrix
    dbs_with_data = []
    matrix = []
    for db in db_labels:
        db_df = sdf[sdf["Database"] == db]
        if db_df.empty:
            continue
        seq_row = db_df[db_df["KeyType"] == "SEQUENTIAL"]
        if seq_row.empty or seq_row.iloc[0]["Median"] == 0:
            continue  # Skip databases without SEQUENTIAL baseline for heatmap
        baseline = seq_row.iloc[0]["Median"]
        row = []
        for kt in all_key_types:
            kt_row = db_df[db_df["KeyType"] == kt]
            if not kt_row.empty:
                row.append(kt_row.iloc[0]["Median"] / baseline * 100)
            else:
                row.append(np.nan)
        matrix.append(row)
        dbs_with_data.append(db)

    if not dbs_with_data or not matrix:
        return ""

    data = np.array(matrix)
    col_labels = [KEY_TYPE_LABELS.get(k, k) for k in all_key_types]

    fig, ax = plt.subplots(figsize=(max(6, len(all_key_types) * 1.2 + 1),
                                    max(3, len(dbs_with_data) * 0.8 + 1)))

    # Color map: for throughput, higher is better (green); for latency, lower is better
    if metric in LATENCY_METRICS:
        cmap = "RdYlGn_r"  # Red = high latency (bad), green = low (good)
    else:
        cmap = "RdYlGn"    # Green = high throughput (good), red = low (bad)

    # Center colormap at 100%
    vmin = np.nanmin(data)
    vmax = np.nanmax(data)
    # Symmetric range around 100
    extent = max(abs(vmin - 100), abs(vmax - 100), 10)
    im = ax.imshow(data, cmap=cmap, aspect="auto",
                   vmin=100 - extent, vmax=100 + extent)

    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha="right")
    ax.set_yticks(range(len(dbs_with_data)))
    ax.set_yticklabels(dbs_with_data)

    # Annotate cells
    for i in range(len(dbs_with_data)):
        for j in range(len(all_key_types)):
            val = data[i, j]
            if not np.isnan(val):
                color = "white" if abs(val - 100) > extent * 0.6 else "black"
                ax.text(j, i, f"{val:.0f}\\%", ha="center", va="center",
                        color=color, fontsize=9)

    ax.set_title(f"{scenario_title(scenario)} -- {metric_label(metric)}")
    fig.colorbar(im, ax=ax, label=NORMALIZED_YLABEL.get(metric, r"Relative (\%)"),
                 shrink=0.8)

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{scenario}_{metric}_heatmap.pdf")
    _save(fig, filepath)
    return filepath


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_csv(path: str) -> pd.DataFrame:
    """Load and validate a benchmark CSV."""
    df = pd.read_csv(path, skipinitialspace=True)
    required = {"Scenario", "KeyType", "Metric", "Median", "StdDev"}
    missing = required - set(df.columns)
    if missing:
        print(f"Error: CSV '{path}' missing columns: {missing}", file=sys.stderr)
        sys.exit(1)
    df = df.dropna(subset=["Median"])
    df["StdDev"] = df["StdDev"].fillna(0)
    return df


def parse_cross_db_args(cross_db_args: list) -> list:
    """Parse 'label:path' pairs, return list of (label, path)."""
    pairs = []
    for arg in cross_db_args:
        if ":" not in arg:
            print(f"Error: --cross-db argument '{arg}' must be LABEL:CSV_PATH",
                  file=sys.stderr)
            sys.exit(1)
        label, path = arg.split(":", 1)
        pairs.append((label, path))
    return pairs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate PDF/PGF bar charts and heatmaps from benchmark CSV.")
    parser.add_argument("csv_file", nargs="?", default=None,
                        help="Path to stats CSV (single-database mode)")
    parser.add_argument("--cross-db", nargs="+", metavar="LABEL:CSV",
                        help="Cross-database mode: postgres:pg.csv mysql:my.csv ...")
    parser.add_argument("--output-dir", default="plots",
                        help="Output directory for PDFs/PGFs (default: plots/)")
    parser.add_argument("--scenario", default=None,
                        help="Filter to one scenario (e.g., insert_performance)")
    parser.add_argument("--metric", default=None,
                        help="Filter to one metric (e.g., p99_latency_us)")
    args = parser.parse_args()

    if not args.csv_file and not args.cross_db:
        parser.error("Provide either a CSV file or --cross-db arguments")

    os.makedirs(args.output_dir, exist_ok=True)
    generated = []

    if args.cross_db:
        # ----- Cross-database mode -----
        pairs = parse_cross_db_args(args.cross_db)
        frames = []
        db_labels = []
        for label, path in pairs:
            df = load_csv(path)
            df["Database"] = label
            frames.append(df)
            db_labels.append(label)
        cross_df = pd.concat(frames, ignore_index=True)

        if args.scenario:
            cross_df = cross_df[cross_df["Scenario"] == args.scenario]
        if args.metric:
            cross_df = cross_df[cross_df["Metric"] == args.metric]

        for (scenario, metric), _ in cross_df.groupby(["Scenario", "Metric"]):
            path = plot_cross_db_bars(cross_df, scenario, metric,
                                      db_labels, args.output_dir)
            if path:
                generated.append(path)
            path = plot_heatmap(cross_df, scenario, metric,
                                db_labels, args.output_dir)
            if path:
                generated.append(path)
    else:
        # ----- Single-database mode -----
        df = load_csv(args.csv_file)

        if args.scenario:
            df = df[df["Scenario"] == args.scenario]
            if df.empty:
                print(f"Error: no data for scenario '{args.scenario}'",
                      file=sys.stderr)
                sys.exit(1)
        if args.metric:
            df = df[df["Metric"] == args.metric]
            if df.empty:
                print(f"Error: no data for metric '{args.metric}'",
                      file=sys.stderr)
                sys.exit(1)

        for (scenario, metric), group_df in df.groupby(["Scenario", "Metric"]):
            path = plot_single_db(group_df, scenario, metric, args.output_dir)
            if path:
                generated.append(path)

    print(f"Generated {len(generated)} plots in {args.output_dir}/")
    for p in sorted(generated):
        print(f"  {p}")


if __name__ == "__main__":
    main()
