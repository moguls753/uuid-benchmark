package display

import (
	"fmt"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

func InsertPerformanceStatistics(results map[string]map[string]statistics.Stats, keyTypes []string, numRecords, connections, batchSize, numRuns int, database string) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("Insert Performance - Statistical Summary (%d runs per UUID type)\n", numRuns)
	fmt.Println(strings.Repeat("=", 100))

	fmt.Println("\nThroughput (records/sec)")
	displayMetricTable(results, keyTypes, "throughput", "%.0f")
	displayComparisons(results, keyTypes, "throughput")

	// Page Splits (B-tree databases only)
	if database != "cassandra" {
		fmt.Println("\nPage Splits")
		displayMetricTable(results, keyTypes, "page_splits", "%.0f")
		displayComparisons(results, keyTypes, "page_splits")
	}

	// SSTable Count (Cassandra only)
	if database == "cassandra" {
		fmt.Println("\nSSTable Count")
		displayMetricTable(results, keyTypes, "sstable_count", "%.0f")
		displayComparisons(results, keyTypes, "sstable_count")

		fmt.Println("\nSpace Amplification (%)")
		displayMetricTable(results, keyTypes, "fragmentation", "%.2f")
		displayComparisons(results, keyTypes, "fragmentation")
	}

	// Leaf Fragmentation (PostgreSQL only)
	if database == "postgres" {
		fmt.Println("\nLeaf Fragmentation (%)")
		displayMetricTable(results, keyTypes, "fragmentation", "%.2f")
		displayComparisons(results, keyTypes, "fragmentation")

		fmt.Println("\nAvg Leaf Density (%)")
		displayMetricTable(results, keyTypes, "avg_leaf_density", "%.2f")
		displayComparisons(results, keyTypes, "avg_leaf_density")
	}

	fmt.Println("\nTable Size (MB)")
	displayMetricTable(results, keyTypes, "table_size_mb", "%.1f")
	displayComparisons(results, keyTypes, "table_size_mb")

	fmt.Println("\nIndex Size (MB)")
	displayMetricTable(results, keyTypes, "index_size_mb", "%.1f")
	displayComparisons(results, keyTypes, "index_size_mb")

	fmt.Println("\nLatency P99 (µs)")
	displayMetricTable(results, keyTypes, "p99_latency_us", "%.0f")
	displayComparisons(results, keyTypes, "p99_latency_us")

	fmt.Println("\nWrite IOPS")
	displayMetricTable(results, keyTypes, "write_iops", "%.0f")
	displayComparisons(results, keyTypes, "write_iops")
}

func displayMetricTable(results map[string]map[string]statistics.Stats, keyTypes []string, metric, format string) {
	// Compute column widths dynamically
	keyW := len("Key Type")
	for _, kt := range keyTypes {
		if l := len(strings.ToUpper(kt)); l > keyW {
			keyW = l
		}
	}

	valW := len("Median") // minimum value column width
	for _, kt := range keyTypes {
		s := results[kt][metric]
		for _, v := range []float64{s.Median, s.Mean, s.StdDev, s.Min, s.Max} {
			if l := len(fmt.Sprintf(format, v)); l > valW {
				valW = l
			}
		}
	}
	if valW < 8 {
		valW = 8
	}

	cvW := 7 // CV column content width (fits "CV %" header and values like " 75.5")

	// Build format strings — each cell is │ + space + content + space + │
	// so the rule segment between ┬/┼ chars = content_width + 2
	hRule := func(left, mid, right, fill string) string {
		return left + strings.Repeat(fill, keyW+2) +
			mid + strings.Repeat(fill, valW+2) +
			mid + strings.Repeat(fill, valW+2) +
			mid + strings.Repeat(fill, valW+2) +
			mid + strings.Repeat(fill, valW+2) +
			mid + strings.Repeat(fill, valW+2) +
			mid + strings.Repeat(fill, cvW+2) + right
	}

	fmt.Println(hRule("┌", "┬", "┐", "─"))
	fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
		keyW, "Key Type", valW, "Median", valW, "Mean", valW, "StdDev", valW, "Min", valW, "Max", cvW, "CV %")
	fmt.Println(hRule("├", "┼", "┤", "─"))

	for _, keyType := range keyTypes {
		stats := results[keyType][metric]
		fmt.Printf("│ %-*s │ %*s │ %*s │ %*s │ %*s │ %*s │ %*.1f │\n",
			keyW, strings.ToUpper(keyType),
			valW, fmt.Sprintf(format, stats.Median),
			valW, fmt.Sprintf(format, stats.Mean),
			valW, fmt.Sprintf(format, stats.StdDev),
			valW, fmt.Sprintf(format, stats.Min),
			valW, fmt.Sprintf(format, stats.Max),
			cvW, stats.CV,
		)
	}

	fmt.Println(hRule("└", "┴", "┘", "─"))
}

// ScenarioStatistics displays multi-run statistical results for any scenario
func ScenarioStatistics(title string, results map[string]map[string]statistics.Stats, keyTypes []string, numRuns int, database string) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("%s - Statistical Summary (%d runs per UUID type)\n", title, numRuns)
	fmt.Println(strings.Repeat("=", 100))

	type metricDisplay struct {
		key   string
		label string
		fmt   string
	}

	// Database-aware label for fragmentation
	fragLabel := "Fragmentation (%)"
	switch database {
	case "cassandra":
		fragLabel = "Space Amplification (%)"
	case "postgres":
		fragLabel = "Leaf Fragmentation (%)"
	}

	metrics := []metricDisplay{
		{"throughput", "Throughput (records/sec)", "%.0f"},
		{"read_throughput", "Read Throughput (ops/sec)", "%.0f"},
		{"update_throughput", "Update Throughput (ops/sec)", "%.0f"},
		{"overall_throughput", "Overall Throughput (ops/sec)", "%.0f"},
		{"page_splits", "Page Splits", "%.0f"},
		{"sstable_count", "SSTable Count", "%.0f"},
		{"bloom_filter_fp", "Bloom Filter FP (delta)", "%.0f"},
		{"fragmentation", fragLabel, "%.2f"},
		{"avg_leaf_density", "Avg Leaf Density (%)", "%.2f"},
		{"cache_hit_ratio", "Cache Hit Ratio", "%.4f"},
		{"index_hit_ratio", "Index Hit Ratio", "%.4f"},
		{"table_size_mb", "Table Size (MB)", "%.1f"},
		{"index_size_mb", "Index Size (MB)", "%.1f"},
		{"p50_latency_us", "Latency P50 (µs)", "%.0f"},
		{"p95_latency_us", "Latency P95 (µs)", "%.0f"},
		{"p99_latency_us", "Latency P99 (µs)", "%.0f"},
		{"read_iops", "Read IOPS", "%.0f"},
		{"write_iops", "Write IOPS", "%.0f"},
		{"read_throughput_mb", "Read Throughput (MB/s)", "%.1f"},
		{"write_throughput_mb", "Write Throughput (MB/s)", "%.1f"},
	}

	for _, m := range metrics {
		// Only display metrics that exist in the results
		found := false
		for _, keyType := range keyTypes {
			if _, ok := results[keyType][m.key]; ok {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		fmt.Printf("\n%s\n", m.label)
		displayMetricTable(results, keyTypes, m.key, m.fmt)
		displayComparisons(results, keyTypes, m.key)
	}
}

func displayComparisons(results map[string]map[string]statistics.Stats, keyTypes []string, metric string) {
	// Compute comparison column width from key type names
	compW := len("Comparison")
	prefix := "SEQUENTIAL vs "
	for _, kt := range keyTypes {
		if kt == "sequential" {
			continue
		}
		if l := len(prefix) + len(strings.ToUpper(kt)); l > compW {
			compW = l
		}
	}

	diffW := 11 // "Median Diff" header
	pvalW := 8  // "p-value"
	overW := 9  // "Overlap?"
	sigW := 13  // "*** (p<0.001)"

	hRule := func(left, mid, right, fill string) string {
		return left + strings.Repeat(fill, compW+2) +
			mid + strings.Repeat(fill, diffW+2) +
			mid + strings.Repeat(fill, pvalW+2) +
			mid + strings.Repeat(fill, overW+2) +
			mid + strings.Repeat(fill, sigW+2) + right
	}

	fmt.Println("\nStatistical Comparisons (vs SEQUENTIAL):")
	fmt.Println(hRule("┌", "┬", "┐", "─"))
	fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
		compW, "Comparison", diffW, "Median Diff", pvalW, "p-value", overW, "Overlap?", sigW, "Significant?")
	fmt.Println(hRule("├", "┼", "┤", "─"))

	sequentialStats := results["sequential"][metric]

	for _, keyType := range keyTypes {
		if keyType == "sequential" {
			continue
		}

		stats := results[keyType][metric]
		comp := statistics.Compare(sequentialStats, stats)

		significance := ""
		if !comp.HasOverlap {
			significance = "No overlap"
		} else if comp.PValue < 0.001 {
			significance = "*** (p<0.001)"
		} else if comp.PValue < 0.01 {
			significance = "** (p<0.01)"
		} else if comp.PValue < 0.05 {
			significance = "* (p<0.05)"
		} else {
			significance = "n.s."
		}

		overlap := "No"
		if comp.HasOverlap {
			overlap = "Yes"
		}

		fmt.Printf("│ %-*s │ %+*.1f%% │ %*.4f │ %-*s │ %-*s │\n",
			compW, prefix+strings.ToUpper(keyType),
			diffW-1, comp.MedianDiffPct,
			pvalW, comp.PValue,
			overW, overlap,
			sigW, significance,
		)
	}

	fmt.Println(hRule("└", "┴", "┘", "─"))
}
