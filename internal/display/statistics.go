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

	// SSTable Delta (Cassandra only)
	if database == "cassandra" {
		fmt.Println("\nSSTable Delta")
		displayMetricTable(results, keyTypes, "page_splits", "%.0f")
		displayComparisons(results, keyTypes, "page_splits")

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
	fmt.Println("┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬───────┐")
	fmt.Println("│ Key Type    │ Median   │ Mean     │ StdDev   │ Min      │ Max      │ CV %  │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼───────┤")

	for _, keyType := range keyTypes {
		stats := results[keyType][metric]

		fmt.Printf("│ %-11s │ "+format+" │ "+format+" │ "+format+" │ "+format+" │ "+format+" │ %5.1f │\n",
			strings.ToUpper(keyType),
			stats.Median,
			stats.Mean,
			stats.StdDev,
			stats.Min,
			stats.Max,
			stats.CV,
		)
	}

	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴───────┘")
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

	// Database-aware label for page_splits
	pageSplitsLabel := "Page Splits"
	if database == "cassandra" {
		pageSplitsLabel = "SSTable Delta"
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
		{"page_splits", pageSplitsLabel, "%.0f"},
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
	fmt.Println("\nStatistical Comparisons (vs SEQUENTIAL):")
	fmt.Println("┌─────────────────────────┬─────────────┬──────────┬───────────┬──────────────┐")
	fmt.Println("│ Comparison              │ Median Diff │ p-value  │ Overlap?  │ Significant? │")
	fmt.Println("├─────────────────────────┼─────────────┼──────────┼───────────┼──────────────┤")

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

		fmt.Printf("│ SEQUENTIAL vs %-9s │ %+10.1f%% │ %8.4f │ %-9s │ %-12s │\n",
			strings.ToUpper(keyType),
			comp.MedianDiffPct,
			comp.PValue,
			overlap,
			significance,
		)
	}

	fmt.Println("└─────────────────────────┴─────────────┴──────────┴───────────┴──────────────┘")
}
