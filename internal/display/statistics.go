package display

import (
	"fmt"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

func InsertPerformanceStatistics(results map[string]map[string]statistics.Stats, keyTypes []string, numRecords, connections, batchSize, numRuns int) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("Insert Performance - Statistical Summary (%d runs per UUID type)\n", numRuns)
	fmt.Println(strings.Repeat("=", 100))

	fmt.Println("\nThroughput (records/sec)")
	displayMetricTable(results, keyTypes, "throughput", "%.0f")
	displayComparisons(results, keyTypes, "throughput")

	fmt.Println("\nPage Splits")
	displayMetricTable(results, keyTypes, "page_splits", "%.0f")
	displayComparisons(results, keyTypes, "page_splits")

	fmt.Println("\nIndex Fragmentation (%)")
	displayMetricTable(results, keyTypes, "fragmentation", "%.2f")
	displayComparisons(results, keyTypes, "fragmentation")

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

func displayComparisons(results map[string]map[string]statistics.Stats, keyTypes []string, metric string) {
	fmt.Println("\nStatistical Comparisons (vs BIGSERIAL):")
	fmt.Println("┌─────────────────────────┬─────────────┬──────────┬───────────┬──────────────┐")
	fmt.Println("│ Comparison              │ Median Diff │ p-value  │ Overlap?  │ Significant? │")
	fmt.Println("├─────────────────────────┼─────────────┼──────────┼───────────┼──────────────┤")

	bigserialStats := results["bigserial"][metric]

	for _, keyType := range keyTypes {
		if keyType == "bigserial" {
			continue
		}

		stats := results[keyType][metric]
		comp := statistics.Compare(bigserialStats, stats)

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

		fmt.Printf("│ BIGSERIAL vs %-10s │ %+10.1f%% │ %8.4f │ %-9s │ %-12s │\n",
			strings.ToUpper(keyType),
			comp.MedianDiffPct,
			comp.PValue,
			overlap,
			significance,
		)
	}

	fmt.Println("└─────────────────────────┴─────────────┴──────────┴───────────┴──────────────┘")
}
