package export

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

// ScenarioStats holds aggregated statistics for a single scenario
type ScenarioStats struct {
	Name    string
	Results map[string]map[string]statistics.Stats
}

// logicalMetricOrder defines the preferred order for CSV export:
// throughput -> latency -> cache/fragmentation -> sizes -> I/O
var logicalMetricOrder = []string{
	// Throughput
	"throughput",
	"read_throughput",
	"update_throughput",
	"overall_throughput",
	// Latency
	"p50_latency_us",
	"p95_latency_us",
	"p99_latency_us",
	// Cache & index quality
	"cache_hit_ratio",
	"index_hit_ratio",
	"page_splits",
	"fragmentation",
	"avg_leaf_density",
	// Sizes
	"table_size_mb",
	"index_size_mb",
	// I/O
	"read_iops",
	"write_iops",
	"read_throughput_mb",
	"write_throughput_mb",
}

// orderedMetricKeys returns metric keys present in m, ordered by logicalMetricOrder.
// Any keys not in logicalMetricOrder are appended at the end alphabetically.
func orderedMetricKeys(m map[string]statistics.Stats) []string {
	present := make(map[string]bool, len(m))
	for k := range m {
		present[k] = true
	}

	var result []string
	for _, k := range logicalMetricOrder {
		if present[k] {
			result = append(result, k)
			delete(present, k)
		}
	}

	if len(present) > 0 {
		var extra []string
		for k := range present {
			extra = append(extra, k)
		}
		sort.Strings(extra)
		result = append(result, extra...)
	}

	return result
}

// StatsToCSV exports statistical results for one or more scenarios to CSV format
func StatsToCSV(scenarios []ScenarioStats, keyTypes []string, outputPath string) (err error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close CSV file: %w", cerr)
		}
	}()

	writer := csv.NewWriter(file)

	header := []string{"Scenario", "KeyType", "Metric", "Median", "Mean", "StdDev", "Min", "Max", "CV_Percent"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, scenario := range scenarios {
		for _, keyType := range keyTypes {
			metrics := orderedMetricKeys(scenario.Results[keyType])
			for _, metric := range metrics {
				stats := scenario.Results[keyType][metric]
				row := []string{
					scenario.Name,
					strings.ToUpper(keyType),
					metric,
					fmt.Sprintf("%.2f", stats.Median),
					fmt.Sprintf("%.2f", stats.Mean),
					fmt.Sprintf("%.2f", stats.StdDev),
					fmt.Sprintf("%.2f", stats.Min),
					fmt.Sprintf("%.2f", stats.Max),
					fmt.Sprintf("%.2f", stats.CV),
				}
				if err := writer.Write(row); err != nil {
					return fmt.Errorf("failed to write CSV row: %w", err)
				}
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return nil
}

// RawRunsToCSV exports raw run data for one or more scenarios to CSV format
func RawRunsToCSV(scenarios []ScenarioStats, keyTypes []string, outputPath string) (err error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close CSV file: %w", cerr)
		}
	}()

	writer := csv.NewWriter(file)

	// Determine max number of runs across all scenarios
	maxRuns := 0
	for _, scenario := range scenarios {
		for _, keyType := range keyTypes {
			for _, stats := range scenario.Results[keyType] {
				if len(stats.Values) > maxRuns {
					maxRuns = len(stats.Values)
				}
			}
		}
	}

	header := []string{"Scenario", "KeyType", "Metric"}
	for i := 1; i <= maxRuns; i++ {
		header = append(header, fmt.Sprintf("Run%d", i))
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, scenario := range scenarios {
		for _, keyType := range keyTypes {
			metrics := orderedMetricKeys(scenario.Results[keyType])
			for _, metric := range metrics {
				stats := scenario.Results[keyType][metric]
				row := []string{scenario.Name, strings.ToUpper(keyType), metric}

				for _, val := range stats.Values {
					row = append(row, fmt.Sprintf("%.2f", val))
				}

				// Pad with empty strings if this metric has fewer runs
				for len(row) < len(header) {
					row = append(row, "")
				}

				if err := writer.Write(row); err != nil {
					return fmt.Errorf("failed to write CSV row: %w", err)
				}
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return nil
}
