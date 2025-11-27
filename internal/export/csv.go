package export

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

// InsertPerformanceStatsToCSV exports statistical results to CSV format for plotting
func InsertPerformanceStatsToCSV(results map[string]map[string]statistics.Stats, keyTypes []string, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header row
	header := []string{"KeyType", "Metric", "Median", "Mean", "StdDev", "Min", "Max", "CV_Percent"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Metrics to export
	metrics := []string{
		"throughput",
		"page_splits",
		"fragmentation",
		"table_size_mb",
		"index_size_mb",
		"p99_latency_us",
		"write_iops",
	}

	// Write data rows
	for _, keyType := range keyTypes {
		for _, metric := range metrics {
			stats := results[keyType][metric]
			row := []string{
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

	return nil
}

// InsertPerformanceRawRunsToCSV exports raw run data (all individual runs) for detailed analysis
func InsertPerformanceRawRunsToCSV(results map[string]map[string]statistics.Stats, keyTypes []string, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Determine max number of runs
	maxRuns := 0
	for _, keyType := range keyTypes {
		for _, stats := range results[keyType] {
			if len(stats.Values) > maxRuns {
				maxRuns = len(stats.Values)
			}
		}
	}

	// Header row: KeyType, Metric, Run1, Run2, ..., RunN
	header := []string{"KeyType", "Metric"}
	for i := 1; i <= maxRuns; i++ {
		header = append(header, fmt.Sprintf("Run%d", i))
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Metrics to export
	metrics := []string{
		"throughput",
		"page_splits",
		"fragmentation",
		"table_size_mb",
		"index_size_mb",
		"p99_latency_us",
		"write_iops",
	}

	// Write data rows
	for _, keyType := range keyTypes {
		for _, metric := range metrics {
			stats := results[keyType][metric]
			row := []string{strings.ToUpper(keyType), metric}

			// Add all run values
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

	return nil
}
