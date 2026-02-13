package display

import (
	"fmt"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// colWidth computes the column width needed for key type headers.
func colWidth(keyTypes []string) int {
	w := 14 // minimum
	for _, kt := range keyTypes {
		if l := len(strings.ToUpper(kt)) + 2; l > w {
			w = l
		}
	}
	return w
}

func tableHeader(title string, keyTypes []string, w int) {
	totalW := 20 + w*len(keyTypes)
	fmt.Println()
	fmt.Println()
	fmt.Printf("COMPARISON - %s\n", title)
	fmt.Println(strings.Repeat("=", totalW))
	fmt.Printf("%-20s", "Metric")
	for _, keyType := range keyTypes {
		fmt.Printf("%-*s", w, strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", totalW))
}

func printRow(label string, keyTypes []string, w int, valFn func(string) string) {
	fmt.Printf("%-20s", label)
	for _, kt := range keyTypes {
		fmt.Printf("%-*s", w, valFn(kt))
	}
	fmt.Println()
}

// InsertPerformance displays a comparison table for insert performance results
func InsertPerformance(results map[string]*benchmark.InsertPerformanceResult, keyTypes []string, connections, batchSize int, database string) {
	w := colWidth(keyTypes)
	tableHeader("Insert Performance", keyTypes, w)

	printRow("Duration", keyTypes, w, func(kt string) string {
		return results[kt].Duration.Round(time.Millisecond).String()
	})
	printRow("Throughput", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.0f rec/s", results[kt].Throughput)
	})

	if database != "cassandra" {
		printRow("Page Splits", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%d", results[kt].PageSplits)
		})
	}

	if database == "cassandra" {
		printRow("SSTable Delta", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%d", results[kt].PageSplits)
		})
		printRow("SSTable Count", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%d", results[kt].Fragmentation.LeafPages)
		})
	}

	printRow("Index Size", keyTypes, w, func(kt string) string {
		return benchmark.FormatBytes(results[kt].IndexSize)
	})

	if database == "postgres" {
		printRow("Leaf Fragmentation", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
		printRow("Leaf Density", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.AvgLeafDensity)
		})
	}

	if database == "cassandra" {
		printRow("Space Amplification", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	printRow("Latency p50", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP50.Round(time.Microsecond).String()
	})
	printRow("Latency p95", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP95.Round(time.Microsecond).String()
	})
	printRow("Latency p99", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP99.Round(time.Microsecond).String()
	})
	printRow("Read IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].ReadIOPS)
	})
	printRow("Write IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].WriteIOPS)
	})
	printRow("Read MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].ReadThroughputMB)
	})
	printRow("Write MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].WriteThroughputMB)
	})
}

// ReadPerformance displays a comparison table for read-performance results
func ReadPerformance(results map[string]*benchmark.ReadPerformanceResult, keyTypes []string, database string) {
	w := colWidth(keyTypes)
	tableHeader("Read Performance", keyTypes, w)

	printRow("Duration", keyTypes, w, func(kt string) string {
		return results[kt].ReadDuration.Round(time.Millisecond).String()
	})
	printRow("Read Throughput", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.0f ops/s", results[kt].ReadThroughput)
	})
	printRow("Cache Hit Ratio", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f%%", results[kt].BufferHitRatio*100)
	})
	printRow("Index Hit Ratio", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f%%", results[kt].IndexBufferHitRatio*100)
	})

	if database == "postgres" {
		printRow("Leaf Fragmentation", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	if database == "cassandra" {
		printRow("Space Amplification", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	printRow("Latency p50", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP50.Round(time.Microsecond).String()
	})
	printRow("Latency p95", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP95.Round(time.Microsecond).String()
	})
	printRow("Read IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].ReadIOPS)
	})
	printRow("Write IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].WriteIOPS)
	})
	printRow("Read MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].ReadThroughputMB)
	})
	printRow("Write MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].WriteThroughputMB)
	})
}

// UpdatePerformance displays a comparison table for update performance results
func UpdatePerformance(results map[string]*benchmark.UpdatePerformanceResult, keyTypes []string, database string) {
	w := colWidth(keyTypes)
	tableHeader("Update Performance", keyTypes, w)

	printRow("Duration", keyTypes, w, func(kt string) string {
		return results[kt].UpdateDuration.Round(time.Millisecond).String()
	})
	printRow("Update Throughput", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.0f ops/s", results[kt].UpdateThroughput)
	})
	printRow("Latency p50", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP50.Round(time.Microsecond).String()
	})
	printRow("Latency p95", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP95.Round(time.Microsecond).String()
	})

	if database == "postgres" {
		printRow("Leaf Fragmentation", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	if database == "cassandra" {
		printRow("Space Amplification", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	printRow("Read IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].ReadIOPS)
	})
	printRow("Write IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].WriteIOPS)
	})
	printRow("Read MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].ReadThroughputMB)
	})
	printRow("Write MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].WriteThroughputMB)
	})
}

// MixedWorkload displays a comparison table for mixed workload results
func MixedWorkload(results map[string]*benchmark.MixedWorkloadResult, keyTypes []string, workloadName, database string) {
	w := colWidth(keyTypes)
	tableHeader(fmt.Sprintf("Mixed Workload: %s", workloadName), keyTypes, w)

	printRow("Duration", keyTypes, w, func(kt string) string {
		return results[kt].Duration.Round(time.Millisecond).String()
	})
	printRow("Overall Throughput", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.0f ops/s", results[kt].OverallThroughput)
	})

	if results[keyTypes[0]].InsertOps > 0 {
		printRow("Insert Throughput", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.0f rec/s", results[kt].InsertThroughput)
		})
	}

	if results[keyTypes[0]].ReadOps > 0 {
		printRow("Read Throughput", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.0f rec/s", results[kt].ReadThroughput)
		})
	}

	if results[keyTypes[0]].UpdateOps > 0 {
		printRow("Update Throughput", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.0f rec/s", results[kt].UpdateThroughput)
		})
	}

	printRow("Latency p50", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP50.Round(time.Microsecond).String()
	})
	printRow("Latency p95", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP95.Round(time.Microsecond).String()
	})
	printRow("Latency p99", keyTypes, w, func(kt string) string {
		return results[kt].LatencyP99.Round(time.Microsecond).String()
	})
	printRow("Cache Hit Ratio", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f%%", results[kt].BufferHitRatio*100)
	})
	printRow("Index Hit Ratio", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f%%", results[kt].IndexBufferHitRatio*100)
	})
	printRow("Index Size", keyTypes, w, func(kt string) string {
		return benchmark.FormatBytes(results[kt].IndexSize)
	})

	if database == "postgres" {
		printRow("Leaf Fragmentation", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	if database == "cassandra" {
		printRow("Space Amplification", keyTypes, w, func(kt string) string {
			return fmt.Sprintf("%.2f%%", results[kt].Fragmentation.FragmentationPercent)
		})
	}

	printRow("Read IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].ReadIOPS)
	})
	printRow("Write IOPS", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.1f", results[kt].WriteIOPS)
	})
	printRow("Read MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].ReadThroughputMB)
	})
	printRow("Write MB/s", keyTypes, w, func(kt string) string {
		return fmt.Sprintf("%.2f", results[kt].WriteThroughputMB)
	})
}
