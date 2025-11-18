package display

import (
	"fmt"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// InsertPerformance displays a comparison table for insert performance results
func InsertPerformance(results map[string]*benchmark.InsertPerformanceResult, keyTypes []string, connections, batchSize int) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Insert Performance")
	fmt.Println(strings.Repeat("=", 70))

	// Header
	fmt.Printf("%-15s", "Metric")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	// Duration
	fmt.Printf("%-15s", "Duration")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", results[keyType].Duration.Round(time.Millisecond))
	}
	fmt.Println()

	// Throughput
	fmt.Printf("%-15s", "Throughput")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].Throughput))
	}
	fmt.Println()

	// Page splits
	fmt.Printf("%-15s", "Page Splits")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20d", results[keyType].PageSplits)
	}
	fmt.Println()

	// Index size
	fmt.Printf("%-15s", "Index Size")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", benchmark.FormatBytes(results[keyType].IndexSize))
	}
	fmt.Println()

	// Fragmentation
	fmt.Printf("%-15s", "Fragmentation")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	// Leaf density
	fmt.Printf("%-15s", "Leaf Density")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.AvgLeafDensity))
	}
	fmt.Println()

	// Read IOPS
	fmt.Printf("%-15s", "Read IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	// Write IOPS
	fmt.Printf("%-15s", "Write IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	// Read throughput
	fmt.Printf("%-15s", "Read MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	// Write throughput
	fmt.Printf("%-15s", "Write MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

// ReadAfterFragmentation displays a comparison table for read-after-fragmentation results
func ReadAfterFragmentation(results map[string]*benchmark.ReadAfterFragmentationResult, keyTypes []string) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Read After Fragmentation")
	fmt.Println(strings.Repeat("=", 70))

	// Header
	fmt.Printf("%-20s", "Metric")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	// Read throughput
	fmt.Printf("%-20s", "Read Throughput")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].ReadThroughput))
	}
	fmt.Println()

	// Buffer hit ratio
	fmt.Printf("%-20s", "Buffer Hit Ratio")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].BufferHitRatio*100))
	}
	fmt.Println()

	// Index buffer hit ratio
	fmt.Printf("%-20s", "Index Hit Ratio")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].IndexBufferHitRatio*100))
	}
	fmt.Println()

	// Fragmentation
	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	// Read latency p50
	fmt.Printf("%-20s", "Latency p50")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP50.Round(time.Microsecond))
	}
	fmt.Println()

	// Read latency p95
	fmt.Printf("%-20s", "Latency p95")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP95.Round(time.Microsecond))
	}
	fmt.Println()

	// Read IOPS
	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	// Write IOPS
	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	// Read throughput MB/s
	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	// Write throughput MB/s
	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

// UpdatePerformance displays a comparison table for update performance results
func UpdatePerformance(results map[string]*benchmark.UpdatePerformanceResult, keyTypes []string) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Update Performance")
	fmt.Println(strings.Repeat("=", 70))

	// Header
	fmt.Printf("%-20s", "Metric")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	// Update throughput
	fmt.Printf("%-20s", "Update Throughput")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].UpdateThroughput))
	}
	fmt.Println()

	// Update latency p50
	fmt.Printf("%-20s", "Latency p50")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP50.Round(time.Microsecond))
	}
	fmt.Println()

	// Update latency p95
	fmt.Printf("%-20s", "Latency p95")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP95.Round(time.Microsecond))
	}
	fmt.Println()

	// Fragmentation after updates
	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	// Read IOPS
	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	// Write IOPS
	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	// Read throughput MB/s
	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	// Write throughput MB/s
	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

// MixedWorkload displays a comparison table for mixed workload results
func MixedWorkload(results map[string]*benchmark.MixedWorkloadResult, keyTypes []string, workloadName string) {
	fmt.Println()
	fmt.Println()
	fmt.Printf("COMPARISON - Mixed Workload: %s\n", workloadName)
	fmt.Println(strings.Repeat("=", 70))

	// Header
	fmt.Printf("%-20s", "Metric")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	// Overall throughput
	fmt.Printf("%-20s", "Overall Throughput")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].OverallThroughput))
	}
	fmt.Println()

	// Insert throughput
	if results[keyTypes[0]].InsertOps > 0 {
		fmt.Printf("%-20s", "Insert Throughput")
		for _, keyType := range keyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].InsertThroughput))
		}
		fmt.Println()
	}

	// Read throughput
	if results[keyTypes[0]].ReadOps > 0 {
		fmt.Printf("%-20s", "Read Throughput")
		for _, keyType := range keyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].ReadThroughput))
		}
		fmt.Println()
	}

	// Update throughput
	if results[keyTypes[0]].UpdateOps > 0 {
		fmt.Printf("%-20s", "Update Throughput")
		for _, keyType := range keyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].UpdateThroughput))
		}
		fmt.Println()
	}

	// Buffer hit ratio
	fmt.Printf("%-20s", "Buffer Hit Ratio")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].BufferHitRatio*100))
	}
	fmt.Println()

	// Index buffer hit ratio
	fmt.Printf("%-20s", "Index Hit Ratio")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].IndexBufferHitRatio*100))
	}
	fmt.Println()

	// Index size
	fmt.Printf("%-20s", "Index Size")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", benchmark.FormatBytes(results[keyType].IndexSize))
	}
	fmt.Println()

	// Fragmentation
	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	// Read IOPS
	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	// Write IOPS
	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	// Read throughput MB/s
	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	// Write throughput MB/s
	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range keyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}
