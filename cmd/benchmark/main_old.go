package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/cmd/benchmark/scenarios"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

var allKeyTypes = []string{"bigserial", "uuidv4", "uuidv7", "ulid", "uuidv1"}

type ContainerConfig struct {
	Name         string
	ComposeFile  string
	WaitForReady func() error
}

var postgresConfig = ContainerConfig{
	Name:         "PostgreSQL",
	ComposeFile:  "docker/docker-compose.postgres.yml",
	WaitForReady: postgres.WaitForReady,
}

func main() {
	scenario := flag.String("scenario", "insert-performance", "Scenario to run (insert-performance, read-after-fragmentation, update-performance, mixed-insert-heavy, mixed-read-heavy, mixed-balanced)")
	numRecords := flag.Int("num-records", 100000, "Number of records for insert operations")
	numOps := flag.Int("num-ops", 10000, "Number of operations for read/update/mixed scenarios")
	connections := flag.Int("connections", 1, "Number of concurrent connections")
	batchSize := flag.Int("batch-size", 100, "Batch size for inserts/updates")
	flag.Parse()

	fmt.Println("UUID Benchmark - PostgreSQL")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Scenario:     %s\n", *scenario)
	fmt.Printf("Records:      %d\n", *numRecords)
	if *connections > 1 {
		fmt.Printf("Connections:  %d\n", *connections)
	}
	if *batchSize > 1 {
		fmt.Printf("Batch Size:   %d\n", *batchSize)
	}
	fmt.Printf("Testing:      %v\n", allKeyTypes)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()

	switch *scenario {
	case "insert-performance":
		runInsertPerformanceForAllTypes(*numRecords, *batchSize, *connections)

	case "read-after-fragmentation":
		runReadAfterFragmentationForAllTypes(*numRecords, *numOps)

	case "update-performance":
		runUpdatePerformanceForAllTypes(*numRecords, *numOps, *batchSize)

	case "mixed-insert-heavy":
		runMixedWorkloadInsertHeavyForAllTypes(*numOps, *connections, *batchSize)

	case "mixed-read-heavy":
		runMixedWorkloadReadHeavyForAllTypes(*numOps, *connections)

	case "mixed-balanced":
		runMixedWorkloadBalancedForAllTypes(*numOps, *connections)

	default:
		log.Fatalf("Invalid scenario: %s", *scenario)
	}

	fmt.Println()
	fmt.Println("All scenarios completed successfully!")
}

func runInsertPerformanceForAllTypes(numRecords, batchSize, connections int) {
	results := make(map[string]*benchmark.InsertPerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.InsertPerformance(keyType, numRecords, batchSize, connections)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayInsertPerformanceComparison(results, connections, batchSize)
}

func runReadAfterFragmentationForAllTypes(numRecords, numOps int) {
	results := make(map[string]*benchmark.ReadAfterFragmentationResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.ReadAfterFragmentation(keyType, numRecords, numOps)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayReadAfterFragmentationComparison(results)
}

func runUpdatePerformanceForAllTypes(numRecords, numOps, batchSize int) {
	results := make(map[string]*benchmark.UpdatePerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.UpdatePerformance(keyType, numRecords, numOps, batchSize)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayUpdatePerformanceComparison(results)
}

func runMixedWorkloadInsertHeavyForAllTypes(totalOps, connections, batchSize int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.MixedWorkloadInsertHeavy(keyType, totalOps, connections, batchSize)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayMixedWorkloadComparison(results, "Insert-Heavy (90% insert, 10% read)")
}

func runMixedWorkloadReadHeavyForAllTypes(totalOps, connections int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.MixedWorkloadReadHeavy(keyType, totalOps, connections)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayMixedWorkloadComparison(results, "Read-Heavy (10% insert, 90% read)")
}

func runMixedWorkloadBalancedForAllTypes(totalOps, connections int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		startContainer(postgresConfig)

		result, err := scenarios.MixedWorkloadBalanced(keyType, totalOps, connections)
		if err != nil {
			stopContainer(postgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		stopContainer(postgresConfig.ComposeFile)
	}

	displayMixedWorkloadComparison(results, "Balanced (50% insert, 30% read, 20% update)")
}

func displayInsertPerformanceComparison(results map[string]*benchmark.InsertPerformanceResult, connections, batchSize int) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Insert Performance")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("%-15s", "Metric")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-15s", "Duration")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", results[keyType].Duration.Round(time.Millisecond))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Throughput")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].Throughput))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Page Splits")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20d", results[keyType].PageSplits)
	}
	fmt.Println()

	fmt.Printf("%-15s", "Index Size")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", benchmark.FormatBytes(results[keyType].IndexSize))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Fragmentation")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Leaf Density")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.AvgLeafDensity))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Read IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Write IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Read MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	fmt.Printf("%-15s", "Write MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

func displayReadAfterFragmentationComparison(results map[string]*benchmark.ReadAfterFragmentationResult) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Read After Fragmentation")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("%-20s", "Metric")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-20s", "Read Throughput")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].ReadThroughput))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Buffer Hit Ratio")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].BufferHitRatio*100))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Index Hit Ratio")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].IndexBufferHitRatio*100))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Latency p50")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP50.Round(time.Microsecond))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Latency p95")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP95.Round(time.Microsecond))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

func displayUpdatePerformanceComparison(results map[string]*benchmark.UpdatePerformanceResult) {
	fmt.Println()
	fmt.Println()
	fmt.Println("COMPARISON - Update Performance")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("%-20s", "Metric")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-20s", "Update Throughput")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].UpdateThroughput))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Latency p50")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP50.Round(time.Microsecond))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Latency p95")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", results[keyType].LatencyP95.Round(time.Microsecond))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

func displayMixedWorkloadComparison(results map[string]*benchmark.MixedWorkloadResult, workloadName string) {
	fmt.Println()
	fmt.Println()
	fmt.Printf("COMPARISON - Mixed Workload: %s\n", workloadName)
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("%-20s", "Metric")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", strings.ToUpper(keyType))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-20s", "Overall Throughput")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.0f ops/s", results[keyType].OverallThroughput))
	}
	fmt.Println()

	if results[allKeyTypes[0]].InsertOps > 0 {
		fmt.Printf("%-20s", "Insert Throughput")
		for _, keyType := range allKeyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].InsertThroughput))
		}
		fmt.Println()
	}

	if results[allKeyTypes[0]].ReadOps > 0 {
		fmt.Printf("%-20s", "Read Throughput")
		for _, keyType := range allKeyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].ReadThroughput))
		}
		fmt.Println()
	}

	if results[allKeyTypes[0]].UpdateOps > 0 {
		fmt.Printf("%-20s", "Update Throughput")
		for _, keyType := range allKeyTypes {
			fmt.Printf("%-20s", fmt.Sprintf("%.0f rec/s", results[keyType].UpdateThroughput))
		}
		fmt.Println()
	}

	fmt.Printf("%-20s", "Buffer Hit Ratio")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].BufferHitRatio*100))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Index Hit Ratio")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].IndexBufferHitRatio*100))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Index Size")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", benchmark.FormatBytes(results[keyType].IndexSize))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Fragmentation")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f%%", results[keyType].Fragmentation.FragmentationPercent))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].ReadIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write IOPS")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.1f", results[keyType].WriteIOPS))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Read MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].ReadThroughputMB))
	}
	fmt.Println()

	fmt.Printf("%-20s", "Write MB/s")
	for _, keyType := range allKeyTypes {
		fmt.Printf("%-20s", fmt.Sprintf("%.2f", results[keyType].WriteThroughputMB))
	}
	fmt.Println()
}

func startContainer(cfg ContainerConfig) {
	fmt.Printf("🐳 Starting fresh %s container...\n", cfg.Name)

	cmd := exec.Command("docker", "compose", "-f", cfg.ComposeFile, "up", "-d")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to start container: %v\nOutput: %s", err, string(output))
	}

	fmt.Printf("⏳ Waiting for %s to initialize...\n", cfg.Name)
	if err := cfg.WaitForReady(); err != nil {
		log.Fatalf("%s failed to start: %v", cfg.Name, err)
	}

	fmt.Println("✅ Container ready\n")
}

func stopContainer(composeFile string) {
	fmt.Println("\n🧹 Cleaning up container...")

	cmd := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	cmd.Run()

	fmt.Println("✅ Container stopped and removed")
}
