package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/container"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/display"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/runner"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

var allKeyTypes = []string{"bigserial", "uuidv4", "uuidv7", "ulid", "uuidv1"}

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

		container.Start(container.PostgresConfig)

		result, err := runner.InsertPerformance(keyType, numRecords, batchSize, connections)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.InsertPerformance(results, allKeyTypes, connections, batchSize)
}

func runReadAfterFragmentationForAllTypes(numRecords, numOps int) {
	results := make(map[string]*benchmark.ReadAfterFragmentationResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(container.PostgresConfig)

		result, err := runner.ReadAfterFragmentation(keyType, numRecords, numOps)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.ReadAfterFragmentation(results, allKeyTypes)
}

func runUpdatePerformanceForAllTypes(numRecords, numOps, batchSize int) {
	results := make(map[string]*benchmark.UpdatePerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(container.PostgresConfig)

		result, err := runner.UpdatePerformance(keyType, numRecords, numOps, batchSize)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.UpdatePerformance(results, allKeyTypes)
}

func runMixedWorkloadInsertHeavyForAllTypes(totalOps, connections, batchSize int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(container.PostgresConfig)

		result, err := runner.MixedWorkloadInsertHeavy(keyType, totalOps, connections, batchSize)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.MixedWorkload(results, allKeyTypes, "Insert-Heavy (90% insert, 10% read)")
}

func runMixedWorkloadReadHeavyForAllTypes(totalOps, connections int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(container.PostgresConfig)

		result, err := runner.MixedWorkloadReadHeavy(keyType, totalOps, connections)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.MixedWorkload(results, allKeyTypes, "Read-Heavy (10% insert, 90% read)")
}

func runMixedWorkloadBalancedForAllTypes(totalOps, connections int) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\n▶ Testing %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(container.PostgresConfig)

		result, err := runner.MixedWorkloadBalanced(keyType, totalOps, connections)
		if err != nil {
			container.Stop(container.PostgresConfig.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(container.PostgresConfig.ComposeFile)
	}

	display.MixedWorkload(results, allKeyTypes, "Balanced (50% insert, 30% read, 20% update)")
}
