package postgres

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

func (p *PostgresBenchmarker) RunMixedWorkloadPgbench(keyType string, initialDataset, totalOps, connections int, insertWeight, readWeight, updateWeight int) (*benchmark.MixedWorkloadResult, error) {
	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	_, err := p.InsertRecordsPgbench(keyType, initialDataset, 100)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	fmt.Println("Resetting statistics...")
	err = p.ResetStats()
	if err != nil {
		return nil, fmt.Errorf("reset stats: %w", err)
	}

	insertOps := (totalOps * insertWeight) / 100
	readOps := (totalOps * readWeight) / 100
	updateOps := (totalOps * updateWeight) / 100

	fmt.Printf("Running mixed workload (%d inserts, %d reads, %d updates)...\n",
		insertOps, readOps, updateOps)

	startLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture start LSN: %w", err)
	}
	p.startLSN = startLSN

	startTime := time.Now()

	// pgbench supports weighted mixed workloads via multiple -f flags with @weight
	// but for simplicity, we use the conditional script approach
	script := pgbench.GenerateMixedScript(keyType, p.tableName, insertWeight, readWeight, updateWeight)

	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", initialDataset, script)

	scriptName := fmt.Sprintf("mixed_%s_%d_%d_%d.sql", keyType, insertWeight, readWeight, updateWeight)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	execCfg := pgbench.ExecutorConfig{
		ContainerName: "uuid-bench-postgres",
		Connections:   connections,
		Transactions:  totalOps / connections,
		ScriptPath:    containerPath,
	}

	execResult, err := pgbench.Execute(execCfg)
	if err != nil {
		return nil, fmt.Errorf("execute pgbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return nil, fmt.Errorf("pgbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	duration := time.Since(startTime)

	parsed, err := pgbench.ParsePgbenchOutput(execResult.Stdout)
	if err != nil {
		return nil, fmt.Errorf("parse pgbench output: %w", err)
	}

	endLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture end LSN: %w", err)
	}
	p.endLSN = endLSN

	fmt.Println("Measuring metrics...")
	metrics, err := p.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	return &benchmark.MixedWorkloadResult{
		KeyType:           keyType,
		NumRecords:        initialDataset,
		TotalOps:          totalOps,
		InsertOps:         insertOps,
		ReadOps:           readOps,
		UpdateOps:         updateOps,
		Duration:          duration,
		OverallThroughput: parsed.TPS,
		// NOTE: pgbench mixed workloads only report OverallThroughput.
		// Per-operation throughput metrics (InsertThroughput, ReadThroughput, UpdateThroughput)
		// are set to 0 because pgbench doesn't separate throughput by operation type in mixed mode.
		// To measure per-operation throughput, run separate scenarios (insert-performance,
		// read-after-fragmentation, update-performance) instead of mixed workloads.
		InsertThroughput:    0,
		ReadThroughput:      0,
		UpdateThroughput:    0,
		BufferHitRatio:      metrics.BufferHitRatio,
		IndexBufferHitRatio: metrics.IndexBufferHitRatio,
		Fragmentation:       metrics.Fragmentation,
		TableSize:           metrics.TableSize,
		IndexSize:           metrics.IndexSize,
	}, nil
}
