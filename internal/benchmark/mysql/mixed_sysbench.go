package mysql

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql/sysbench"
)

func (m *MySQLBenchmarker) RunMixedWorkloadSysbench(keyType string, initialDataset, totalOps, connections int, insertWeight, readWeight, updateWeight int) (*benchmark.MixedWorkloadResult, error) {
	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	_, err := m.InsertRecordsSysbench(keyType, initialDataset, 100)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	fmt.Println("Resetting statistics...")
	err = m.ResetStats()
	if err != nil {
		fmt.Printf("Warning: Could not reset stats: %v\n", err)
	}

	insertOps := (totalOps * insertWeight) / 100
	readOps := (totalOps * readWeight) / 100
	updateOps := (totalOps * updateWeight) / 100

	fmt.Printf("Running mixed workload (%d inserts, %d reads, %d updates)...\n",
		insertOps, readOps, updateOps)

	// Pre-generate UUIDs for the insert portion of the mixed workload
	if insertWeight > 0 {
		cleanup, err := sysbench.PrepareUUIDs("uuid-bench-mysql", keyType, totalOps)
		if err != nil {
			return nil, fmt.Errorf("prepare UUIDs for mixed workload: %w", err)
		}
		defer cleanup()
	}

	startPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture start page splits: %v\n", err)
		m.pageSplitCountsCaptured = false
	} else {
		m.startPageSplitCount = startPageSplits
		m.pageSplitCountsCaptured = true
	}

	startTime := time.Now()

	script := sysbench.GenerateMixedScript(keyType, m.tableName, insertWeight, readWeight, updateWeight, connections)

	scriptName := fmt.Sprintf("mixed_%s_%d_%d_%d.lua", keyType, insertWeight, readWeight, updateWeight)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       connections,
		Events:        totalOps,
		ScriptPath:    containerPath,
		TableName:     m.tableName,
		DBName:        dbName,
		NumRecords:    initialDataset,
	}

	execResult, err := sysbench.Execute(execCfg)
	if err != nil {
		return nil, fmt.Errorf("execute sysbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return nil, fmt.Errorf("sysbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	duration := time.Since(startTime)

	parsed, err := sysbench.ParseSysbenchOutput(execResult.Stdout, "uuid-bench-mysql")
	if err != nil {
		return nil, fmt.Errorf("parse sysbench output: %w", err)
	}

	endPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture end page splits: %v\n", err)
	}
	m.endPageSplitCount = endPageSplits

	fmt.Println("Measuring metrics...")
	metrics, err := m.MeasureMetrics()
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
		// NOTE: sysbench mixed workloads only report OverallThroughput.
		// Per-operation throughput metrics are set to 0 because sysbench
		// doesn't separate throughput by operation type in mixed mode.
		InsertThroughput:    0,
		ReadThroughput:      0,
		UpdateThroughput:    0,
		LatencyP50:          parsed.P50,
		LatencyP95:          parsed.P95,
		LatencyP99:          parsed.P99,
		BufferHitRatio:      metrics.BufferHitRatio,
		IndexBufferHitRatio: metrics.IndexBufferHitRatio,
		Fragmentation:       metrics.Fragmentation,
		TableSize:           metrics.TableSize,
		IndexSize:           metrics.IndexSize,
	}, nil
}
