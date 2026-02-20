package mysql

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql/sysbench"
)

func (m *MySQLBenchmarker) InsertRecordsSysbench(keyType string, numRecords, batchSize int) (time.Duration, error) {
	threads := 1
	cleanup, err := sysbench.PrepareUUIDs("uuid-bench-mysql", keyType, numRecords, threads)
	if err != nil {
		return 0, fmt.Errorf("prepare UUIDs: %w", err)
	}
	defer cleanup()

	startPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture start page splits: %v\n", err)
	} else {
		m.startPageSplitCount = startPageSplits
		m.pageSplitCountsCaptured = true
	}

	startTime := time.Now()
	script := sysbench.GenerateInsertScript(keyType, m.tableName, threads)
	if batchSize > 1 {
		script = sysbench.GenerateBatchInsertScript(keyType, m.tableName, batchSize, threads)
	}

	scriptName := fmt.Sprintf("insert_%s.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	events := numRecords
	if batchSize > 1 {
		events = numRecords / batchSize
		if numRecords%batchSize != 0 {
			events++
		}
	}

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       1,
		Events:        events,
		ScriptPath:    containerPath,
		TableName:     m.tableName,
		DBName:        dbName,
	}

	execResult, err := sysbench.Execute(execCfg)
	if err != nil {
		return 0, fmt.Errorf("execute sysbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return 0, fmt.Errorf("sysbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	parsed, err := sysbench.ParseSysbenchOutput(execResult.Stdout, "uuid-bench-mysql")
	if err != nil {
		// Fall back to wall clock time
		duration := time.Since(startTime)
		endPageSplits, _ := m.capturePageSplitCount()
		m.endPageSplitCount = endPageSplits
		return duration, nil
	}

	endPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture end page splits: %v\n", err)
	}
	m.endPageSplitCount = endPageSplits

	return parsed.TotalTime, nil
}

func (m *MySQLBenchmarker) InsertRecordsSysbenchConcurrent(keyType string, numRecords, connections, batchSize int) (*benchmark.ConcurrentBenchmarkResult, error) {
	cleanup, err := sysbench.PrepareUUIDs("uuid-bench-mysql", keyType, numRecords, connections)
	if err != nil {
		return nil, fmt.Errorf("prepare UUIDs: %w", err)
	}
	defer cleanup()

	startPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture start page splits: %v\n", err)
	} else {
		m.startPageSplitCount = startPageSplits
		m.pageSplitCountsCaptured = true
	}

	startTime := time.Now()

	script := sysbench.GenerateInsertScript(keyType, m.tableName, connections)
	if batchSize > 1 {
		script = sysbench.GenerateBatchInsertScript(keyType, m.tableName, batchSize, connections)
	}

	scriptName := fmt.Sprintf("insert_%s_concurrent.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	events := numRecords
	if batchSize > 1 {
		events = numRecords / batchSize
	}

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       connections,
		Events:        events,
		ScriptPath:    containerPath,
		TableName:     m.tableName,
		DBName:        dbName,
	}

	execResult, err := sysbench.Execute(execCfg)
	if err != nil {
		return nil, fmt.Errorf("execute sysbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return nil, fmt.Errorf("sysbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	parsed, err := sysbench.ParseSysbenchOutput(execResult.Stdout, "uuid-bench-mysql")
	if err != nil {
		return nil, fmt.Errorf("parse sysbench output: %w", err)
	}

	endPageSplits, err := m.capturePageSplitCount()
	if err != nil {
		fmt.Printf("Warning: Could not capture end page splits: %v\n", err)
	}
	m.endPageSplitCount = endPageSplits

	duration := time.Since(startTime)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numRecords,
		Throughput:   parsed.TPS * float64(batchSize),
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.TotalEvents,
		ErrorCount:   parsed.Errors,
	}, nil
}
