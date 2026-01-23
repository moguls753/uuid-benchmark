package mysql

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql/sysbench"
)

func (m *MySQLBenchmarker) UpdateRecordsSysbench(keyType string, numTotalRecords, numUpdates, batchSize int) (time.Duration, error) {
	script := sysbench.GenerateUpdateScript(keyType, m.tableName)

	scriptName := fmt.Sprintf("update_%s.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	startTime := time.Now()

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       1,
		Events:        numUpdates,
		ScriptPath:    containerPath,
		TableName:     m.tableName,
		DBName:        dbName,
		NumRecords:    numTotalRecords,
	}

	execResult, err := sysbench.Execute(execCfg)
	if err != nil {
		return 0, fmt.Errorf("execute sysbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return 0, fmt.Errorf("sysbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	duration := time.Since(startTime)

	return duration, nil
}

func (m *MySQLBenchmarker) UpdateRecordsSysbenchConcurrent(keyType string, numTotalRecords, numUpdates, connections, batchSize int) (*benchmark.ConcurrentBenchmarkResult, error) {
	script := sysbench.GenerateUpdateScript(keyType, m.tableName)

	scriptName := fmt.Sprintf("update_%s_concurrent.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	startTime := time.Now()

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       connections,
		Events:        numUpdates,
		ScriptPath:    containerPath,
		TableName:     m.tableName,
		DBName:        dbName,
		NumRecords:    numTotalRecords,
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

	duration := time.Since(startTime)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numUpdates,
		Throughput:   parsed.TPS,
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.TotalEvents,
		ErrorCount:   parsed.Errors,
	}, nil
}
