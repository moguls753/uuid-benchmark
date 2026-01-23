package mysql

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql/sysbench"
)

func (m *MySQLBenchmarker) ReadRecordsSysbench(keyType string, numTotalRecords, numReads int) (time.Duration, error) {
	script := sysbench.GenerateSelectScript(keyType, m.tableName)

	scriptName := fmt.Sprintf("select_%s.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	startTime := time.Now()

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       1,
		Events:        numReads,
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

func (m *MySQLBenchmarker) ReadRecordsSysbenchConcurrent(keyType string, numTotalRecords, numReads, connections int) (*benchmark.ConcurrentBenchmarkResult, error) {
	script := sysbench.GenerateSelectScript(keyType, m.tableName)

	scriptName := fmt.Sprintf("select_%s_concurrent.lua", keyType)
	containerPath, err := sysbench.CopyScriptToContainer("uuid-bench-mysql", script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	startTime := time.Now()

	execCfg := sysbench.ExecutorConfig{
		ContainerName: "uuid-bench-mysql",
		Threads:       connections,
		Events:        numReads,
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
		TotalOps:     numReads,
		Throughput:   parsed.TPS,
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.TotalEvents,
		ErrorCount:   parsed.Errors,
	}, nil
}
