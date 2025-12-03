package postgres

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

func (p *PostgresBenchmarker) UpdateRecordsPgbench(keyType string, numTotalRecords, numUpdates, batchSize int) (time.Duration, error) {
	script := pgbench.GenerateUpdateScript(keyType, p.tableName)

	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", numTotalRecords, script)

	scriptName := fmt.Sprintf("update_%s.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	execCfg := pgbench.ExecutorConfig{
		ContainerName: "uuid-bench-postgres",
		Connections:   1,
		Transactions:  numUpdates,
		ScriptPath:    containerPath,
	}

	startTime := time.Now()

	execResult, err := pgbench.Execute(execCfg)
	if err != nil {
		return 0, fmt.Errorf("execute pgbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return 0, fmt.Errorf("pgbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	duration := time.Since(startTime)

	return duration, nil
}

func (p *PostgresBenchmarker) UpdateRecordsPgbenchConcurrent(keyType string, numTotalRecords, numUpdates, connections, batchSize int) (*benchmark.ConcurrentBenchmarkResult, error) {
	script := pgbench.GenerateUpdateScript(keyType, p.tableName)

	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", numTotalRecords, script)

	scriptName := fmt.Sprintf("update_%s_concurrent.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	transactionsPerClient := numUpdates / connections

	startTime := time.Now()

	execCfg := pgbench.ExecutorConfig{
		ContainerName: "uuid-bench-postgres",
		Connections:   connections,
		Transactions:  transactionsPerClient,
		ScriptPath:    containerPath,
	}

	execResult, err := pgbench.Execute(execCfg)
	if err != nil {
		return nil, fmt.Errorf("execute pgbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return nil, fmt.Errorf("pgbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	parsed, err := pgbench.ParsePgbenchOutput(execResult.Stdout)
	if err != nil {
		return nil, fmt.Errorf("parse pgbench output: %w", err)
	}

	duration := time.Since(startTime)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numUpdates,
		Throughput:   parsed.TPS,
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.Transactions,
		ErrorCount:   numUpdates - parsed.Transactions,
	}, nil
}
