package postgres

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

func (p *PostgresBenchmarker) ReadRecordsPgbenchConcurrent(keyType string, numTotalRecords, numReads, connections int) (*benchmark.ConcurrentBenchmarkResult, error) {
	script := pgbench.GenerateSelectScript(keyType, p.tableName)

	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", numTotalRecords, script)

	scriptName := fmt.Sprintf("select_%s_concurrent.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	transactionsPerClient := numReads / connections

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

	parsed, err := pgbench.ParsePgbenchOutput(execResult.Stdout, "uuid-bench-postgres")
	if err != nil {
		return nil, fmt.Errorf("parse pgbench output: %w", err)
	}

	duration := time.Since(startTime)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numReads,
		Throughput:   parsed.TPS,
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.Transactions,
		ErrorCount:   numReads - parsed.Transactions,
	}, nil
}
