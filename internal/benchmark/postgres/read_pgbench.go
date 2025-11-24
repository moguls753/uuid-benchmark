package postgres

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

// ReadRecordsPgbench performs random point lookups using pgbench
func (p *PostgresBenchmarker) ReadRecordsPgbench(keyType string, numTotalRecords, numReads int) (time.Duration, error) {
	// Generate SELECT script
	script := pgbench.GenerateSelectScript(keyType, p.tableName)

	// Copy script to container
	scriptName := fmt.Sprintf("select_%s.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", script, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	// Execute via pgbench
	execCfg := pgbench.ExecutorConfig{
		ContainerName: "uuid-bench-postgres",
		Connections:   1, // Sequential reads
		Transactions:  numReads,
		ScriptPath:    containerPath,
	}

	// Set pgbench variable for num_records (used in script for BIGSERIAL random range)
	// We need to pass this via environment or script customization
	// For now, we'll use a workaround: modify the script to include the value
	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", numTotalRecords, script)
	containerPath, err = pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script with vars to container: %w", err)
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

// ReadRecordsPgbenchConcurrent performs concurrent random point lookups using pgbench
func (p *PostgresBenchmarker) ReadRecordsPgbenchConcurrent(keyType string, numTotalRecords, numReads, connections int) (*benchmark.ConcurrentBenchmarkResult, error) {
	// Generate SELECT script
	script := pgbench.GenerateSelectScript(keyType, p.tableName)

	// Add num_records variable
	scriptWithVars := fmt.Sprintf("\\set num_records %d\n%s", numTotalRecords, script)

	// Copy script to container
	scriptName := fmt.Sprintf("select_%s_concurrent.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", scriptWithVars, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	// Calculate transactions per client
	transactionsPerClient := numReads / connections

	startTime := time.Now()

	// Execute via pgbench with concurrency
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

	// Parse pgbench output
	parsed, err := pgbench.ParsePgbenchOutput(execResult.Stdout)
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
