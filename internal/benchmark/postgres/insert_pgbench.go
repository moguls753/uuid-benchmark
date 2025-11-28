package postgres

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

// InsertRecordsPgbench performs inserts using pgbench for all key types
func (p *PostgresBenchmarker) InsertRecordsPgbench(keyType string, numRecords, batchSize int) (time.Duration, error) {
	// Capture LSN before inserts
	startLSN, err := p.getCurrentLSN()
	if err != nil {
		return 0, fmt.Errorf("capture start LSN: %w", err)
	}
	p.startLSN = startLSN

	startTime := time.Now()
	var duration time.Duration

	// Use pgbench for all key types (native server-side generation)
	script := pgbench.GenerateInsertScript(keyType, p.tableName)
	if batchSize > 1 {
		script = pgbench.GenerateMultipleInserts(keyType, p.tableName, batchSize)
	}

	// Copy script to container
	scriptName := fmt.Sprintf("insert_%s.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", script, scriptName)
	if err != nil {
		return 0, fmt.Errorf("copy script to container: %w", err)
	}

	// Calculate transactions
	// For batched inserts, each transaction processes batchSize records
	transactions := numRecords
	if batchSize > 1 {
		transactions = numRecords / batchSize
		if numRecords%batchSize != 0 {
			transactions++
		}
	}

	// Execute via pgbench
	execCfg := pgbench.ExecutorConfig{
		ContainerName: "uuid-bench-postgres",
		Connections:   1, // Sequential execution
		Transactions:  transactions,
		ScriptPath:    containerPath,
	}

	execResult, err := pgbench.Execute(execCfg)
	if err != nil {
		return 0, fmt.Errorf("execute pgbench: %w", err)
	}

	if execResult.ExitCode != 0 {
		return 0, fmt.Errorf("pgbench failed with exit code %d: %s", execResult.ExitCode, execResult.Stderr)
	}

	// Parse pgbench output for duration
	parsed, err := pgbench.ParsePgbenchOutput(execResult.Stdout)
	if err != nil {
		// Fallback: use measured time
		duration = time.Since(startTime)
	} else {
		duration = parsed.Duration
	}

	// Capture LSN after inserts
	endLSN, err := p.getCurrentLSN()
	if err != nil {
		return 0, fmt.Errorf("capture end LSN: %w", err)
	}
	p.endLSN = endLSN

	return duration, nil
}

// InsertRecordsPgbenchConcurrent performs concurrent inserts using pgbench's -c flag
func (p *PostgresBenchmarker) InsertRecordsPgbenchConcurrent(keyType string, numRecords, connections, batchSize int) (*benchmark.ConcurrentBenchmarkResult, error) {
	// Capture LSN before inserts
	startLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture start LSN: %w", err)
	}
	p.startLSN = startLSN

	startTime := time.Now()

	// Use pgbench for all key types
	script := pgbench.GenerateInsertScript(keyType, p.tableName)
	if batchSize > 1 {
		script = pgbench.GenerateMultipleInserts(keyType, p.tableName, batchSize)
	}

	// Copy script to container
	scriptName := fmt.Sprintf("insert_%s_concurrent.sql", keyType)
	containerPath, err := pgbench.CopyScriptToContainer("uuid-bench-postgres", script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("copy script to container: %w", err)
	}

	// Calculate transactions per client
	transactionsPerClient := numRecords / connections
	if batchSize > 1 {
		transactionsPerClient = (numRecords / batchSize) / connections
	}

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

	// Capture LSN after inserts
	endLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture end LSN: %w", err)
	}
	p.endLSN = endLSN

	duration := time.Since(startTime)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numRecords,
		Throughput:   parsed.TPS,
		LatencyP50:   parsed.P50,
		LatencyP95:   parsed.P95,
		LatencyP99:   parsed.P99,
		SuccessCount: parsed.Transactions,
		ErrorCount:   numRecords - parsed.Transactions,
	}, nil
}
