package pgbench

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// PgbenchResult contains parsed metrics from pgbench output
type PgbenchResult struct {
	TPS               float64       // Transactions per second (excluding connections establishing)
	TPSIncludingSetup float64       // Transactions per second (including connection time)
	LatencyAvg        time.Duration // Average latency
	LatencyStdDev     time.Duration // Latency standard deviation
	P50               time.Duration // 50th percentile latency
	P95               time.Duration // 95th percentile latency
	P99               time.Duration // 99th percentile latency
	Transactions      int           // Number of actually processed transactions
	Duration          time.Duration // Total duration
	ContainerName     string        // Container name for log file parsing
}

// ParsePgbenchOutput parses the stdout from pgbench and extracts metrics
func ParsePgbenchOutput(output, containerName string) (*PgbenchResult, error) {
	result := &PgbenchResult{
		ContainerName: containerName,
	}

	// Example pgbench output:
	// transaction type: Custom query
	// scaling factor: 1
	// query mode: simple
	// number of clients: 4
	// number of threads: 4
	// number of transactions per client: 25000
	// number of transactions actually processed: 100000/100000
	// latency average = 1.234 ms
	// latency stddev = 0.567 ms
	// initial connection time = 12.345 ms
	// tps = 80000.123456 (without initial connection time)
	// OR for percentiles:
	// latency average = 1.234 ms
	// latency stddev = 0.567 ms
	// percentile 50 = 1.100 ms
	// percentile 95 = 2.300 ms
	// percentile 99 = 3.500 ms

	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Parse number of transactions actually processed
		if strings.Contains(line, "number of transactions actually processed") {
			re := regexp.MustCompile(`(\d+)/\d+`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.Atoi(matches[1]); err == nil {
					result.Transactions = val
				}
			}
		}

		// Parse latency average
		if strings.HasPrefix(line, "latency average") {
			val, err := parseLatency(line)
			if err == nil {
				result.LatencyAvg = val
			}
		}

		// Parse latency stddev
		if strings.HasPrefix(line, "latency stddev") {
			val, err := parseLatency(line)
			if err == nil {
				result.LatencyStdDev = val
			}
		}

		// Parse TPS (excluding connection time)
		if strings.HasPrefix(line, "tps") && strings.Contains(line, "without") {
			re := regexp.MustCompile(`tps\s*=\s*([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.TPS = val
				}
			}
		}

		// Parse TPS (including connection time)
		if strings.HasPrefix(line, "tps") && strings.Contains(line, "including") {
			re := regexp.MustCompile(`tps\s*=\s*([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.TPSIncludingSetup = val
				}
			}
		}

		// Parse percentiles
		if strings.Contains(line, "percentile") {
			if strings.Contains(line, "50") {
				val, err := parseLatency(line)
				if err == nil {
					result.P50 = val
				}
			} else if strings.Contains(line, "95") {
				val, err := parseLatency(line)
				if err == nil {
					result.P95 = val
				}
			} else if strings.Contains(line, "99") {
				val, err := parseLatency(line)
				if err == nil {
					result.P99 = val
				}
			}
		}
	}

	// Calculate duration from TPS and transactions
	if result.TPS > 0 && result.Transactions > 0 {
		result.Duration = time.Duration(float64(result.Transactions)/result.TPS*1000) * time.Millisecond
	}

	// Validation
	if result.TPS == 0 {
		return nil, fmt.Errorf("failed to parse TPS from pgbench output")
	}
	if result.Transactions == 0 {
		return nil, fmt.Errorf("failed to parse transaction count from pgbench output")
	}

	// If percentiles weren't in stdout (common case), parse from log files
	if result.P50 == 0 && result.P95 == 0 && result.P99 == 0 && containerName != "" {
		p50, p95, p99, err := ParseLogFilesForPercentiles(containerName)
		if err == nil {
			result.P50 = p50
			result.P95 = p95
			result.P99 = p99
		}
		// Silently ignore log parsing errors - percentiles are optional
	}

	return result, nil
}

// parseLatency parses a latency value from a line like "latency average = 1.234 ms"
func parseLatency(line string) (time.Duration, error) {
	// Match patterns like "= 1.234 ms" or "= 1234.567 us"
	re := regexp.MustCompile(`=\s*([0-9.]+)\s*(ms|us)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) < 3 {
		return 0, fmt.Errorf("failed to parse latency from line: %s", line)
	}

	val, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse latency value: %w", err)
	}

	unit := matches[2]
	switch unit {
	case "ms":
		return time.Duration(val * float64(time.Millisecond)), nil
	case "us":
		return time.Duration(val * float64(time.Microsecond)), nil
	default:
		return 0, fmt.Errorf("unknown latency unit: %s", unit)
	}
}
