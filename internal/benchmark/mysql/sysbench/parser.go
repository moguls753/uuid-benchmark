package sysbench

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SysbenchResult contains parsed metrics from sysbench output
type SysbenchResult struct {
	TPS          float64       // Transactions per second
	QPS          float64       // Queries per second
	LatencyAvg   time.Duration // Average latency
	LatencyMin   time.Duration // Minimum latency
	LatencyMax   time.Duration // Maximum latency
	Latency95    time.Duration // 95th percentile latency
	P50          time.Duration // 50th percentile latency (from histogram)
	P95          time.Duration // 95th percentile latency (from histogram)
	P99          time.Duration // 99th percentile latency (from histogram)
	TotalEvents  int           // Number of events (transactions)
	TotalTime    time.Duration // Total time
	Errors       int           // Number of errors
}

// ParseSysbenchOutput parses the stdout from sysbench and extracts metrics
func ParseSysbenchOutput(output, containerName string) (*SysbenchResult, error) {
	result := &SysbenchResult{}

	// Example sysbench output:
	// SQL statistics:
	//     queries performed:
	//         read:                            0
	//         write:                           100000
	//         other:                           0
	//         total:                           100000
	//     transactions:                        100000 (12345.67 per sec.)
	//     queries:                             100000 (12345.67 per sec.)
	//     ignored errors:                      0      (0.00 per sec.)
	//     reconnects:                          0      (0.00 per sec.)
	//
	// General statistics:
	//     total time:                          8.1000s
	//     total number of events:              100000
	//
	// Latency (ms):
	//          min:                                    0.12
	//          avg:                                    0.32
	//          max:                                   15.67
	//          95th percentile:                        0.74
	//          sum:                                32000.00

	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Parse transactions per second
		if strings.Contains(line, "transactions:") && strings.Contains(line, "per sec") {
			re := regexp.MustCompile(`transactions:\s+(\d+)\s+\(([0-9.]+)\s+per sec`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 3 {
				if val, err := strconv.Atoi(matches[1]); err == nil {
					result.TotalEvents = val
				}
				if val, err := strconv.ParseFloat(matches[2], 64); err == nil {
					result.TPS = val
				}
			}
		}

		// Parse queries per second
		if strings.HasPrefix(line, "queries:") && strings.Contains(line, "per sec") {
			re := regexp.MustCompile(`queries:\s+\d+\s+\(([0-9.]+)\s+per sec`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.QPS = val
				}
			}
		}

		// Parse total time
		if strings.Contains(line, "total time:") {
			re := regexp.MustCompile(`total time:\s+([0-9.]+)s`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.TotalTime = time.Duration(val * float64(time.Second))
				}
			}
		}

		// Parse total events (backup if not found in transactions line)
		if strings.Contains(line, "total number of events:") {
			re := regexp.MustCompile(`total number of events:\s+(\d+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.Atoi(matches[1]); err == nil && result.TotalEvents == 0 {
					result.TotalEvents = val
				}
			}
		}

		// Parse latency min
		if strings.Contains(line, "min:") && !strings.Contains(line, "admin") {
			re := regexp.MustCompile(`min:\s+([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.LatencyMin = time.Duration(val * float64(time.Millisecond))
				}
			}
		}

		// Parse latency avg
		if strings.Contains(line, "avg:") {
			re := regexp.MustCompile(`avg:\s+([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.LatencyAvg = time.Duration(val * float64(time.Millisecond))
				}
			}
		}

		// Parse latency max
		if strings.Contains(line, "max:") && !strings.Contains(line, "admin") {
			re := regexp.MustCompile(`max:\s+([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.LatencyMax = time.Duration(val * float64(time.Millisecond))
				}
			}
		}

		// Parse 95th percentile latency
		if strings.Contains(line, "95th percentile:") {
			re := regexp.MustCompile(`95th percentile:\s+([0-9.]+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.ParseFloat(matches[1], 64); err == nil {
					result.Latency95 = time.Duration(val * float64(time.Millisecond))
					result.P95 = result.Latency95
				}
			}
		}

		// Parse ignored errors
		if strings.Contains(line, "ignored errors:") {
			re := regexp.MustCompile(`ignored errors:\s+(\d+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				if val, err := strconv.Atoi(matches[1]); err == nil {
					result.Errors = val
				}
			}
		}
	}

	// Try to get percentiles from histogram if available
	p50, p95, p99, err := ParseLatencyHistogram(output)
	if err == nil {
		result.P50 = p50
		result.P95 = p95
		result.P99 = p99
	} else {
		// Use approximations if histogram not available
		result.P50 = result.LatencyAvg
		if result.P95 == 0 {
			result.P95 = result.Latency95
		}
		result.P99 = result.LatencyMax
	}

	// Validation
	if result.TPS == 0 && result.TotalEvents > 0 && result.TotalTime > 0 {
		result.TPS = float64(result.TotalEvents) / result.TotalTime.Seconds()
	}

	if result.TPS == 0 {
		return nil, fmt.Errorf("failed to parse TPS from sysbench output")
	}
	if result.TotalEvents == 0 {
		return nil, fmt.Errorf("failed to parse event count from sysbench output")
	}

	return result, nil
}
