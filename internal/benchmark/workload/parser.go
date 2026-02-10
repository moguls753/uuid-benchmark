package workload

import (
	"encoding/json"
	"fmt"
	"time"
)

// WorkloadResult holds parsed results from the workload binary.
type WorkloadResult struct {
	Throughput float64
	LatencyP50 time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration
	TotalOps   int
	Duration   time.Duration
	Errors     int
	InsertOps  int
	ReadOps    int
	UpdateOps  int
}

type rawResult struct {
	Throughput float64 `json:"throughput"`
	LatencyP50 int64   `json:"latency_p50_us"`
	LatencyP95 int64   `json:"latency_p95_us"`
	LatencyP99 int64   `json:"latency_p99_us"`
	TotalOps   int     `json:"total_ops"`
	DurationMs int64   `json:"duration_ms"`
	Errors     int     `json:"errors"`
	InsertOps  int     `json:"insert_ops"`
	ReadOps    int     `json:"read_ops"`
	UpdateOps  int     `json:"update_ops"`
}

// ParseResult parses JSON output from the workload binary.
func ParseResult(output string) (*WorkloadResult, error) {
	var raw rawResult
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	return &WorkloadResult{
		Throughput: raw.Throughput,
		LatencyP50: time.Duration(raw.LatencyP50) * time.Microsecond,
		LatencyP95: time.Duration(raw.LatencyP95) * time.Microsecond,
		LatencyP99: time.Duration(raw.LatencyP99) * time.Microsecond,
		TotalOps:   raw.TotalOps,
		Duration:   time.Duration(raw.DurationMs) * time.Millisecond,
		Errors:     raw.Errors,
		InsertOps:  raw.InsertOps,
		ReadOps:    raw.ReadOps,
		UpdateOps:  raw.UpdateOps,
	}, nil
}
