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
	// NotFound counts operations whose query succeeded but matched no row.
	NotFound int
	// FetchDuration is the time the workload spent fetching target ids from
	// the database; zero whenever the ids came from an id file.
	FetchDuration time.Duration
	// IDFileSHA256 fingerprints the read set an insert wrote, empty when the
	// run did not write one.
	IDFileSHA256 string
	InsertOps    int
	ReadOps      int
	UpdateOps    int
}

type rawResult struct {
	Throughput   float64 `json:"throughput"`
	LatencyP50   int64   `json:"latency_p50_us"`
	LatencyP95   int64   `json:"latency_p95_us"`
	LatencyP99   int64   `json:"latency_p99_us"`
	TotalOps     int     `json:"total_ops"`
	DurationMs   int64   `json:"duration_ms"`
	Errors       int     `json:"errors"`
	NotFound     int     `json:"not_found"`
	FetchMs      int64   `json:"fetch_ms"`
	IDFileSHA256 string  `json:"id_file_sha256"`
	InsertOps    int     `json:"insert_ops"`
	ReadOps      int     `json:"read_ops"`
	UpdateOps    int     `json:"update_ops"`
}

// ParseResult parses JSON output from the workload binary.
func ParseResult(output string) (*WorkloadResult, error) {
	var raw rawResult
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	return &WorkloadResult{
		Throughput:    raw.Throughput,
		LatencyP50:    time.Duration(raw.LatencyP50) * time.Microsecond,
		LatencyP95:    time.Duration(raw.LatencyP95) * time.Microsecond,
		LatencyP99:    time.Duration(raw.LatencyP99) * time.Microsecond,
		TotalOps:      raw.TotalOps,
		Duration:      time.Duration(raw.DurationMs) * time.Millisecond,
		Errors:        raw.Errors,
		NotFound:      raw.NotFound,
		FetchDuration: time.Duration(raw.FetchMs) * time.Millisecond,
		IDFileSHA256:  raw.IDFileSHA256,
		InsertOps:     raw.InsertOps,
		ReadOps:       raw.ReadOps,
		UpdateOps:     raw.UpdateOps,
	}, nil
}
