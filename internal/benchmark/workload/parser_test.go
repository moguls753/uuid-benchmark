package workload

import (
	"testing"
	"time"
)

func TestParseResult(t *testing.T) {
	t.Parallel()

	t.Run("valid JSON with all fields", func(t *testing.T) {
		t.Parallel()
		input := `{
			"throughput": 12345.67,
			"latency_p50_us": 500,
			"latency_p95_us": 2000,
			"latency_p99_us": 5000,
			"total_ops": 100000,
			"duration_ms": 8100,
			"errors": 3,
			"not_found": 7,
			"fetch_ms": 1500,
			"id_file_sha256": "deadbeef",
			"insert_ops": 90000,
			"read_ops": 5000,
			"update_ops": 5000
		}`
		result, err := ParseResult(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Throughput != 12345.67 {
			t.Errorf("Throughput = %v, want 12345.67", result.Throughput)
		}
		if result.LatencyP50 != 500*time.Microsecond {
			t.Errorf("LatencyP50 = %v, want 500us", result.LatencyP50)
		}
		if result.LatencyP95 != 2000*time.Microsecond {
			t.Errorf("LatencyP95 = %v, want 2ms", result.LatencyP95)
		}
		if result.LatencyP99 != 5000*time.Microsecond {
			t.Errorf("LatencyP99 = %v, want 5ms", result.LatencyP99)
		}
		if result.TotalOps != 100000 {
			t.Errorf("TotalOps = %d, want 100000", result.TotalOps)
		}
		if result.Duration != 8100*time.Millisecond {
			t.Errorf("Duration = %v, want 8.1s", result.Duration)
		}
		if result.Errors != 3 {
			t.Errorf("Errors = %d, want 3", result.Errors)
		}
		if result.InsertOps != 90000 {
			t.Errorf("InsertOps = %d, want 90000", result.InsertOps)
		}
		if result.ReadOps != 5000 {
			t.Errorf("ReadOps = %d, want 5000", result.ReadOps)
		}
		if result.UpdateOps != 5000 {
			t.Errorf("UpdateOps = %d, want 5000", result.UpdateOps)
		}
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		t.Parallel()
		_, err := ParseResult(`{invalid json}`)
		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}
	})

	t.Run("empty string returns error", func(t *testing.T) {
		t.Parallel()
		_, err := ParseResult("")
		if err == nil {
			t.Fatal("expected error for empty string")
		}
	})
}

// The JSON tags on both sides of this boundary have to stay in step. A rename
// in cmd/workload would otherwise zero not_found and fetch_ms in every CSV
// without any error: only id_file_sha256 is backstopped, because an empty
// digest makes the runner's read-set comparison fail loudly.
func TestParseResultCarriesReadSetFields(t *testing.T) {
	t.Parallel()

	result, err := ParseResult(`{
		"throughput": 100,
		"total_ops": 1000,
		"duration_ms": 10000,
		"errors": 2,
		"not_found": 5,
		"fetch_ms": 2500,
		"id_file_sha256": "abc123"
	}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NotFound != 5 {
		t.Errorf("NotFound = %d, want 5", result.NotFound)
	}
	if result.FetchDuration != 2500*time.Millisecond {
		t.Errorf("FetchDuration = %v, want 2.5s", result.FetchDuration)
	}
	if result.IDFileSHA256 != "abc123" {
		t.Errorf("IDFileSHA256 = %q, want abc123", result.IDFileSHA256)
	}
}
