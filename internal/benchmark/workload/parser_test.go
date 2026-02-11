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
