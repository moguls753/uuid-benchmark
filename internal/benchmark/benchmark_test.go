package benchmark

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		bytes int64
		want  string
	}{
		{"zero bytes", 0, "0 B"},
		{"below KB", 500, "500 B"},
		{"exactly 1 KB", 1024, "1.0 KB"},
		{"fractional KB", 1536, "1.5 KB"},
		{"exactly 1 MB", 1048576, "1.0 MB"},
		{"exactly 1 GB", 1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := FormatBytes(tt.bytes)
			if got != tt.want {
				t.Errorf("FormatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

func TestCalculatePercentiles(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		p50, p95, p99 := CalculatePercentiles(nil)
		if p50 != 0 || p95 != 0 || p99 != 0 {
			t.Errorf("expected all zeros, got p50=%v p95=%v p99=%v", p50, p95, p99)
		}
	})

	t.Run("single element", func(t *testing.T) {
		t.Parallel()
		latencies := []time.Duration{5 * time.Millisecond}
		p50, p95, p99 := CalculatePercentiles(latencies)
		want := 5 * time.Millisecond
		if p50 != want || p95 != want || p99 != want {
			t.Errorf("expected all %v, got p50=%v p95=%v p99=%v", want, p50, p95, p99)
		}
	})

	t.Run("100 elements", func(t *testing.T) {
		t.Parallel()
		latencies := make([]time.Duration, 100)
		for i := range latencies {
			latencies[i] = time.Duration(i+1) * time.Millisecond
		}
		p50, p95, p99 := CalculatePercentiles(latencies)

		if p50 != 51*time.Millisecond {
			t.Errorf("p50 = %v, want 51ms", p50)
		}
		if p95 != 96*time.Millisecond {
			t.Errorf("p95 = %v, want 96ms", p95)
		}
		if p99 != 100*time.Millisecond {
			t.Errorf("p99 = %v, want 100ms", p99)
		}
	})
}
