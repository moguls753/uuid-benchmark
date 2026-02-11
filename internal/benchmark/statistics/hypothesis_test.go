package statistics

import (
	"testing"
)

func TestMannWhitneyU(t *testing.T) {
	t.Parallel()

	t.Run("empty group returns 1", func(t *testing.T) {
		t.Parallel()
		p := MannWhitneyU(nil, []float64{1, 2, 3})
		assertFloat(t, "p-value", p, 1.0, 1e-9)
	})

	t.Run("all tied values returns 1", func(t *testing.T) {
		t.Parallel()
		a := []float64{5, 5, 5, 5, 5}
		b := []float64{5, 5, 5, 5, 5}
		p := MannWhitneyU(a, b)
		assertFloat(t, "p-value", p, 1.0, 1e-9)
	})

	t.Run("clearly different groups", func(t *testing.T) {
		t.Parallel()
		a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		b := []float64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110}
		p := MannWhitneyU(a, b)
		if p >= 0.05 {
			t.Errorf("expected p < 0.05 for clearly different groups, got %v", p)
		}
	})

	t.Run("similar groups not significant", func(t *testing.T) {
		t.Parallel()
		a := []float64{10, 11, 12, 13, 14}
		b := []float64{11, 12, 13, 14, 15}
		p := MannWhitneyU(a, b)
		if p < 0.05 {
			t.Errorf("expected p >= 0.05 for similar groups, got %v", p)
		}
	})
}

func TestCompare(t *testing.T) {
	t.Parallel()

	t.Run("median diff percentage", func(t *testing.T) {
		t.Parallel()
		a := Calculate([]float64{100, 100, 100})
		b := Calculate([]float64{120, 120, 120})
		c := Compare(a, b)
		// (120-100)/100 * 100 = 20%
		assertFloat(t, "MedianDiffPct", c.MedianDiffPct, 20.0, 1e-9)
	})

	t.Run("median A is zero no divide by zero", func(t *testing.T) {
		t.Parallel()
		a := Calculate([]float64{0, 0, 0})
		b := Calculate([]float64{5, 5, 5})
		c := Compare(a, b)
		assertFloat(t, "MedianDiffPct", c.MedianDiffPct, 0.0, 1e-9)
	})

	t.Run("significant difference sets all flags", func(t *testing.T) {
		t.Parallel()
		a := Calculate([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
		b := Calculate([]float64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110})
		c := Compare(a, b)
		if !c.Significant {
			t.Error("expected Significant=true")
		}
		if c.HasOverlap {
			t.Error("expected HasOverlap=false")
		}
	})
}
