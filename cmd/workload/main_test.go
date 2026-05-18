package main

import (
	"crypto/rand"
	"slices"
	"testing"

	"github.com/gocql/gocql"
)

func TestBucketForIDDeterministic(t *testing.T) {
	id := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	a := bucketForID(id, 1000)
	b := bucketForID(id, 1000)
	if a != b {
		t.Fatalf("not deterministic: %d != %d", a, b)
	}
}

func TestBucketForIDRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		got := bucketForID(id, 100)
		if got < 0 || got >= 100 {
			t.Fatalf("out of range: %d (n=100)", got)
		}
	}
}

func TestBucketForIDDistribution(t *testing.T) {
	const n = 100
	const samples = 10000
	counts := make([]int, n)
	for i := 0; i < samples; i++ {
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		counts[bucketForID(id, n)]++
	}
	// Each bucket should hit ~100 times on average. μ=100, σ≈9.95 per bucket
	// (binomial). Symmetric [50, 150] is ~5σ both sides; expected false-fail
	// rate across 100 buckets is ~6e-5 — safe for CI.
	for i, c := range counts {
		if c < 50 || c > 150 {
			t.Errorf("bucket %d outside [50,150] tolerance: count=%d", i, c)
		}
	}
}

func TestBucketForIDEmptyInput(t *testing.T) {
	// nil and empty slice must produce the same deterministic result —
	// pins the contract for idAsBytes callers that might emit empty bytes.
	if bucketForID(nil, 1000) != bucketForID([]byte{}, 1000) {
		t.Fatal("nil and empty slice must agree")
	}
}

func TestBucketForIDPanicsOnInvalidN(t *testing.T) {
	// Misconfigured numBuckets must fail loudly, not silently route to bucket 0.
	for _, n := range []int{0, -1, -100} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for n=%d, got none", n)
				}
			}()
			_ = bucketForID([]byte{0x01}, n)
		}()
	}
}

func TestIDAsBytes(t *testing.T) {
	t.Run("gocql.UUID returns 16 bytes preserving order", func(t *testing.T) {
		u := gocql.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		got := idAsBytes(u)
		if len(got) != 16 {
			t.Fatalf("len: got %d want 16", len(got))
		}
		for i := 0; i < 16; i++ {
			if got[i] != u[i] {
				t.Errorf("byte %d: got %#x want %#x", i, got[i], u[i])
			}
		}
	})
	t.Run("[]byte passes through", func(t *testing.T) {
		in := []byte{0xaa, 0xbb, 0xcc}
		got := idAsBytes(in)
		if len(got) != 3 || got[0] != 0xaa || got[1] != 0xbb || got[2] != 0xcc {
			t.Fatalf("got %#v", got)
		}
	})
	t.Run("int64 big-endian 8 bytes", func(t *testing.T) {
		got := idAsBytes(int64(0x0102030405060708))
		want := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		if len(got) != 8 {
			t.Fatalf("len: got %d want 8", len(got))
		}
		for i := 0; i < 8; i++ {
			if got[i] != want[i] {
				t.Errorf("byte %d: got %#x want %#x", i, got[i], want[i])
			}
		}
	})
	t.Run("determinism — same input same output", func(t *testing.T) {
		u := gocql.UUID{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
			0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef}
		a := idAsBytes(u)
		b := idAsBytes(u)
		if len(a) != len(b) {
			t.Fatalf("len mismatch")
		}
		for i := range a {
			if a[i] != b[i] {
				t.Errorf("byte %d differs", i)
			}
		}
	})
}

func BenchmarkBucketForID(b *testing.B) {
	id := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bucketForID(id, 1000)
	}
}

func TestBucketForIDValueMatchesBucketForID(t *testing.T) {
	// bucketForIDValue must produce the same hash as bucketForID(idAsBytes(.))
	// for every supported type — otherwise the insert and read/update paths
	// would compute different buckets for the same id, silently breaking reads.
	t.Run("gocql.UUID", func(t *testing.T) {
		u := gocql.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		got := bucketForIDValue(u, 1000)
		want := bucketForID(idAsBytes(u), 1000)
		if got != want {
			t.Errorf("got %d want %d", got, want)
		}
	})
	t.Run("[]byte", func(t *testing.T) {
		b := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		got := bucketForIDValue(b, 1000)
		want := bucketForID(idAsBytes(b), 1000)
		if got != want {
			t.Errorf("got %d want %d", got, want)
		}
	})
	t.Run("int64", func(t *testing.T) {
		v := int64(0x0102030405060708)
		got := bucketForIDValue(v, 1000)
		want := bucketForID(idAsBytes(v), 1000)
		if got != want {
			t.Errorf("got %d want %d", got, want)
		}
	})
}

func TestBucketForIDValuePanicsOnInvalidN(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for n=%d, got none", n)
				}
			}()
			_ = bucketForIDValue(int64(1), n)
		}()
	}
}

func TestBucketForIDValueZeroAlloc(t *testing.T) {
	// Pin the zero-allocation contract for the hot path. The id is boxed once
	// into any outside the measured region (mirroring production, where
	// generateCassandraKey performs the boxing before bucketForIDValue is
	// called). Inside, bucketForIDValue must add 0 allocations per call.
	cases := []struct {
		name string
		key  any
	}{
		{"gocql.UUID", gocql.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}},
		{"[]byte", []byte{0xaa, 0xbb, 0xcc, 0xdd}},
		{"int64", int64(0x0102030405060708)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				_ = bucketForIDValue(tc.key, 1000)
			})
			if allocs != 0 {
				t.Errorf("expected 0 allocs/op, got %.2f", allocs)
			}
		})
	}
}

func TestParseContactPoints(t *testing.T) {
	cases := map[string]struct {
		in   string
		want []string
	}{
		"single host":      {"127.0.0.1", []string{"127.0.0.1"}},
		"three hosts":      {"taurus5,taurus6,taurus7", []string{"taurus5", "taurus6", "taurus7"}},
		"hosts with port":  {"taurus5:9042,taurus6:9042", []string{"taurus5:9042", "taurus6:9042"}},
		"trims whitespace": {" taurus5 , taurus6 ", []string{"taurus5", "taurus6"}},
		"empty input":      {"", []string{}},
		// Lock in the lenient contract documented on parseContactPoints:
		// stray empty entries (leading, trailing, or doubled commas) are
		// dropped rather than rejected. A future refactor that tightens this
		// would silently regress operator ergonomics; this test pins it.
		"strips empty entries": {",taurus5,,taurus6,", []string{"taurus5", "taurus6"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := parseContactPoints(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Errorf("parseContactPoints(%q): got %v want %v", tc.in, got, tc.want)
			}
		})
	}
}

func BenchmarkBucketForIDValue(b *testing.B) {
	var key any = gocql.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bucketForIDValue(key, 1000)
	}
}
