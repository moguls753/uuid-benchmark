package main

import (
	"crypto/rand"
	"errors"
	"os"
	"slices"
	"strings"
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

// makeFakeDataset returns a cassandraBucketQuery that simulates a dataset
// distributed evenly across numBuckets partitions, with `rowsPerBucket`
// rows in each bucket. ids are gocql.UUIDs with a recoverable bucket value
// encoded in the first byte so tests can verify the resulting sample's
// bucket distribution via bucketForIDValue.
func makeFakeDataset(numBuckets, rowsPerBucket int) cassandraBucketQuery {
	return func(bucket, perBucketLimit int) ([]any, error) {
		take := perBucketLimit
		if take > rowsPerBucket {
			take = rowsPerBucket
		}
		out := make([]any, 0, take)
		for r := 0; r < take; r++ {
			var u gocql.UUID
			// Encode (bucket, row) so the test can confirm bucket coverage
			// without relying on bucketForIDValue agreeing with the synthetic
			// scheme. We check coverage by mapping returned ids through
			// bucketForIDValue with the same numBuckets — the *spread* (number
			// of distinct buckets hit) is what FIX 1 asserts.
			u[0] = byte(bucket)
			u[1] = byte(bucket >> 8)
			u[15] = byte(r)
			out = append(out, u)
		}
		return out, nil
	}
}

func TestFetchIDsAcrossBucketsSpreadsSample(t *testing.T) {
	t.Parallel()
	// FIX 1 contract: with N=1000 buckets and a populated dataset, the
	// returned sample must hit a large fraction of distinct buckets — NOT
	// concentrate on the first few partitions as the previous token-order
	// LIMIT scan did. With perBucket = ceil(1000/1000) = 1, we'll fetch
	// 1 row from each of 1000 buckets and hit ~all of them (with some
	// FNV-1a hashing collapse, since we re-hash via bucketForIDValue to
	// classify the returned ids).
	const numBuckets = 1000
	const limit = 1000
	dataset := makeFakeDataset(numBuckets, 5) // 5 rows in each bucket

	ids, err := fetchIDsAcrossBuckets(dataset, limit, numBuckets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) > limit {
		t.Errorf("got %d ids, expected at most %d", len(ids), limit)
	}
	if len(ids) == 0 {
		t.Fatalf("got 0 ids, expected ~%d", limit)
	}

	// Count distinct buckets hit when classifying the sample via
	// bucketForIDValue (the same hash used by the workload's read path).
	// A serial-LIMIT implementation would concentrate the sample on a
	// few buckets; spread across ≥ 50% of buckets is the FIX 1 contract.
	seen := make(map[int]bool, numBuckets)
	for _, id := range ids {
		seen[bucketForIDValue(id, numBuckets)] = true
	}
	const threshold = numBuckets / 2
	if len(seen) < threshold {
		t.Errorf("sample concentrated on %d/%d buckets (want ≥ %d) — bucket-spread regression",
			len(seen), numBuckets, threshold)
	}
}

func TestFetchIDsAcrossBucketsRespectsLimit(t *testing.T) {
	t.Parallel()
	// Pin the "no over-fetching" contract: with a dense dataset (many rows
	// per bucket), the iteration must stop as soon as `limit` ids have been
	// accumulated, not drain every bucket. Otherwise large numBuckets
	// values would balloon the sample size and the read workload would
	// process more ops than requested.
	const numBuckets = 100
	const limit = 250
	dataset := makeFakeDataset(numBuckets, 100) // 100 rows per bucket = 10k total

	ids, err := fetchIDsAcrossBuckets(dataset, limit, numBuckets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) > limit {
		t.Errorf("over-fetched: got %d ids, want ≤ %d", len(ids), limit)
	}
	// Should also reach exactly the limit since the dataset is dense.
	if len(ids) != limit {
		t.Errorf("got %d ids, expected exactly %d (dense dataset)", len(ids), limit)
	}
}

func TestFetchIDsAcrossBucketsPropagatesErrors(t *testing.T) {
	t.Parallel()
	// Per-bucket query errors must propagate, not be silently swallowed.
	// Silently returning a short sample would bias the downstream workload
	// onto whatever buckets succeeded before the failure.
	wantErr := errors.New("bucket query exploded")
	failingQuery := cassandraBucketQuery(func(bucket, perBucketLimit int) ([]any, error) {
		// Fail on the 3rd bucket — after some successful ones, so the test
		// also covers the partial-then-fail case.
		if bucket == 2 {
			return nil, wantErr
		}
		return []any{gocql.UUID{byte(bucket)}}, nil
	})
	_, err := fetchIDsAcrossBuckets(failingQuery, 100, 10)
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error %v, got %v", wantErr, err)
	}
}

func TestFetchIDsAcrossBucketsZeroLimit(t *testing.T) {
	t.Parallel()
	// Defensive: limit <= 0 returns no ids without invoking the query —
	// guards against an over-zealous caller wasting RTTs on a no-op.
	called := false
	q := cassandraBucketQuery(func(bucket, perBucketLimit int) ([]any, error) {
		called = true
		return nil, nil
	})
	ids, err := fetchIDsAcrossBuckets(q, 0, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("got %d ids, want 0", len(ids))
	}
	if called {
		t.Error("query invoked for limit=0; want short-circuit")
	}
}

func TestSampleIndicesDrawsDistinctAscendingIndices(t *testing.T) {
	const k, n = 500, 100000
	got := sampleIndices(k, n, 42)

	if len(got) != k {
		t.Fatalf("got %d indices, want %d", len(got), k)
	}
	seen := make(map[int]bool, k)
	for i, idx := range got {
		if idx < 0 || idx >= n {
			t.Fatalf("index %d out of range [0,%d)", idx, n)
		}
		if seen[idx] {
			t.Fatalf("duplicate index %d", idx)
		}
		seen[idx] = true
		if i > 0 && got[i-1] >= idx {
			t.Fatalf("not ascending at %d: %d >= %d", i, got[i-1], idx)
		}
	}
}

// The insert loop walks record indices in ascending order and advances a
// single cursor, so an unsorted or duplicated draw would silently skip
// sample slots.
func TestSampleIndicesDeterministicPerSeed(t *testing.T) {
	a := sampleIndices(100, 10000, 7)
	b := sampleIndices(100, 10000, 7)
	c := sampleIndices(100, 10000, 8)

	if !slices.Equal(a, b) {
		t.Fatal("same seed produced different samples")
	}
	if slices.Equal(a, c) {
		t.Fatal("different seeds produced identical samples")
	}
}

// k close to n takes the permutation branch; rejection sampling would spin
// there. Smoke-test configurations hit this.
func TestSampleIndicesHandlesDenseAndDegenerateDraws(t *testing.T) {
	dense := sampleIndices(90, 100, 3)
	if len(dense) != 90 {
		t.Fatalf("dense draw: got %d, want 90", len(dense))
	}

	all := sampleIndices(10, 10, 3)
	if len(all) != 10 {
		t.Fatalf("k==n: got %d, want 10", len(all))
	}

	if over := sampleIndices(50, 10, 3); len(over) != 10 {
		t.Fatalf("k>n should clamp to n: got %d, want 10", len(over))
	}
	if empty := sampleIndices(0, 10, 3); empty != nil {
		t.Fatalf("k=0 should return nil, got %v", empty)
	}
	if empty := sampleIndices(5, 0, 3); empty != nil {
		t.Fatalf("n=0 should return nil, got %v", empty)
	}
}

func TestShuffleIDsPermutesDeterministically(t *testing.T) {
	build := func() []any {
		ids := make([]any, 200)
		for i := range ids {
			ids[i] = int64(i)
		}
		return ids
	}

	a, b, c := build(), build(), build()
	shuffleIDs(a, 99)
	shuffleIDs(b, 99)
	shuffleIDs(c, 100)

	if !slices.Equal(a, b) {
		t.Fatal("same seed produced different orders")
	}
	if slices.Equal(a, c) {
		t.Fatal("different seeds produced identical orders")
	}

	seen := make(map[int64]bool, len(a))
	for _, id := range a {
		seen[id.(int64)] = true
	}
	if len(seen) != 200 {
		t.Fatalf("shuffle lost elements: %d of 200 survived", len(seen))
	}

	ordered := build()
	shuffleIDs(ordered, 99)
	if slices.Equal(ordered, build()) {
		t.Fatal("shuffle left the list in insert order, which is disk order for time-ordered keys")
	}
}

func TestIDFileRoundTripsEveryKeyType(t *testing.T) {
	uuidA, err := gocql.ParseUUID("018f3a1c-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("parse fixture uuid: %v", err)
	}
	uuidB, err := gocql.ParseUUID("018f3a1c-0000-7000-8000-000000000002")
	if err != nil {
		t.Fatalf("parse fixture uuid: %v", err)
	}

	cases := map[string][]any{
		"sequential":     {int64(1), int64(2), int64(9223372036854775807)},
		"uuidv1":         {uuidA, uuidB},
		"uuidv4":         {uuidA, uuidB},
		"uuidv7":         {uuidA, uuidB},
		"ulid":           {[]byte{0x01, 0x02, 0x03}, []byte{0xff, 0xee}},
		"ulid_monotonic": {[]byte{0xaa, 0xbb, 0xcc}},
	}

	for keyType, ids := range cases {
		t.Run(keyType, func(t *testing.T) {
			path := t.TempDir() + "/ids.txt"
			if _, err := writeIDFile(path, keyType, 4242, ids); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, _, err := loadIDFile(path, keyType)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if len(got) != len(ids) {
				t.Fatalf("got %d ids, want %d", len(got), len(ids))
			}
			for i := range ids {
				wantBytes := idAsBytes(ids[i])
				gotBytes := idAsBytes(got[i])
				if !slices.Equal(wantBytes, gotBytes) {
					t.Fatalf("id %d differs: %x vs %x", i, gotBytes, wantBytes)
				}
			}
		})
	}
}

// A file left over from another key type would otherwise produce a run in
// which every lookup misses while the throughput number looks healthy.
func TestLoadIDFileRejectsForeignKeyType(t *testing.T) {
	path := t.TempDir() + "/ids.txt"
	if _, err := writeIDFile(path, "sequential", 4242, []any{int64(1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := loadIDFile(path, "uuidv4"); err == nil {
		t.Fatal("expected key type mismatch to be rejected")
	}
}

func TestLoadIDFileRejectsEmptyFile(t *testing.T) {
	path := t.TempDir() + "/ids.txt"
	if _, err := writeIDFile(path, "sequential", 4242, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := loadIDFile(path, "sequential"); err == nil {
		t.Fatal("expected a file with no ids to be rejected")
	}
}

func TestFormatIDRejectsUnsupportedType(t *testing.T) {
	if _, err := formatID("not-an-id"); err == nil {
		t.Fatal("expected unsupported id type to be rejected")
	}
}

// Threads own unequal slices whenever numRecords does not divide by threads.
// A per-thread draw of a fixed share would then give rows in the shorter
// slices a higher inclusion probability; the global draw must not.
func TestSplitSampleByThreadPreservesGlobalDraw(t *testing.T) {
	const numRecords, threads = 10, 4
	recordsPerThread, remainder := numRecords/threads, numRecords%threads

	global := []int{0, 2, 3, 5, 6, 9}
	got := splitSampleByThread(global, recordsPerThread, remainder, threads)

	if len(got) != threads {
		t.Fatalf("got %d thread slices, want %d", len(got), threads)
	}

	var rebuilt []int
	for tid, local := range got {
		start, end := threadRange(tid, recordsPerThread, remainder)
		for i, idx := range local {
			if idx < 0 || start+idx >= end {
				t.Fatalf("thread %d: local index %d maps outside [%d,%d)", tid, idx, start, end)
			}
			if i > 0 && local[i-1] >= idx {
				t.Fatalf("thread %d: local indices not ascending: %v", tid, local)
			}
			rebuilt = append(rebuilt, start+idx)
		}
	}
	slices.Sort(rebuilt)
	if !slices.Equal(rebuilt, global) {
		t.Fatalf("rebuilt %v, want %v", rebuilt, global)
	}
}

func TestThreadRangesTileInsertOrderExactly(t *testing.T) {
	for _, tc := range []struct{ numRecords, threads int }{
		{10, 4}, {50000000, 8}, {7, 8}, {8, 8}, {1, 1},
	} {
		recordsPerThread, remainder := tc.numRecords/tc.threads, tc.numRecords%tc.threads
		prevEnd := 0
		for tid := 0; tid < tc.threads; tid++ {
			start, end := threadRange(tid, recordsPerThread, remainder)
			if start != prevEnd {
				t.Fatalf("numRecords=%d threads=%d: gap or overlap at thread %d (%d != %d)", tc.numRecords, tc.threads, tid, start, prevEnd)
			}
			prevEnd = end
		}
		if prevEnd != tc.numRecords {
			t.Fatalf("numRecords=%d threads=%d: ranges cover %d rows", tc.numRecords, tc.threads, prevEnd)
		}
	}
}

// Every row must be equally likely to be drawn. With unequal thread slices a
// per-thread fixed share breaks that; this checks the global draw does not.
func TestSampleIndicesUniformAcrossUnequalThreadSlices(t *testing.T) {
	const numRecords, threads, sampleSize, trials = 10, 4, 5, 40000
	recordsPerThread, remainder := numRecords/threads, numRecords%threads

	hits := make([]int, numRecords)
	for trial := 0; trial < trials; trial++ {
		split := splitSampleByThread(sampleIndices(sampleSize, numRecords, int64(trial)), recordsPerThread, remainder, threads)
		for tid, local := range split {
			start, _ := threadRange(tid, recordsPerThread, remainder)
			for _, idx := range local {
				hits[start+idx]++
			}
		}
	}

	want := float64(trials*sampleSize) / float64(numRecords)
	for row, got := range hits {
		if deviation := float64(got)/want - 1; deviation < -0.05 || deviation > 0.05 {
			t.Errorf("row %d drawn %d times, expected about %.0f (%.1f %% off)", row, got, want, deviation*100)
		}
	}
}

// A write cut off halfway leaves a file whose ids all parse. Without the
// header count the run would proceed on a short target set and report a
// plausible throughput over the wrong number of operations.
func TestLoadIDFileRejectsTruncatedBody(t *testing.T) {
	path := t.TempDir() + "/ids.txt"
	if _, err := writeIDFile(path, "sequential", 4242, []any{int64(1), int64(2), int64(3)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if err := os.WriteFile(path, []byte(strings.Join(lines[:len(lines)-1], "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	if _, _, err := loadIDFile(path, "sequential"); err == nil {
		t.Fatal("expected a truncated id file to be rejected")
	}
}

func TestLoadIDFileRejectsMalformedHeader(t *testing.T) {
	path := t.TempDir() + "/ids.txt"
	if err := os.WriteFile(path, []byte("sequential\n1\n2\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := loadIDFile(path, "sequential"); err == nil {
		t.Fatal("expected a header without count and seed to be rejected")
	}
}

// The digest travels with the read set so the runner can prove the phase that
// measured is the phase whose targets were drawn. A file edited between the
// two phases must therefore produce a different digest.
func TestLoadIDFileDigestCoversContent(t *testing.T) {
	path := t.TempDir() + "/ids.txt"
	written, err := writeIDFile(path, "sequential", 4242, []any{int64(1), int64(2), int64(3)})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	_, read, err := loadIDFile(path, "sequential")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if written != read {
		t.Fatalf("writer digest %s, reader digest %s", written, read)
	}

	other := t.TempDir() + "/ids.txt"
	if _, err := writeIDFile(other, "sequential", 4242, []any{int64(1), int64(2), int64(4)}); err != nil {
		t.Fatalf("write second: %v", err)
	}
	_, changed, err := loadIDFile(other, "sequential")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}
	if changed == read {
		t.Fatal("a different target set produced the same digest")
	}
}
