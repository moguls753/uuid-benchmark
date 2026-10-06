package ih

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func fakeBackend(c Config) (Backend, *int64) {
	n := c.Preload
	next := int64(0)
	b := Backend{
		Preload: func() error { return nil },
		IDs: func() ([]Key, error) {
			ids := make([]Key, c.Preload)
			for i := range ids {
				id := int64(i + 1)
				ids[i] = Key{Value: id, Token: fmt.Sprint(id), Sequential: id}
			}
			return ids, nil
		},
		Count:   func() (int64, Range, error) { return n, Range{1, n}, nil },
		Prepare: func(max int64) error { next = max; return nil },
		Insert:  func() (int64, error) { next++; n++; return next, nil },
		Read: func(v any) error {
			if v.(int64) < 1 || v.(int64) > c.Preload {
				return fmt.Errorf("read outside fixed preload")
			}
			return nil
		},
	}
	return b, &n
}
func TestCorrectedCounterAndImmutableTargets(t *testing.T) {
	c := Config{Engine: "cassandra", Scheme: "sequential", Preload: 100, Operations: 2000, Seed: 42}
	b, _ := fakeBackend(c)
	r := Run(c, b)
	if !r.Valid {
		t.Fatal(r.Failure)
	}
	if r.InsertRange.Min != 101 || r.EndCount != 100+r.Inserts {
		t.Fatalf("counter not continued: %+v", r)
	}
	// Same seed repeats the operation mix and read target choices, never UUID entropy.
	b, _ = fakeBackend(c)
	again := Run(c, b)
	if r.Inserts != again.Inserts || r.Reads != again.Reads {
		t.Fatal("non-reproducible choices")
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Inserts != r.Inserts || parsed.PreloadRange != r.PreloadRange {
		t.Fatal("JSON roundtrip")
	}
	if _, err := Parse(strings.NewReader(string(data) + "{}")); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}
func TestRejectDuplicateTargets(t *testing.T) {
	c := Config{Engine: "mongodb", Scheme: "sequential", Preload: 100, Operations: 100, Seed: 1}
	b, _ := fakeBackend(c)
	ids, _ := b.IDs()
	ids[1] = ids[0]
	b.IDs = func() ([]Key, error) { return ids, nil }
	r := Run(c, b)
	if r.Valid || r.Completed != 0 {
		t.Fatalf("duplicate accepted: %+v", r)
	}
}
func TestRejectUpsertAndReadMiss(t *testing.T) {
	c := Config{Engine: "cassandra", Scheme: "sequential", Preload: 100, Operations: 100, Seed: 1}
	t.Run("upsert", func(t *testing.T) {
		b, _ := fakeBackend(c)
		b.Insert = func() (int64, error) { return 1, nil }
		r := Run(c, b)
		if r.Valid || !strings.Contains(r.Failure, "cardinality") {
			t.Fatalf("upsert accepted: %+v", r)
		}
	})
	t.Run("read-miss", func(t *testing.T) {
		b, _ := fakeBackend(c)
		b.Read = func(any) error { return ErrReadMiss }
		r := Run(c, b)
		if r.Valid || r.ReadMisses != 1 || r.Completed >= c.Operations || r.ReadAttempts != r.Reads+1 {
			t.Fatalf("miss not stopped: %+v", r)
		}
	})
	t.Run("insert-error", func(t *testing.T) {
		b, _ := fakeBackend(c)
		b.Insert = func() (int64, error) { return 0, fmt.Errorf("duplicate key") }
		r := Run(c, b)
		if r.Valid || r.InsertAttempts != 1 || r.Inserts != 0 || r.Errors["insert"] != 1 {
			t.Fatal(r)
		}
	})
	t.Run("fetch-error", func(t *testing.T) {
		b, _ := fakeBackend(c)
		b.IDs = func() ([]Key, error) { return nil, fmt.Errorf("cursor failed") }
		r := Run(c, b)
		if r.Valid || r.Completed != 0 {
			t.Fatal(r)
		}
	})
}
func TestPGLogAndScripts(t *testing.T) {
	r := NewResult(Config{})
	if err := ParsePGLog([]byte("0 0 20 0 100 1\n0 1 35 1 100 2\n0 2 failed 0 100 3\n"), r); err != nil {
		t.Fatal(err)
	}
	if r.Inserts != 1 || r.Reads != 1 || r.InsertAttempts != 2 || r.Errors["pgbench_failed"] != 1 || r.P99 != 35 {
		t.Fatalf("bad counters: %+v", r)
	}
	for _, bad := range []string{"0 0 1 2 100 1\n", "0 0 1 0 100 1\n0 0 1 0 100 1\n", "0 0 1 0 100 1 9\n", "1 0 1 0 100 1\n"} {
		if err := ParsePGLog([]byte(bad), NewResult(Config{})); err == nil {
			t.Fatalf("accepted bad log %q", bad)
		}
	}
	for _, scheme := range []string{"sequential", "uuidv1", "uuidv4", "uuidv7", "ulid", "ulid_monotonic"} {
		ins, read := PostgresScripts(scheme, 100000)
		if !strings.Contains(ins, "RETURNING id \\gset") || !strings.Contains(read, "random(1, 100000)") || !strings.Contains(read, "\\gset read_") || strings.Contains(read, ";") {
			t.Fatalf("invalid scripts: %s\n%s", ins, read)
		}
	}
}
func TestConfigRejectsUnsupportedProtocol(t *testing.T) {
	for _, c := range []Config{{Engine: "postgres", Scheme: "objectid", Preload: 100, Operations: 1}, {Engine: "mysql", Scheme: "uuidv4", Preload: 101, Operations: 1}, {Engine: "mysql", Scheme: "uuidv4", Preload: 100, Operations: 0}} {
		if c.Validate() == nil {
			t.Fatal(c)
		}
	}
}
