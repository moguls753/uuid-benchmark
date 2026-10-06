package main

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/gocql/gocql"
	"github.com/moguls753/uuid-benchmark/internal/ih"
)

// The fake rejects any partition other than 1 and any bounded target query.
// Thus the real IH adapter, including final verification, must use the same
// partition rather than the Multinode N=1 (bucket 0) path.
type fakeIHDB struct {
	t                                     *testing.T
	rows                                  []any
	preload                               map[string]bool
	batches, fetches, reads, inserts      int
	batchErr, iterErr, readErr, insertErr error
}

func (d *fakeIHDB) NewBatch(kind gocql.BatchType) *gocql.Batch {
	if kind != gocql.UnloggedBatch {
		d.t.Fatalf("batch type = %v", kind)
	}
	return gocql.NewBatch(kind)
}
func (d *fakeIHDB) checkBucket(args []any) {
	d.t.Helper()
	if len(args) == 0 || args[0] != 1 {
		d.t.Fatalf("IH bucket arguments = %v, want fixed bucket 1", args)
	}
}
func (d *fakeIHDB) ExecuteBatch(b *gocql.Batch) error {
	d.batches++
	if len(b.Entries) != 100 {
		d.t.Fatalf("preload batch size = %d", len(b.Entries))
	}
	for _, e := range b.Entries {
		if e.Stmt != cassandraInsertQuery {
			d.t.Fatalf("batch query = %s", e.Stmt)
		}
		d.checkBucket(e.Args)
		if len(e.Args) != 3 {
			d.t.Fatalf("insert args = %v", e.Args)
		}
	}
	if d.batchErr != nil {
		return d.batchErr
	}
	for _, e := range b.Entries {
		d.rows = append(d.rows, e.Args[1])
		d.preload[fmt.Sprint(e.Args[1])] = true
	}
	return nil
}
func (d *fakeIHDB) IHQuery(stmt string, args ...any) ihCassandraQuery {
	d.checkBucket(args)
	return &fakeIHQuery{db: d, stmt: stmt, args: args}
}

type fakeIHQuery struct {
	db   *fakeIHDB
	stmt string
	args []any
}

func (q *fakeIHQuery) Exec() error {
	if q.stmt != cassandraInsertQuery || len(q.args) != 3 {
		q.db.t.Fatalf("mixed insert = %s %v", q.stmt, q.args)
	}
	q.db.inserts++
	if q.db.insertErr != nil {
		return q.db.insertErr
	}
	q.db.rows = append(q.db.rows, q.args[1])
	return nil
}
func (q *fakeIHQuery) Scan(dest ...any) error {
	if q.stmt != cassandraReadQuery || len(q.args) != 2 {
		q.db.t.Fatalf("read query = %s %v", q.stmt, q.args)
	}
	q.db.reads++
	if !q.db.preload[fmt.Sprint(q.args[1])] {
		q.db.t.Fatal("read target was not in the fixed preload pool")
	}
	if q.db.readErr != nil {
		return q.db.readErr
	}
	*dest[0].(*[]byte) = []byte("payload")
	return nil
}
func (q *fakeIHQuery) IHIter() ihCassandraIter {
	if q.stmt != "SELECT id FROM bench WHERE bucket = ?" || len(q.args) != 1 {
		q.db.t.Fatalf("unbounded ID query = %s %v", q.stmt, q.args)
	}
	q.db.fetches++
	return &fakeIHIter{rows: q.db.rows, err: q.db.iterErr}
}

type fakeIHIter struct {
	rows []any
	pos  int
	buf  []byte
	err  error
}

func (i *fakeIHIter) Scan(dest ...any) bool {
	if i.pos == len(i.rows) {
		return false
	}
	v := i.rows[i.pos]
	i.pos++
	switch p := dest[0].(type) {
	case *int64:
		*p = v.(int64)
	case *gocql.UUID:
		*p = v.(gocql.UUID)
	case *[]byte:
		// Reuse the driver buffer to ensure the adapter copies blob identifiers.
		i.buf = append(i.buf[:0], v.([]byte)...)
		*p = i.buf
	default:
		panic("unexpected scanner type")
	}
	return true
}
func (i *fakeIHIter) Close() error { return i.err }

func newIHTestBackend(t *testing.T, scheme string) (ih.Config, ih.Backend, *fakeIHDB) {
	t.Helper()
	c := ih.Config{Engine: "cassandra", Scheme: scheme, Preload: 200, Operations: 500, Seed: 42}
	d := &fakeIHDB{t: t, preload: map[string]bool{}}
	var counter atomic.Int64
	kg := newKeyGenerator(scheme, &counter)
	b := ih.Backend{Prepare: func(max int64) error { counter.Store(max); return nil }}
	toKey := func(v any) ih.Key {
		k := ih.Key{Value: v, Token: fmt.Sprintf("%T:%v", v, v)}
		if n, ok := v.(int64); ok {
			k.Sequential = n
		}
		return k
	}
	return c, correctedCassandraBackend(c, d, kg, b, toKey), d
}

func TestCorrectedCassandraFixedPartitionLifecycle(t *testing.T) {
	for _, scheme := range []string{"sequential", "uuidv1", "uuidv4", "uuidv7", "ulid", "ulid_monotonic"} {
		t.Run(scheme, func(t *testing.T) {
			c, b, d := newIHTestBackend(t, scheme)
			r := ih.Run(c, b)
			if !r.Valid {
				t.Fatalf("IH invalid: %+v", r)
			}
			if d.batches != 2 || d.fetches != 3 || d.reads == 0 || d.inserts == 0 {
				t.Fatalf("paths not exercised: batches=%d fetches=%d reads=%d inserts=%d", d.batches, d.fetches, d.reads, d.inserts)
			}
			if r.TargetCount != c.Preload || r.EndCount != c.Preload+r.Inserts || r.ReadMisses != 0 {
				t.Fatalf("counters: %+v", r)
			}
			if bucketForIDValue(d.rows[0], 1) != 0 {
				t.Fatal("Multinode N=1 contract changed")
			}
		})
	}
}

func TestCorrectedCassandraFailuresAreNotAccepted(t *testing.T) {
	boom := errors.New("injected failure")
	for _, kind := range []string{"preload", "fetch", "insert", "read", "read_miss"} {
		t.Run(kind, func(t *testing.T) {
			c, b, d := newIHTestBackend(t, "sequential")
			switch kind {
			case "preload":
				d.batchErr = boom
			case "fetch":
				d.iterErr = boom
			case "insert":
				d.insertErr = boom
			case "read":
				d.readErr = boom
			case "read_miss":
				d.readErr = gocql.ErrNotFound
			}
			r := ih.Run(c, b)
			if r.Valid || r.Failure == "" {
				t.Fatalf("failure accepted: %+v", r)
			}
			if kind == "read_miss" && (r.ReadMisses != 1 || r.Errors["read_miss"] != 1) {
				t.Fatalf("miss not recorded: %+v", r)
			}
			if kind == "insert" || kind == "read" || kind == "read_miss" {
				if r.Errors[kind] != 1 || d.fetches != 3 {
					t.Fatalf("error/end verification missing: %+v fetches=%d", r, d.fetches)
				}
			} else if !r.MixedStarted.IsZero() {
				t.Fatal("mixed phase started after invalid preload/fetch")
			}
		})
	}
}

func TestCorrectedCassandraFinalVerificationRejectsMissingRows(t *testing.T) {
	c, b, d := newIHTestBackend(t, "sequential")
	count := b.Count
	calls := 0
	b.Count = func() (int64, ih.Range, error) {
		calls++
		if calls == 2 {
			// Simulate an acknowledged insert that is absent at final verification.
			d.rows = d.rows[:len(d.rows)-1]
		}
		return count()
	}
	r := ih.Run(c, b)
	if r.Valid || r.Failure == "" || r.Completed != c.Operations || calls != 2 {
		t.Fatalf("end-count mismatch not rejected: %+v, counts=%d", r, calls)
	}
}

func TestCorrectedCassandraIDScanCopiesBlobsAndPropagatesErrors(t *testing.T) {
	_, b, d := newIHTestBackend(t, "ulid")
	d.rows = []any{[]byte{1, 2}, []byte{3, 4}}
	ids, err := b.IDs()
	if err != nil || len(ids) != 2 || !reflect.DeepEqual(ids[0].Value, []byte{1, 2}) || !reflect.DeepEqual(ids[1].Value, []byte{3, 4}) {
		t.Fatalf("aliased/incomplete IDs: %v %v", ids, err)
	}
	d.iterErr = errors.New("incomplete scan")
	if _, err := b.IDs(); !errors.Is(err, d.iterErr) {
		t.Fatalf("scan error = %v", err)
	}
	if _, _, err := b.Count(); !errors.Is(err, d.iterErr) {
		t.Fatalf("count error = %v", err)
	}
}
