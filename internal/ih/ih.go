// Package ih implements the opt-in corrected, single-client insert-heavy protocol.
// It intentionally does not change the historical mixed workload runners.
package ih

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

const Protocol = "ih-fixed-preload-v1"

var ErrReadMiss = errors.New("read target not found")

type Config struct {
	Engine     string `json:"engine"`
	Scheme     string `json:"scheme"`
	Preload    int64  `json:"requested_preload"`
	Operations int64  `json:"requested_ops"`
	Seed       uint64 `json:"seed"`
}

func (c Config) Validate() error {
	if c.Preload <= 0 || c.Preload%100 != 0 || c.Operations <= 0 {
		return fmt.Errorf("positive preload divisible by 100 and positive operations required")
	}
	switch c.Engine {
	case "mysql", "mongodb", "cassandra", "postgres":
	default:
		return fmt.Errorf("unknown engine %q", c.Engine)
	}
	switch c.Scheme {
	case "sequential", "uuidv1", "uuidv4", "uuidv7", "ulid", "ulid_monotonic":
	case "objectid":
		if c.Engine != "mongodb" {
			return fmt.Errorf("objectid requires mongodb")
		}
	default:
		return fmt.Errorf("unknown scheme %q", c.Scheme)
	}
	return nil
}

type Range struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}
type Result struct {
	Config
	Protocol       string             `json:"protocol"`
	Started        time.Time          `json:"started"`
	Ended          time.Time          `json:"ended"`
	MixedStarted   time.Time          `json:"mixed_started"`
	MixedEnded     time.Time          `json:"mixed_ended"`
	Phases         map[string]float64 `json:"phase_seconds"`
	PreloadCount   int64              `json:"preload_count"`
	TargetCount    int64              `json:"unique_read_targets"`
	EndCount       int64              `json:"end_count"`
	PreloadRange   Range              `json:"preload_sequential_range"`
	InsertRange    Range              `json:"insert_sequential_range"`
	EndRange       Range              `json:"end_sequential_range"`
	InsertAttempts int64              `json:"insert_attempts"`
	Inserts        int64              `json:"insert_successes"`
	ReadAttempts   int64              `json:"read_attempts"`
	Reads          int64              `json:"read_successes"`
	Completed      int64              `json:"completed_ops"`
	ReadMisses     int64              `json:"read_misses"`
	Errors         map[string]int64   `json:"errors"`
	Throughput     float64            `json:"throughput"`
	P50            int64              `json:"latency_p50_us"`
	P95            int64              `json:"latency_p95_us"`
	P99            int64              `json:"latency_p99_us"`
	Valid          bool               `json:"valid"`
	Failure        string             `json:"failure,omitempty"`
}

func NewResult(c Config) *Result {
	return &Result{Config: c, Protocol: Protocol, Started: time.Now().UTC(), Phases: map[string]float64{}, Errors: map[string]int64{}}
}
func (r *Result) Finish(err error) {
	r.Ended = time.Now().UTC()
	r.Phases["total"] = r.Ended.Sub(r.Started).Seconds()
	if err == nil {
		err = r.Validate()
	}
	r.Valid = err == nil
	if err != nil {
		r.Failure = err.Error()
	}
}
func (r *Result) Validate() error {
	if err := r.Config.Validate(); err != nil {
		return err
	}
	if r.Protocol != Protocol || r.PreloadCount != r.Preload || r.TargetCount != r.Preload {
		return fmt.Errorf("preload/target/protocol mismatch")
	}
	if r.Completed != r.Operations || r.Inserts+r.Reads != r.Completed || r.InsertAttempts != r.Inserts || r.ReadAttempts != r.Reads {
		return fmt.Errorf("operation counters do not reconcile")
	}
	if r.ReadMisses != 0 || len(r.Errors) != 0 {
		return fmt.Errorf("errors or read misses")
	}
	if r.EndCount != r.PreloadCount+r.Inserts {
		return fmt.Errorf("cardinality mismatch: end=%d preload=%d inserts=%d", r.EndCount, r.PreloadCount, r.Inserts)
	}
	if r.Scheme == "sequential" {
		if r.PreloadRange.Min != 1 || r.PreloadRange.Max != r.Preload || r.EndRange.Min != 1 || r.EndRange.Max != r.EndCount {
			return fmt.Errorf("invalid sequential extent")
		}
		if r.Inserts > 0 && (r.InsertRange.Min != r.PreloadRange.Max+1 || r.InsertRange.Max != r.EndRange.Max) {
			return fmt.Errorf("sequential inserts overlap preload or have gaps")
		}
	}
	if r.MixedStarted.IsZero() || !r.MixedEnded.After(r.MixedStarted) || r.Throughput <= 0 || math.IsNaN(r.Throughput) || math.IsInf(r.Throughput, 0) {
		return fmt.Errorf("missing/invalid mixed timing")
	}
	return nil
}
func Parse(reader io.Reader) (*Result, error) {
	var r Result
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing result data")
	}
	if !r.Valid || r.Failure != "" {
		return &r, fmt.Errorf("invalid run: %s", r.Failure)
	}
	return &r, r.Validate()
}

// IDs are captured once before MixedStarted. No newly inserted ID can enter this slice.
type Key struct {
	Value      any
	Token      string
	Sequential int64
}
type Backend struct {
	Preload func() error
	IDs     func() ([]Key, error)
	Count   func() (int64, Range, error)
	Prepare func(max int64) error
	Insert  func() (int64, error) // returned integer is only used for sequential ranges
	Read    func(any) error
}

func VerifyTargets(ids []Key, expected int64) (Range, error) {
	if int64(len(ids)) != expected {
		return Range{}, fmt.Errorf("expected %d IDs, got %d", expected, len(ids))
	}
	seen := make(map[string]bool, len(ids))
	extent := Range{}
	for i, id := range ids {
		if id.Token == "" || seen[id.Token] {
			return Range{}, fmt.Errorf("empty or duplicate preload ID")
		}
		seen[id.Token] = true
		if i == 0 || id.Sequential < extent.Min {
			extent.Min = id.Sequential
		}
		if id.Sequential > extent.Max {
			extent.Max = id.Sequential
		}
	}
	return extent, nil
}
func Percentiles(latencies []int64) (int64, int64, int64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	return latencies[n*50/100], latencies[n*95/100], latencies[n*99/100]
}
func Run(c Config, b Backend) (r *Result) {
	r = NewResult(c)
	var err error
	defer func() { r.Finish(err) }()
	if err = c.Validate(); err != nil {
		return
	}
	start := time.Now()
	err = b.Preload()
	r.Phases["preload"] = time.Since(start).Seconds()
	if err != nil {
		return
	}
	start = time.Now()
	var ids []Key
	ids, err = b.IDs()
	if err != nil {
		return
	}
	r.PreloadRange, err = VerifyTargets(ids, c.Preload)
	if err != nil {
		return
	}
	r.TargetCount = int64(len(ids))
	var extent Range
	r.PreloadCount, extent, err = b.Count()
	if err != nil {
		return
	}
	if r.PreloadCount != c.Preload || (c.Scheme == "sequential" && extent != r.PreloadRange) {
		err = fmt.Errorf("preload count/range mismatch")
		return
	}
	err = b.Prepare(r.PreloadRange.Max)
	if err != nil {
		return
	}
	r.Phases["verify_preload"] = time.Since(start).Seconds()
	rng := rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15))
	latencies := make([]int64, 0, c.Operations)
	r.MixedStarted = time.Now().UTC()
	for i := int64(0); i < c.Operations; i++ {
		insert := rng.IntN(100) < 70
		var target any
		if !insert {
			target = ids[rng.IntN(len(ids))].Value
		}
		opStart := time.Now()
		if insert {
			r.InsertAttempts++
			var id int64
			id, err = b.Insert()
			if err == nil {
				r.Inserts++
				if c.Scheme == "sequential" {
					if r.Inserts == 1 || id < r.InsertRange.Min {
						r.InsertRange.Min = id
					}
					if id > r.InsertRange.Max {
						r.InsertRange.Max = id
					}
				}
			}
		} else {
			r.ReadAttempts++
			err = b.Read(target)
			if err == nil {
				r.Reads++
			} else if errors.Is(err, ErrReadMiss) {
				r.ReadMisses++
			}
		}
		latencies = append(latencies, time.Since(opStart).Microseconds())
		if err != nil {
			kind := "read"
			if insert {
				kind = "insert"
			}
			if errors.Is(err, ErrReadMiss) {
				kind = "read_miss"
			}
			r.Errors[kind]++
			break
		}
		r.Completed++
	}
	r.MixedEnded = time.Now().UTC()
	duration := r.MixedEnded.Sub(r.MixedStarted).Seconds()
	r.Phases["mixed"] = duration
	r.Throughput = float64(r.Completed) / duration
	r.P50, r.P95, r.P99 = Percentiles(latencies)
	// Always attempt the independent end count, even after an operation error.
	start = time.Now()
	var countErr error
	r.EndCount, r.EndRange, countErr = b.Count()
	r.Phases["verify_end"] = time.Since(start).Seconds()
	if err == nil {
		err = countErr
	}
	return
}
