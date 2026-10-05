package ih

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

// Separate weighted scripts make script_no an independent operation counter.
// gset requires exactly one result row (including full data), unlike SQL success.
func PostgresScripts(scheme string, preload int64) (string, string) {
	table := "bench_" + scheme
	insert := strings.TrimSuffix(pgbench.GenerateInsertScript(scheme, table), ";") + " RETURNING id \\gset inserted_\n"
	read := fmt.Sprintf("\\set rn random(1, %d)\nSELECT * FROM %s WHERE id = (SELECT id FROM %s_ids WHERE rn = :rn) \\gset read_\n", preload, table, table)
	return insert, read
}

// ParsePGLog rejects malformed, sampled, retried, foreign-client and duplicate logs.
// Only successful script executions contribute to success counters and percentiles.
func ParsePGLog(data []byte, r *Result) error {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	seen := map[int64]bool{}
	var latencies []int64
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 6 {
			return fmt.Errorf("invalid pgbench log row %q", scanner.Text())
		}
		if fields[0] != "0" {
			return fmt.Errorf("unexpected pgbench client")
		}
		txn, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || txn < 0 || seen[txn] {
			return fmt.Errorf("invalid/duplicate transaction")
		}
		seen[txn] = true
		script, err := strconv.Atoi(fields[3])
		if err != nil || script < 0 || script > 1 {
			return fmt.Errorf("unknown script")
		}
		for _, field := range fields[4:] {
			n, err := strconv.ParseInt(field, 10, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid log timestamp")
			}
		}
		if script == 0 {
			r.InsertAttempts++
		} else {
			r.ReadAttempts++
		}
		latency, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || latency < 0 {
			r.Errors["pgbench_"+fields[2]]++
			continue
		}
		if script == 0 {
			r.Inserts++
		} else {
			r.Reads++
		}
		r.Completed++
		latencies = append(latencies, latency)
	}
	r.P50, r.P95, r.P99 = Percentiles(latencies)
	return scanner.Err()
}

func RunPostgres(c Config, dsn, dir string) (r *Result) {
	r = NewResult(c)
	var err error
	defer func() { r.Finish(err) }()
	if err = c.Validate(); err != nil {
		return
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return
	} // never reuse evidence directories
	var db *sql.DB
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	execSQL := func(q string) error { _, e := db.Exec(q); return e }
	for _, ext := range []string{"pgstattuple", "pg_walinspect", "\"uuid-ossp\"", "pgx_ulid"} {
		if err = execSQL("CREATE EXTENSION IF NOT EXISTS " + ext); err != nil {
			return
		}
	}
	typ := "UUID"
	if c.Scheme == "sequential" {
		typ = "BIGSERIAL"
	}
	if strings.HasPrefix(c.Scheme, "ulid") {
		typ = "ulid"
	}
	table := "bench_" + c.Scheme
	if err = execSQL(fmt.Sprintf("CREATE TABLE %s (id %s PRIMARY KEY,data BYTEA,created_at TIMESTAMP DEFAULT NOW())", table, typ)); err != nil {
		return
	}
	// Log complete stdout/stderr for both phases; pgbench connects locally inside container.
	run := func(phase string, args ...string) error {
		cmd := exec.Command("pgbench", append([]string{"-U", "benchmark", "-d", "uuid_benchmark", "-n", "-c", "1", "-j", "1", "--exit-on-abort", "--max-tries=1"}, args...)...)
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		e := cmd.Run()
		if writeErr := os.WriteFile(filepath.Join(dir, phase+".stdout"), out.Bytes(), 0600); writeErr != nil {
			return writeErr
		}
		if writeErr := os.WriteFile(filepath.Join(dir, phase+".stderr"), stderr.Bytes(), 0600); writeErr != nil {
			return writeErr
		}
		if e != nil {
			return fmt.Errorf("%s pgbench failed: %w: %s", phase, e, stderr.String())
		}
		return nil
	}
	preloadScript := filepath.Join(dir, "preload.sql")
	if err = os.WriteFile(preloadScript, []byte(pgbench.GenerateMultipleInserts(c.Scheme, table, 100)), 0600); err != nil {
		return
	}
	start := time.Now()
	err = run("preload", "-f", preloadScript, "-t", fmt.Sprint(c.Preload/100))
	r.Phases["preload"] = time.Since(start).Seconds()
	if err != nil {
		return
	}
	count := func() (int64, Range, error) {
		var n int64
		var extent Range
		if c.Scheme == "sequential" {
			e := db.QueryRow("SELECT COUNT(*),COALESCE(MIN(id),0),COALESCE(MAX(id),0) FROM "+table).Scan(&n, &extent.Min, &extent.Max)
			return n, extent, e
		}
		e := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n)
		return n, extent, e
	}
	start = time.Now()
	r.PreloadCount, r.PreloadRange, err = count()
	if err != nil {
		return
	}
	if r.PreloadCount != c.Preload {
		err = fmt.Errorf("preload cardinality mismatch")
		return
	}
	// Same physical lookup layout as the historical path, with uniqueness checked below.
	if err = execSQL(fmt.Sprintf("CREATE TABLE %s_ids AS SELECT ROW_NUMBER() OVER ()::bigint AS rn,id FROM %s", table, table)); err != nil {
		return
	}
	if err = execSQL(fmt.Sprintf("CREATE INDEX ON %s_ids (rn)", table)); err != nil {
		return
	}
	if err = execSQL("ANALYZE " + table + "_ids"); err != nil {
		return
	}
	var distinctIDs, distinctRN, missing, minRN, maxRN int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*),COUNT(DISTINCT l.id),COUNT(DISTINCT rn),COUNT(*) FILTER (WHERE b.id IS NULL),MIN(rn),MAX(rn) FROM %s_ids l LEFT JOIN %s b ON b.id=l.id", table, table)).Scan(&r.TargetCount, &distinctIDs, &distinctRN, &missing, &minRN, &maxRN)
	if err != nil {
		return
	}
	if r.TargetCount != c.Preload || distinctIDs != c.Preload || distinctRN != c.Preload || missing != 0 || minRN != 1 || maxRN != c.Preload {
		err = fmt.Errorf("invalid PostgreSQL target list")
		return
	}
	if err = execSQL("SELECT pg_stat_reset()"); err != nil {
		return
	}
	r.Phases["verify_preload"] = time.Since(start).Seconds()
	ins, read := PostgresScripts(c.Scheme, c.Preload)
	ip, rp := filepath.Join(dir, "insert.sql"), filepath.Join(dir, "read.sql")
	if err = os.WriteFile(ip, []byte(ins), 0600); err != nil {
		return
	}
	if err = os.WriteFile(rp, []byte(read), 0600); err != nil {
		return
	}
	r.MixedStarted = time.Now().UTC()
	err = run("mixed", "-f", ip+"@70", "-f", rp+"@30", "-t", fmt.Sprint(c.Operations), "--random-seed="+fmt.Sprint(c.Seed), "-l", "--log-prefix="+filepath.Join(dir, "mixed_log"))
	r.MixedEnded = time.Now().UTC()
	r.Phases["mixed"] = r.MixedEnded.Sub(r.MixedStarted).Seconds()
	// Fatal gset errors are not transaction-log rows. Preserve diagnostics, never accept a partial run.
	if err != nil {
		r.Errors["pgbench_abort"]++
	}
	start = time.Now()
	files, globErr := filepath.Glob(filepath.Join(dir, "mixed_log.*"))
	if globErr != nil && err == nil {
		err = globErr
	}
	if len(files) != 1 && err == nil {
		err = fmt.Errorf("expected exactly one pgbench log, got %d", len(files))
	}
	if len(files) == 1 {
		data, e := os.ReadFile(files[0])
		if e == nil {
			e = ParsePGLog(data, r)
		}
		if e != nil && err == nil {
			err = e
		}
	}
	r.Throughput = float64(r.Completed) / r.Phases["mixed"]
	var countErr error
	r.EndCount, r.EndRange, countErr = count()
	if err == nil {
		err = countErr
	}
	if c.Scheme == "sequential" && r.Inserts > 0 {
		r.InsertRange = Range{Min: r.PreloadRange.Max + 1, Max: r.EndRange.Max}
	}
	r.Phases["verify_end"] = time.Since(start).Seconds()
	return
}
