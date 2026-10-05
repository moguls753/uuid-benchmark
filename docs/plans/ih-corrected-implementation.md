# Corrected IH implementation (opt-in)

Branch: `fix/ih-corrected-rerun`, based on `6cb737bff35eb1b1d509b20cd4b8900f93c51c06`.
Protocol authority: [ih-rerun.md](ih-rerun.md). Implementation and independent review are complete. The separately authorized small smoke test passed; no full-size pilot or main series has been started.

## Isolation from historical paths

The existing benchmark CLI, mixed runners, historical CSV schema, Docker Compose files and result files remain unchanged. Instead of extending their unsafe fixed-container lifecycle, the new `scripts/ih_campaign.py` launcher starts **only** the corrected IH path in `cmd/workload/main.go` (`--op=ih-corrected`). PostgreSQL still executes pgbench, locally inside its database container; it does not use a Go SQL workload loop. `internal/ih/` contains protocol validation, counters and PostgreSQL execution/parsing. Both existing single-file build commands continue to work.

The legacy `-scenario=mixed-insert-heavy` command is **not corrected** by this change. Never use it to generate this campaign.

## Workload

* Explicit single client, batch-100 preload, random 70/30 insert/read, no updates.
* MySQL/MongoDB/Cassandra retain the existing preload functions and Go key generator. Mixed phase uses a freshly randomized fixed 1024-byte payload (as the separate historical process did); PostgreSQL retains its 1024 `A` bytes and existing generators. Payload compression characteristics are intentionally not harmonized.
* Existing schemas, including `created_at` in SQL engines and Cassandra `bucket=1` / STCS. No table drop: an existing schema is an error.
* Complete, unique fixed preload target list, sampled with replacement. Mongo/MySQL scanners fail on decode/scan/cursor errors. No inserted IDs enter the list. Sequential generators initialize from the verified maximum.
* Independently check final cardinality against counted successful inserts; Cassandra acknowledgements alone are not evidence of new rows. Its final IDs are streamed/paged, rather than relying on a large `COUNT(*)`.
* No extra warmup read pass. Target acquisition/counts/verification are outside MixedStarted/MixedEnded. These checks affect cache state equally across schemes; this is a deliberate corrected protocol, not historical equivalence.
* Go operation selection uses PCG with recorded seed. PostgreSQL uses its own seeded pgbench RNG, weighted `insert.sql@70` and `read.sql@30`. Seeds reproduce choices within the implementation, not UUID entropy, cross-engine traces or database scan ordering.
* pgbench full transaction logs distinguish the two scripts, providing real success counts. Both use `\gset`, which requires exactly one result row; read queries return the full record, and an absent target aborts rather than counting an empty successful SQL query. See [PostgreSQL 18 pgbench](https://www.postgresql.org/docs/18/pgbench.html). No sampling or retry. Raw pgbench scripts, output and logs are retained separately from preload output.
* Primary throughput is completed mixed operations / mixed wallclock. Go timing covers the synchronous mixed loop; PostgreSQL covers pgbench invocation, including process/connection overhead. Percentiles cover successful/attempted Go operations and successful pgbench scripts, respectively; only error-free complete runs are accepted. These instrumented rates are **not directly interchangeable with historical throughput**.
* Storage/cache/I/O metrics are not collected by this narrowly scoped IH rerun path. They must not be filled from historical measurements or inferred. The exported evidence is the corrected throughput/latency and validity protocol requested by the rerun plan.

## Launcher and safety

Without `--execute`, the launcher prints the entire seeded schedule and exits: **no Docker calls, files, builds or measurements**.

```bash
# Safe planning only, small smoke scope (not a scientific pilot):
python3 scripts/ih_campaign.py --mode=smoke --seed=20260401 \
  --engines postgres mysql mongodb cassandra --preload=100 --ops=200

# Safe planning only, eight FULL-SIZE pilots then 125 main runs:
python3 scripts/ih_campaign.py --mode=full --seed=20260401
```

Execution requires a separate user start authorization, plus `--execute --host-ready`. `--host-ready` explicitly confirms mains power, inhibited suspend, quiet host and no competing benchmark. Use an appropriate OS sleep inhibitor when starting; the script does not stop foreign services. Runtime additionally checks available disk space, reported external power, image architecture, exact container image ID, 4 CPU / 8 GiB enforcement. Default overall timeout: 12 hours (including builds/preparation); configurable before starting. `smoke` permits smaller counts and a subset of engines. `pilot` and `full` enforce all four engines, 100,000 preload, 200,000 operations. Preload must be a positive multiple of 100.

Each invocation creates a **new** `results/ih-corrected-*/` directory. Existing directories are refused. Source snapshot includes tracked and untracked Go source and the launcher, Docker inputs, module files and protocol docs (not unrelated CSVs/binaries or `.env`). Binaries are built from that snapshot, not a later working tree. Manifest includes source file hashes, base revision/diff, complete schedule, seeds, host facts, pinned local image IDs/digests and protocol description; there is no `SHA256SUMS` file. Driver/tool versions come from Go buildinfo and version commands. Engine versions/settings are recorded before and after the workload, including PostgreSQL extension versions after setup.

Compose configuration is derived from the checked-in engine configuration but uses a unique per-run project/container and volumes, no published host ports, local image ID and `--pull never --no-build`. Runtime credentials are ephemeral environment values, redacted from persisted command output, never included in the recorded argument list. Successful cleanup rechecks the campaign label; it never calls the old `container.Start/Stop`, fixed-name cleanup or `--remove-orphans`.

Errors, timeout or interruption stop the owned container but preserve its data/volumes and evidence. Stop outcome is inspected again and recorded in `stop-status.json`; if stopped state cannot be confirmed, the launcher raises **STOP UNCONFIRMED** with the exact container identity for manual intervention. No automatic retry or resume. A failure during main requires a new campaign, not selective replacement. On success each run has an `accepted.json`; failures leave partial exports explicitly named `partial-runs.csv`, never a completed n=5 summary.

Before successful cleanup, every run's result JSON/CSV, versions, container configuration/log and (for PostgreSQL) complete pgbench archive must be exported and read back. PostgreSQL copying is mandatory; archived scripts, log counts, percentiles and pgbench stdout must reconcile with the result. An `evidence.json` inventory records file hashes before cleanup. Failed copy/verification takes the stop-and-preserve path, never `down --volumes` or acceptance.

Full mode runs **all eight pilots first**. The explicit pilot gate reopens all acceptance exports, revalidates the evidence inventory, requested and actual counters, actual reads, cardinalities, finite phase durations and timestamp order, throughput reconciliation, and complete lifecycle wallclock. It writes and reads back `pilot-runs.csv` **before** any main run. The predeclared budget rule is **2 × slowest complete pilot wallclock of each engine × its planned main-run count, summed across engines, plus 600 seconds reserve**. This must fit the remaining overall deadline. It is conservative planning, not a runtime guarantee (non-piloted schemes may be slower). Insufficient budget writes `pilot-gate.json` with `passed:false` and stops. Missing/inconsistent evidence or failed exports never produce a passed gate. `passed:true` is written only after every check succeeds. The main sequence is five independently shuffled scheme blocks per engine (35 MongoDB + 90 other runs). Pilot/smoke are never included in the five-run summary. Main summaries report all five raw throughputs, median and 100 × median / new Sequential median, without significance decisions or exclusions. No automatic paper integration.

## Offline validation

```bash
go test ./...
go test -race ./internal/ih ./cmd/workload
go vet ./...
python3 -m unittest discover -s scripts -p 'test_ih_campaign.py'
# Use temporary output paths; do not overwrite existing local binaries.
go build -o /tmp/ih-benchmark-check cmd/benchmark/main.go
CGO_ENABLED=0 go build -o /tmp/ih-workload-check cmd/workload/main.go
```

Unit tests exercise counter continuation, immutable target selection, injected duplicates/upserts/read misses, log parsing, script generation, JSON/CSV roundtrip, schedule/block completeness, baseline isolation and cleanup ownership. Mocked lifecycle tests inject PostgreSQL copy failure and failed container stop: no destructive cleanup, acceptance or next run is allowed. Pilot-gate tests reject missing evidence/timing, changed archives, failed export, duplicate pilots and inadequate remaining budget. They do **not** prove actual PostgreSQL CLI behavior, image initialization, Mongo ID decoding or database-driver behavior. The separately authorized small smoke run below exercised those integration boundaries. Do not treat offline tests or smoke results as satisfying the pilot gate.

## Live smoke validation

Campaign: `ih-corrected-smoke-20261005T222601Z` (local artifacts under `results/`, not part of the code commit).

* All four engines, Sequential and UUIDv4: **8/8 valid runs**.
* Each run: 100 verified preload rows, 100 unique fixed targets, 200 completed operations; zero errors and zero read misses.
* End cardinalities match preload plus actual successful inserts. Sequential mixed inserts start at 101 in every engine.
* PostgreSQL scripts, transaction logs, actual counters, latency percentiles and result exports were archived and reconciled successfully.
* Full per-run lifecycle durations were recorded (roughly 3–66 seconds); campaign completed in about 2 minutes 46 seconds, including preparation. Suspend was inhibited; unrelated containers were not stopped. No campaign containers remained after cleanup.
* This validates the small Sequential/UUIDv4 integration path only, not full-scale performance, every generator or the full-size pilot gate. No pilot/main start is implied.

Offline validation also passed: `go test ./...`, `go vet ./...`, targeted race tests, both single-file builds and 11 Python tests. Independent review initially found three lifecycle/gate issues; all were fixed and the follow-up review found no remaining issues.
