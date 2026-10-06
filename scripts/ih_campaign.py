#!/usr/bin/env python3
"""Opt-in corrected IH launcher. Default: print plan only, no Docker/build/write.
Execution requires --execute --host-ready. Never resumes or overwrites a campaign.
"""
import argparse
import csv
import hashlib
import json
import math
import re
import os
from pathlib import Path
import platform
import random
import secrets
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
ENGINES = ["postgres", "mysql", "mongodb", "cassandra"]
SCHEMES = ["sequential", "uuidv1", "uuidv7", "ulid", "ulid_monotonic", "uuidv4"]
IMAGES = {"postgres": "uuid-benchmark-postgres:18", "mysql": "mysql:8-debian", "mongodb": "mongo:8", "cassandra": "cassandra:5"}
COMPOSE = {e: f"docker/docker-compose.{'mongo' if e == 'mongodb' else e}.yml" for e in ENGINES}
DATA = {"postgres": "/var/lib/postgresql", "mysql": "/var/lib/mysql", "mongodb": "/data/db", "cassandra": "/var/lib/cassandra"}
PROTOCOL = "ih-fixed-preload-v1"
# Fixed before observing any results: conservative planning rule, not a guarantee.
BUDGET_MULTIPLIER = 2.0
BUDGET_RESERVE_SECONDS = 600


def utc():
    return datetime.now(timezone.utc).isoformat()


def schedule(mode, seed, engines, preload=100000, ops=200000, *, skip_pilot=False):
    if skip_pilot and mode != "full":
        raise ValueError("--skip-pilot requires full mode")
    rng = random.Random(seed)
    runs = []
    def add(stage, engine, block, scheme):
        runs.append(dict(run_id=f"{stage}-{engine}-b{block}-{scheme}", stage=stage,
                         engine=engine, block=block, scheme=scheme,
                         seed=rng.randrange(1, 2**63), requested_preload=preload, requested_ops=ops))
    # All eight pilots must pass before any main run. Pilot data never enters n=5.
    for engine in engines:
        for scheme in ("sequential", "uuidv4"):
            add("smoke" if mode == "smoke" else "pilot", engine, 0, scheme)
    if mode == "full":
        for engine in engines:
            for block in range(1, 6):
                schemes = SCHEMES + (["objectid"] if engine == "mongodb" else [])
                rng.shuffle(schemes)
                for scheme in schemes:
                    add("main", engine, block, scheme)
    # Generate then filter: skipping pilots must not change the main order/seeds.
    return [r for r in runs if r["stage"] != "pilot"] if skip_pilot else runs


def positive_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("missing/nonfinite/nonpositive duration")
    return value


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.year < 2000:
        raise ValueError("invalid/missing timezone or timestamp")
    return parsed


def validate_timing(r, *, wall=False):
    start, end = timestamp(r["started"]), timestamp(r["ended"])
    mixed_start, mixed_end = timestamp(r["mixed_started"]), timestamp(r["mixed_ended"])
    if not start <= mixed_start < mixed_end <= end:
        raise ValueError("invalid phase timestamp order")
    phases = r["phase_seconds"]
    for phase in ("preload", "verify_preload", "mixed", "verify_end", "total"):
        positive_seconds(phases[phase])
    if not math.isclose(phases["mixed"], (mixed_end-mixed_start).total_seconds(), abs_tol=0.001):
        raise ValueError("mixed timestamps/duration mismatch")
    if not math.isclose(phases["total"], (end-start).total_seconds(), abs_tol=0.001):
        raise ValueError("total timestamps/duration mismatch")
    if sum(phases[k] for k in ("preload", "verify_preload", "mixed", "verify_end")) > phases["total"] + 0.001:
        raise ValueError("phase durations exceed workload total")
    if not math.isclose(r["throughput"], r["completed_ops"] / phases["mixed"], rel_tol=1e-6):
        raise ValueError("throughput does not reconcile with actual operations/duration")
    if wall:
        seconds = positive_seconds(r["wall_seconds"])
        wall_start, wall_end = timestamp(r["wall_started"]), timestamp(r["wall_ended"])
        if not wall_start <= start < end <= wall_end or seconds < phases["total"]:
            raise ValueError("wallclock does not cover workload/lifecycle")
        # Wall duration uses monotonic time. Fail closed on a material realtime jump.
        if not math.isclose(seconds, (wall_end-wall_start).total_seconds(), abs_tol=0.05):
            raise ValueError("wall timestamps/monotonic duration mismatch")


def validate_result(r, spec):
    for key in ("engine", "scheme", "seed", "requested_preload", "requested_ops"):
        if r.get(key) != spec[key]:
            raise ValueError(f"result/spec mismatch: {key}")
    if r.get("protocol") != PROTOCOL or r.get("valid") is not True or r.get("failure"):
        raise ValueError("invalid protocol/run")
    for key in ("preload_count", "unique_read_targets", "end_count", "insert_successes", "read_successes", "insert_attempts", "read_attempts", "completed_ops", "read_misses", "latency_p50_us", "latency_p95_us", "latency_p99_us"):
        if type(r.get(key)) is not int or r[key] < 0:
            raise ValueError("missing/invalid integer counter: " + key)
    if not r["latency_p50_us"] <= r["latency_p95_us"] <= r["latency_p99_us"]:
        raise ValueError("inconsistent latency percentiles")
    n, ops = spec["requested_preload"], spec["requested_ops"]
    if r.get("preload_count") != n or r.get("unique_read_targets") != n:
        raise ValueError("preload/target mismatch")
    ins, reads = r["insert_successes"], r["read_successes"]
    if min(ins, reads) < 0 or r["insert_attempts"] != ins or r["read_attempts"] != reads:
        raise ValueError("attempt/success mismatch")
    if ins + reads != ops or r["completed_ops"] != ops or r["end_count"] != n + ins:
        raise ValueError("operation/cardinality mismatch")
    if r["errors"] or r["read_misses"] or not (0 < r["throughput"] < float("inf")):
        raise ValueError("error, miss or invalid throughput")
    if spec["scheme"] == "sequential":
        if r["preload_sequential_range"] != dict(min=1, max=n) or r["end_sequential_range"] != dict(min=1, max=n+ins):
            raise ValueError("invalid sequential range")
        if ins and r["insert_sequential_range"] != dict(min=n+1, max=n+ins):
            raise ValueError("sequential overlap/gap")
    validate_timing(r)


def write_json(path, obj):
    # Every final artifact is write-once. Status snapshots use distinct filenames.
    with path.open("x") as f:
        json.dump(obj, f, indent=2, allow_nan=False)
        f.write("\n")


def export_csv(path, rows):
    # Nested evidence is JSON in CSV cells (lossless); no historical CSV schema reuse.
    flat = [{k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
             for k, v in row.items()} for row in rows]
    columns = sorted({k for row in flat for k in row})
    with path.open("x", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(flat)


def verify_csv(path, rows):
    with path.open(newline="") as f:
        actual = list(csv.DictReader(f))
    expected = [{k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else str(v)
                 for k, v in row.items()} for row in rows]
    if actual != expected:
        raise ValueError("CSV roundtrip mismatch: " + str(path))


def verify_pg_archive(directory, r):
    required = ("preload.sql", "insert.sql", "read.sql", "preload.stdout", "preload.stderr", "mixed.stdout", "mixed.stderr")
    for name in required:
        path = directory / name
        if not path.is_file() or path.is_symlink() or (not name.endswith("stderr") and path.stat().st_size == 0):
            raise ValueError("missing PostgreSQL evidence: " + name)
    table = "bench_" + r["scheme"]
    generators = {"uuidv1": "uuid_generate_v1()", "uuidv4": "gen_random_uuid()", "uuidv7": "uuidv7()", "ulid": "gen_ulid()", "ulid_monotonic": "gen_monotonic_ulid()"}
    data = "decode(repeat('41', 1024), 'hex')"
    if r["scheme"] == "sequential":
        insert = f"INSERT INTO {table} (data) VALUES ({data}) RETURNING id \\gset inserted_\n"
    else:
        insert = f"INSERT INTO {table} (id, data) VALUES ({generators[r['scheme']]}, {data}) RETURNING id \\gset inserted_\n"
    read = f"\\set rn random(1, {r['requested_preload']})\nSELECT * FROM {table} WHERE id = (SELECT id FROM {table}_ids WHERE rn = :rn) \\gset read_\n"
    if (directory / "insert.sql").read_text() != insert or (directory / "read.sql").read_text() != read:
        raise ValueError("archived pgbench scripts do not implement the verified read/insert protocol")
    logs = list(directory.glob("mixed_log.*"))
    if len(logs) != 1 or not logs[0].is_file() or logs[0].is_symlink():
        raise ValueError("expected one complete pgbench transaction log")
    counts, seen, latencies = [0, 0], set(), []
    with logs[0].open() as f:
        for line in f:
            fields = line.split()
            if len(fields) != 6 or any(not value.isdigit() for value in fields):
                raise ValueError("failed/malformed PostgreSQL transaction")
            client, txn, latency, script, epoch, micros = map(int, fields)
            if client != 0 or script not in (0, 1) or txn in seen:
                raise ValueError("foreign/duplicate PostgreSQL transaction")
            seen.add(txn)
            counts[script] += 1
            latencies.append(latency)
    if counts != [r["insert_successes"], r["read_successes"]] or len(seen) != r["completed_ops"]:
        raise ValueError("archived PostgreSQL counts do not reconcile")
    latencies.sort()
    for percentile in (50, 95, 99):
        if not latencies or latencies[len(latencies)*percentile//100] != r[f"latency_p{percentile}_us"]:
            raise ValueError("archived PostgreSQL percentiles do not reconcile")
    output = (directory / "mixed.stdout").read_text()
    counts_report = re.search(r"^number of transactions actually processed: (\d+)/(\d+)$", output, re.M)
    if not counts_report or tuple(map(int, counts_report.groups())) != (r["completed_ops"], r["requested_ops"]):
        raise ValueError("pgbench stdout/log count mismatch")


def evidence_files(run_dir, spec):
    paths = [run_dir / name for name in ("spec.json", "compose.json", "container.json", "result.json", "result.csv", "versions-before.txt", "versions-after.txt", "container.log")]
    if spec["engine"] == "postgres":
        paths += sorted((run_dir / "pgbench").iterdir())
    return paths


def verify_run_evidence(run_dir, spec, r):
    validate_result(r, spec)
    if json.loads((run_dir / "spec.json").read_text()) != spec:
        raise ValueError("archived specification mismatch")
    raw = json.loads((run_dir / "result.json").read_text())
    if any(r.get(key) != value for key, value in raw.items()):
        raise ValueError("accepted/raw result mismatch")
    validate_result(raw, spec)
    verify_csv(run_dir / "result.csv", [raw])
    for name in ("versions-before.txt", "versions-after.txt"):
        if not (run_dir / name).read_text().strip():
            raise ValueError("missing version evidence")
    if spec["engine"] == "postgres":
        verify_pg_archive(run_dir / "pgbench", r)
    records = []
    for path in evidence_files(run_dir, spec):
        if not path.is_file() or path.is_symlink():
            raise ValueError("missing/unsafe evidence file: " + str(path))
        records.append(dict(path=str(path.relative_to(run_dir)), sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    return records


def evaluate_pilot(campaign, accepted, runs, deadline):
    expected = [r for r in runs if r["stage"] == "pilot"]
    pilots = [r for r in accepted if r["stage"] == "pilot"]
    pairs = {(e, s) for e in ENGINES for s in ("sequential", "uuidv4")}
    if len(expected) != 8 or len(pilots) != 8 or {(r["engine"], r["scheme"]) for r in expected} != pairs:
        raise ValueError("full eight-run pilot gate not met")
    if len({r["run_id"] for r in pilots}) != 8 or {r["run_id"] for r in expected} != {r["run_id"] for r in pilots}:
        raise ValueError("pilot configurations/identities mismatch")
    by_id = {r["run_id"]: r for r in pilots}
    for spec in expected:
        r = by_id[spec["run_id"]]
        if spec["requested_preload"] != 100000 or spec["requested_ops"] != 200000 or spec["block"] != 0:
            raise ValueError("smoke-size pilot cannot open main gate")
        if any(r.get(key) != spec[key] for key in ("run_id", "stage", "block")):
            raise ValueError("pilot block/stage mismatch")
        directory = campaign / spec["run_id"]
        validate_timing(r, wall=True)
        if json.loads((directory / "accepted.json").read_text()) != r:
            raise ValueError("missing/inconsistent acceptance export")
        records = verify_run_evidence(directory, spec, r)
        if json.loads((directory / "evidence.json").read_text()) != records:
            raise ValueError("evidence changed since acceptance")
        # A gate is not proof of reads if a malformed RNG executed none.
        if r["read_successes"] <= 0 or r["insert_successes"] <= 0:
            raise ValueError("pilot did not exercise both operation paths")
    export_csv(campaign / "pilot-runs.csv", pilots)
    verify_csv(campaign / "pilot-runs.csv", pilots)
    # Estimate all main runs, including schemes not piloted, at twice the slowest
    # complete pilot wallclock of that engine, plus ten minutes export reserve.
    slowest = {e: max(r["wall_seconds"] for r in pilots if r["engine"] == e) for e in ENGINES}
    main = [r for r in runs if r["stage"] == "main"]
    estimated = BUDGET_MULTIPLIER * sum(slowest[r["engine"]] for r in main) + BUDGET_RESERVE_SECONDS
    remaining = deadline - time.monotonic()
    assessment = dict(passed=remaining >= estimated, evaluated=utc(), pilot_ids=[r["run_id"] for r in pilots],
                      pilot_wall_seconds=sum(r["wall_seconds"] for r in pilots), remaining_seconds=remaining,
                      estimated_main_seconds=estimated, budget_multiplier=BUDGET_MULTIPLIER,
                      reserve_seconds=BUDGET_RESERVE_SECONDS, slowest_pilot_seconds=slowest)
    write_json(campaign / "pilot-gate.json", assessment)
    if not assessment["passed"]:
        raise ValueError("pilot budget gate failed; no main run started")
    return assessment


def main_start_gate(campaign, accepted, runs, deadline, *, skip_pilot=False):
    if not skip_pilot:
        return evaluate_pilot(campaign, accepted, runs, deadline)
    if accepted or any(r["stage"] != "main" for r in runs):
        raise ValueError("pilot bypass must start a fresh main-only campaign")
    # Explicit operator override, never disguise a bypass as a passed gate.
    assessment = dict(passed=False, skipped=True, reason="operator requested --skip-pilot",
                      budget_assessed=False, per_run_validation_required=True, recorded=utc())
    write_json(campaign / "pilot-gate.json", assessment)
    return assessment


def summary(rows):
    main = [r for r in rows if r["stage"] == "main"]
    groups = {}
    for r in main:
        groups.setdefault((r["engine"], r["scheme"]), []).append(r)
    out = []
    for (engine, scheme), values in groups.items():
        if sorted(r["block"] for r in values) != [1, 2, 3, 4, 5]:
            raise ValueError("refusing incomplete n=5 summary")
        median = statistics.median(r["throughput"] for r in values)
        baseline = statistics.median(r["throughput"] for r in groups[(engine, "sequential")])
        out.append(dict(engine=engine, scheme=scheme, n=5, median=median,
                        percent_of_new_sequential=100 * median / baseline,
                        raw_throughput=[r["throughput"] for r in values]))
    return out


class Commands:
    def __init__(self, directory, timeout):
        self.directory, self.timeout = directory, timeout
        self.deadline = time.monotonic() + timeout
        self.number = 0
        self.password = secrets.token_hex(24)
        self.env = dict(os.environ, IH_DB_PASSWORD=self.password)

    def run(self, args, *, cwd=None, timeout=None, env=None, check=True, preserve=False):
        self.number += 1
        # Stop/diagnostic commands may still run after the campaign deadline.
        remaining = self.deadline - time.monotonic()
        if remaining <= 0 and not preserve:
            raise TimeoutError("campaign wallclock limit reached")
        timeout = min(timeout or self.timeout, remaining) if not preserve else (timeout or 30)
        prefix = self.directory / f"command-{self.number:05d}"
        write_json(prefix.with_suffix(".json"), dict(args=args, cwd=str(cwd or ROOT), started=utc(), timeout=timeout))
        try:
            p = subprocess.run(args, cwd=cwd or ROOT, env=env or self.env, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            prefix.with_suffix(".timeout").write_text("Command timed out; no automatic retry.\n")
            for name, data in (("stdout", e.stdout), ("stderr", e.stderr)):
                text = data.decode(errors="replace") if isinstance(data, bytes) else data or ""
                prefix.with_suffix("." + name).write_text(text.replace(self.password, "<redacted>"))
            raise
        # Do not persist credentials, including credentials echoed by an error.
        for name in ("stdout", "stderr"):
            prefix.with_suffix("." + name).write_text(getattr(p, name).replace(self.password, "<redacted>"))
        if check and p.returncode:
            raise RuntimeError(f"command {self.number} failed (see logs): {args[0]}")
        return p


def source_snapshot(dest):
    paths = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT).decode().split("\0")
    selected = []
    for name in sorted(set(paths)):
        p = Path(name)
        if not name or p.is_absolute() or ".." in p.parts:
            continue
        allowed = name in ("go.mod", "go.sum") or (p.parts[0] in ("cmd", "internal") and p.suffix == ".go") or (p.parts[0] == "docker" and (p.suffix in (".yml", ".sql") or p.name.startswith("Dockerfile"))) or name in ("scripts/ih_campaign.py", "scripts/ih_repeat.py", "docs/plans/ih-rerun.md", "docs/plans/ih-corrected-implementation.md")
        if not allowed or not (ROOT / p).is_file() or (ROOT / p).is_symlink():
            continue
        target = dest / p
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / p, target)
        selected.append(dict(path=name, sha256=hashlib.sha256(target.read_bytes()).hexdigest()))
    return selected


def compose_config(engine, image, project, snapshot, resolved):
    service = dict(next(iter(resolved["services"].values())))
    for key in ("build", "ports", "networks", "depends_on", "env_file"):
        service.pop(key, None)
    service.update(image=image, container_name=project, pull_policy="never",
                   labels={"uuid-ih.campaign": project},
                   volumes=[dict(type="volume", source="data", target=DATA[engine])])
    env = service.get("environment", {}).copy()
    # Never inherit operator credentials or database names from their environment.
    if engine == "postgres":
        env.update(POSTGRES_USER="benchmark", POSTGRES_DB="uuid_benchmark", POSTGRES_PASSWORD="${IH_DB_PASSWORD:?required}")
    elif engine == "mysql":
        env.update(MYSQL_USER="benchmark", MYSQL_DATABASE="uuid_benchmark", MYSQL_PASSWORD="${IH_DB_PASSWORD:?required}", MYSQL_ROOT_PASSWORD="${IH_DB_PASSWORD:?required}")
        service["volumes"].append(dict(type="bind", source=str(snapshot / "docker/mysql-init"), target="/docker-entrypoint-initdb.d", read_only=True))
    elif engine == "mongodb":
        env.update(MONGO_INITDB_ROOT_USERNAME="benchmark", MONGO_INITDB_DATABASE="uuid_benchmark", MONGO_INITDB_ROOT_PASSWORD="${IH_DB_PASSWORD:?required}")
    service["environment"] = env
    return {"services": {engine: service}, "volumes": {"data": {"labels": {"uuid-ih.campaign": project}}}}


def connection(engine, password):
    return {"postgres": f"host=127.0.0.1 user=benchmark password={password} dbname=uuid_benchmark sslmode=disable connect_timeout=5",
            "mysql": f"benchmark:{password}@tcp(127.0.0.1:3306)/uuid_benchmark?timeout=5s&readTimeout=30s&writeTimeout=30s",
            "mongodb": f"mongodb://benchmark:{password}@127.0.0.1:27017/uuid_benchmark?authSource=admin&serverSelectionTimeoutMS=5000",
            "cassandra": "127.0.0.1"}[engine]


def verify_owned(inspect, project):
    if inspect["Config"].get("Labels", {}).get("uuid-ih.campaign") != project or inspect["Name"] != "/" + project:
        raise ValueError("refusing to touch container not owned by this run")


def inspect_safe(info):
    # Config.Env contains passwords: only persist an explicit allowlist.
    return {k: info[k] for k in ("Id", "Name", "Image", "HostConfig", "Mounts", "State")}


def versions(engine):
    if engine == "postgres":
        return ["psql", "-U", "benchmark", "-d", "uuid_benchmark", "-Atc", "SELECT version(); SELECT extname,extversion FROM pg_extension; SHOW checkpoint_timeout; SHOW max_wal_size; SHOW shared_preload_libraries;"]
    if engine == "mysql":
        return ["sh", "-c", 'MYSQL_PWD="$IH_DB_PASSWORD" mysql -u benchmark uuid_benchmark -N -e "SELECT VERSION(); SHOW VARIABLES WHERE Variable_name IN (\'innodb_buffer_pool_size\',\'innodb_flush_log_at_trx_commit\',\'innodb_flush_method\',\'innodb_log_file_size\',\'performance_schema\');"']
    if engine == "mongodb":
        return ["mongosh", "--quiet", "--eval", 'const c = new Mongo("mongodb://benchmark:"+process.env.IH_DB_PASSWORD+"@127.0.0.1:27017/?authSource=admin"); printjson(c.getDB("admin").runCommand({buildInfo:1}));']
    return ["cqlsh", "-e", "SELECT release_version FROM system.local;"]


def preserve_failed_run(commands, project, run_dir, engine):
    status = dict(container=project, stopped_confirmed=False)
    try:
        p = commands.run(["docker", "inspect", project], check=False, preserve=True, timeout=30)
        if p.returncode != 0:
            raise RuntimeError("container inspection failed; stopped state unknown")
        info = json.loads(p.stdout)[0]
        verify_owned(info, project)
        stopped = commands.run(["docker", "stop", "-t", "10", project], check=False, preserve=True, timeout=30)
        status["stop_returncode"] = stopped.returncode
        after = commands.run(["docker", "inspect", project], check=False, preserve=True, timeout=30)
        if after.returncode != 0:
            raise RuntimeError("post-stop inspection failed")
        info = json.loads(after.stdout)[0]
        verify_owned(info, project)
        status["state"] = info["State"]
        status["stopped_confirmed"] = info["State"].get("Running") is False and info["State"].get("Restarting") is False
    except Exception as exc:
        status["stop_error"] = str(exc).replace(commands.password, "<redacted>")
    # Evidence collection cannot hide an unresolved stop; persist that status first.
    write_json(run_dir / "stop-status.json", status)
    for args in ([["docker", "logs", project]] +
                 ([["docker", "cp", project+":/tmp/ih", str(run_dir / "pgbench-failure")]] if engine == "postgres" else [])):
        try:
            commands.run(args, check=False, preserve=True, timeout=30)
        except Exception:
            pass  # command-level timeout/failure evidence is already recorded
    return status


def run_one(spec, campaign, snapshot, image, resolved, commands, binary, ordinal, token):
    run_dir = campaign / spec["run_id"]
    run_dir.mkdir()
    project = f"ih-{token}-{ordinal:03d}"
    config = compose_config(spec["engine"], image, project, snapshot, resolved)
    compose_file = run_dir / "compose.json"
    write_json(compose_file, config)
    write_json(run_dir / "spec.json", spec)
    compose = ["docker", "compose", "--env-file", str(campaign / "empty.env"), "-p", project, "-f", str(compose_file)]
    exec_env = dict(commands.env, IH_CONNECTION_STRING=connection(spec["engine"], commands.password))
    execute = ["docker", "exec", "-e", "IH_CONNECTION_STRING", project, "/tmp/workload"]
    started = utc()
    begin = time.monotonic()
    try:
        # No initial down, no pull/build, no published ports, no shared project/volumes.
        commands.run(compose + ["up", "-d", "--pull", "never", "--no-build"], timeout=120)
        info = json.loads(commands.run(["docker", "inspect", project]).stdout)[0]
        verify_owned(info, project)
        write_json(run_dir / "container.json", inspect_safe(info))
        if info["Image"] != image or info["HostConfig"]["Memory"] != 8 * 1024**3 or info["HostConfig"]["NanoCpus"] != 4 * 10**9:
            raise ValueError("image/resource limits not applied")
        commands.run(["docker", "cp", str(binary), project + ":/tmp/workload"])
        deadline = time.monotonic() + 180
        while True:
            p = commands.run(execute + ["--op=ih-ready", "--db-type=" + spec["engine"]], env=exec_env, timeout=15, check=False)
            if p.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("readiness deadline; no container restart/retry")
            time.sleep(2)
        before = commands.run(["docker", "exec", "-e", "IH_DB_PASSWORD", project] + versions(spec["engine"]), timeout=60)
        (run_dir / "versions-before.txt").write_text(before.stdout.replace(commands.password, "<redacted>"))
        result = commands.run(execute + ["--op=ih-corrected", "--db-type="+spec["engine"],
                     "--key-type="+spec["scheme"], "--threads=1", "--batch-size=100",
                     "--num-records="+str(spec["requested_preload"]), "--num-ops="+str(spec["requested_ops"]),
                     "--seed="+str(spec["seed"]), "--artifact-dir=/tmp/ih"], env=exec_env, check=False)
        (run_dir / "result.json").write_text(result.stdout.replace(commands.password, "<redacted>"))
        if spec["engine"] == "postgres":
            commands.run(["docker", "cp", project+":/tmp/ih", str(run_dir / "pgbench")])
        if result.returncode:
            raise RuntimeError("corrected workload failed; preserve evidence and stop")
        r = json.loads(result.stdout)
        validate_result(r, spec)
        after = commands.run(["docker", "exec", "-e", "IH_DB_PASSWORD", project] + versions(spec["engine"]), timeout=60)
        (run_dir / "versions-after.txt").write_text(after.stdout.replace(commands.password, "<redacted>"))
        logs = commands.run(["docker", "logs", project])
        (run_dir / "container.log").write_text((logs.stdout+logs.stderr).replace(commands.password, "<redacted>"))
        export_csv(run_dir / "result.csv", [r])
        # Read back and reconcile every required evidence export before any down -v.
        write_json(run_dir / "evidence.json", verify_run_evidence(run_dir, spec, r))
        # Recheck ownership before destructive cleanup; the project has one container.
        info = json.loads(commands.run(["docker", "inspect", project]).stdout)[0]
        verify_owned(info, project)
        commands.run(compose + ["down", "--volumes"], timeout=120)
        r.update({k: spec[k] for k in ("run_id", "stage", "block")})
        if "source_campaign" in spec:
            r.update(source_campaign=spec["source_campaign"], source_run_id=spec["source_run_id"])
        r.update(wall_started=started, wall_ended=utc(), wall_seconds=time.monotonic()-begin)
        validate_timing(r, wall=True)
        write_json(run_dir / "accepted.json", r)
        if json.loads((run_dir / "accepted.json").read_text()) != r:
            raise ValueError("acceptance export readback failed")
        return r
    except BaseException as exc:
        # Killing docker exec alone does not stop the workload in the container.
        status = preserve_failed_run(commands, project, run_dir, spec["engine"])
        write_json(run_dir / "interrupted.json", dict(ended=utc(), wall_seconds=time.monotonic()-begin,
                   container=project, stopped_confirmed=status["stopped_confirmed"],
                   reason="See command logs and stop-status.json. No retry; preserve resources."))
        if not status["stopped_confirmed"]:
            raise RuntimeError(f"STOP UNCONFIRMED: owned container {project} may still be running; manual intervention required") from exc
        raise


def host_info():
    power = {}
    for path in Path("/sys/class/power_supply").glob("*/online"):
        power[str(path)] = path.read_text().strip()
    return dict(platform=platform.platform(), uname=list(platform.uname()), cpu=Path("/proc/cpuinfo").read_text(),
                memory=Path("/proc/meminfo").read_text(), load=os.getloadavg(), power=power,
                free_bytes=shutil.disk_usage(ROOT).free)


def execute_campaign(args, runs):
    campaign = ROOT / "results" / args.campaign
    campaign.mkdir(parents=True, exist_ok=False)
    (campaign / "empty.env").touch()
    snapshot = campaign / "source"
    files = source_snapshot(snapshot)
    commands_dir = campaign / "commands"
    commands_dir.mkdir()
    commands = Commands(commands_dir, args.timeout_hours * 3600)
    accepted = []
    try:
        repeat = getattr(args, "repeat", None)
        if repeat:
            sources = {f["path"]: f["sha256"] for f in files
                       if Path(f["path"]).parts[0] in ("cmd", "internal", "docker") or f["path"] in ("go.mod", "go.sum")}
            if sources != repeat["measurement_sources"]:
                raise ValueError("measurement source/schema/config changed; refuse same-protocol repeat")
        host = host_info()
        if host["free_bytes"] < 20 * 1024**3:
            raise RuntimeError("less than 20 GiB free")
        if host["power"] and "1" not in host["power"].values():
            raise RuntimeError("no online external power supply")
        images, resolved = {}, {}
        for engine in args.engines:
            repeat = getattr(args, "repeat", None)
            image_ref = repeat["images"][engine]["Id"] if repeat else IMAGES[engine]
            image = json.loads(commands.run(["docker", "image", "inspect", image_ref]).stdout)[0]
            images[engine] = {k: image.get(k) for k in ("Id", "RepoDigests", "Architecture", "Os")}
            if image["Architecture"] != platform.machine().replace("x86_64", "amd64").replace("aarch64", "arm64"):
                raise ValueError("image/host architecture mismatch")
            # Resolved config may include env credentials; Commands redacts our secret;
            # use a clean environment so operator overrides cannot leak into logs.
            clean = {k: v for k, v in commands.env.items() if k not in {"POSTGRES_PASSWORD", "POSTGRES_USER", "POSTGRES_DB", "MYSQL_ROOT_PASSWORD", "MYSQL_PASSWORD", "MYSQL_USER", "MYSQL_DATABASE", "MONGO_PASSWORD", "MONGO_USER", "MONGO_DATABASE", "CASSANDRA_CLUSTER_NAME", "CASSANDRA_DC"}}
            resolved[engine] = json.loads(commands.run(["docker", "compose", "--env-file", str(campaign / "empty.env"), "-f", str(snapshot / COMPOSE[engine]), "config", "--format", "json"], env=clean).stdout)
        base = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        diff = subprocess.check_output(["git", "diff", "HEAD", "--", "cmd", "internal", "scripts/ih_campaign.py"], cwd=ROOT)
        (campaign / "source.diff").write_bytes(diff)
        manifest = dict(protocol=PROTOCOL, created=utc(), campaign=args.campaign, mode=args.mode,
                        base_revision=base, files=files, images=images, host=host, schedule=runs,
                        repeat=getattr(args, "repeat", None),
                        skip_pilot=args.skip_pilot,
                        pilot_policy="operator-requested bypass; full-size pilot and budget gate NOT performed" if args.skip_pilot else "required for full mode",
                        order_seed=args.seed, order_rng="Python random.Random/MT19937; stored order is authoritative",
                        python=sys.version, timeout_hours=args.timeout_hours, host_ready_confirmed=True,
                        command=sys.argv, payload="PG: decode(repeat('41',1024),'hex'); others: cryptographic random 1024-byte fixed payload per phase",
                        generators="Existing server-side PG functions / Go keyGenerator; UUID entropy is NOT seeded",
                        schema="Existing schemas; Cassandra bucket=1, STCS; batch-100 preload; single-operation mixed",
                        instrumentation="Go synchronous counters; PG weighted scripts + full transaction log + one-row gset; no retries",
                        timing="Mixed wallclock excludes preload and verification; launcher wallclock includes lifecycle",
                        resources="4 CPUs/8 GiB; Cassandra heap 4G/newgen 1G; local image IDs only",
                        pilot_budget_rule=dict(multiplier=BUDGET_MULTIPLIER, reserve_seconds=BUDGET_RESERVE_SECONDS,
                                               basis="slowest full pilot wallclock per engine × main-run count"),
                        interruption="stop owned container, preserve data; no resume or automatic retries")
        write_json(campaign / "manifest.json", manifest)
        commands.run(["go", "version"])
        commands.run(["docker", "version"])
        commands.run(["docker", "compose", "version"])
        binary = campaign / "workload"
        repeat = getattr(args, "repeat", None)
        if repeat:
            # Reuse the exact archived binaries, not a newly compiled workload.
            for name, digest in repeat["binaries"].items():
                original = Path(repeat["source_directory"]) / name
                target = campaign / name
                shutil.copyfile(original, target)
                if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                    raise ValueError("archived binary changed since repeat selection")
                target.chmod(0o700)
        else:
            build_env = dict(commands.env, CGO_ENABLED="0", GOOS="linux")
            commands.run(["go", "build", "-o", str(binary), "./cmd/workload/main.go"], cwd=snapshot, env=build_env)
            commands.run(["go", "build", "-o", str(campaign / "uuid-benchmark"), "./cmd/benchmark/main.go"], cwd=snapshot, env=build_env)
        commands.run(["go", "version", "-m", str(binary)])
        commands.run(["go", "version", "-m", str(campaign / "uuid-benchmark")])
        token = secrets.token_hex(8)
        for i, spec in enumerate(runs):
            if spec["stage"] == "main" and not any(r["stage"] == "main" for r in accepted):
                main_start_gate(campaign, accepted, runs, commands.deadline, skip_pilot=args.skip_pilot)
            print(f"{i+1}/{len(runs)}: {spec['run_id']}", flush=True)
            accepted.append(run_one(spec, campaign, snapshot, images[spec["engine"]]["Id"], resolved[spec["engine"]], commands, binary, i, token))
        export_csv(campaign / "runs.csv", accepted)
        verify_csv(campaign / "runs.csv", accepted)
        if args.mode == "full":
            export_csv(campaign / "summary.csv", summary(accepted))
        write_json(campaign / "complete.json", dict(ended=utc(), runs=len(accepted), mode=args.mode,
                                                   skip_pilot=args.skip_pilot))
    except BaseException as exc:
        export_csv(campaign / "partial-runs.csv", accepted)
        write_json(campaign / "failed.json", dict(ended=utc(), accepted_runs=len(accepted), error=str(exc).replace(commands.password, "<redacted>")))
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["smoke", "pilot", "full"], default="smoke")
    p.add_argument("--engines", nargs="+", choices=ENGINES, default=ENGINES)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--preload", type=int, default=100000)
    p.add_argument("--ops", type=int, default=200000)
    p.add_argument("--campaign", default="ih-corrected-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    p.add_argument("--timeout-hours", type=float, default=12)
    p.add_argument("--skip-pilot", action="store_true", help="Explicit full-mode protocol deviation: skip pilots and pilot budget gate, retain all per-run checks")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--host-ready", action="store_true", help="Confirm mains power, inhibited suspend, quiet host; no other benchmark")
    args = p.parse_args()
    if args.skip_pilot and args.mode != "full":
        p.error("--skip-pilot is only allowed with --mode=full")
    if args.mode != "smoke" and (args.preload, args.ops, args.engines) != (100000, 200000, ENGINES):
        p.error("pilot/full require all four engines in protocol order, preload=100000 and ops=200000")
    if args.preload <= 0 or args.preload % 100 or args.ops <= 0 or not (0 < args.timeout_hours <= 48) or len(set(args.engines)) != len(args.engines):
        p.error("invalid counts, timeout or duplicate engines")
    if not args.campaign.startswith("ih-corrected-") or not all(c.isalnum() or c in "-_" for c in args.campaign):
        p.error("campaign must be a plain ih-corrected-* directory name")
    runs = schedule(args.mode, args.seed, args.engines, args.preload, args.ops, skip_pilot=args.skip_pilot)
    if args.skip_pilot:
        print("WARNING: explicit pilot/budget-gate bypass; per-run correctness checks remain mandatory.", file=sys.stderr)
    if not args.execute:
        print(json.dumps(dict(mode=args.mode, campaign=args.campaign, skip_pilot=args.skip_pilot, runs=runs), indent=2))
        return
    if not args.host_ready:
        p.error("execution requires --host-ready; review protocol/order before starting")
    execute_campaign(args, runs)


if __name__ == "__main__":
    main()
