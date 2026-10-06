"""Validate the selected ih-fixed-preload-v1 bundle before computing medians.

Reads only the explicit 125-slot selection, never recursively discovers runs.
This checks selected measurement evidence, not omitted provenance or rebuilds.
"""

import csv
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import statistics as st

ENGINES = {"postgres": "Pg", "mysql": "Mysql", "mongodb": "Mongo", "cassandra": "Cass"}
SCHEMES = {"sequential": "Seq", "uuidv1": "VOne", "uuidv7": "VSeven",
           "ulid": "Ulid", "ulid_monotonic": "UlidMono", "uuidv4": "VFour"}
REPEATS = {"main-postgres-b5-ulid_monotonic", "main-postgres-b5-uuidv4", "main-mysql-b1-ulid"}
PROTOCOL = "ih-fixed-preload-v1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def inside(root, relative):
    root = Path(root).resolve()
    p = Path(relative)
    require(not p.is_absolute() and bool(p.parts) and ".." not in p.parts,
            f"Unsafe bundle path: {relative}")
    current = root
    for part in p.parts:
        current /= part
        require(not current.is_symlink(), f"Symlink in bundle path: {relative}")
    require(current.resolve().is_relative_to(root), f"Path escapes bundle: {relative}")
    return current


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def checked_digest(root, relative, expected):
    path = inside(root, relative)
    require(path.is_file() and digest(path) == expected, f"Missing/altered evidence: {path}")
    return path


def csv_rows(path):
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        require(header and len(header) == len(set(header)), f"Invalid CSV header: {path}")
        rows = list(reader)
    require(all(None not in r and all(v is not None for v in r.values()) for r in rows),
            f"CSV row width differs from header: {path}")
    return rows


def flat(row):
    return {k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else str(v)
            for k, v in row.items()}


def near(a, b, message):
    require(math.isfinite(a) and math.isfinite(b) and
            math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-7), message)


def timestamp(value):
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.tzinfo is not None, "Naive timestamp")
    return result


def schemes(engine):
    return dict(SCHEMES, **({"objectid": "ObjectId"} if engine == "mongodb" else {}))


def validate_measurement(r):
    """Validate the raw measurement itself, without launcher-added metadata."""
    required = {
        "engine", "scheme", "seed", "protocol", "valid", "requested_preload", "requested_ops",
        "started", "ended", "mixed_started", "mixed_ended", "phase_seconds",
        "preload_count", "unique_read_targets", "end_count", "preload_sequential_range",
        "insert_sequential_range", "end_sequential_range", "insert_attempts", "insert_successes",
        "read_attempts", "read_successes", "completed_ops", "read_misses", "errors", "throughput",
        "latency_p50_us", "latency_p95_us", "latency_p99_us",
    }
    require(isinstance(r, dict) and required <= r.keys(), "Incomplete measurement-result schema")
    require(r["engine"] in ENGINES and r["scheme"] in schemes(r["engine"]), "Invalid measurement identity")
    require(type(r["seed"]) is int and r["seed"] >= 0, "Invalid seed")
    latencies = [r[f"latency_p{p}_us"] for p in (50, 95, 99)]
    require(all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in latencies)
            and latencies == sorted(latencies), "Invalid latency percentiles")
    require(r["protocol"] == PROTOCOL and r["valid"] is True and not r.get("failure"),
            "Invalid protocol or run status")
    for field in ("requested_preload", "preload_count", "unique_read_targets"):
        require(type(r[field]) is int and r[field] == 100_000, f"Invalid {field}")
    for field in ("requested_ops", "completed_ops"):
        require(type(r[field]) is int and r[field] == 200_000, f"Invalid {field}")
    for field in ("insert_attempts", "insert_successes", "read_attempts", "read_successes", "end_count"):
        require(type(r[field]) is int and r[field] > 0, f"Invalid count: {field}")
    require(r["insert_attempts"] == r["insert_successes"] and
            r["read_attempts"] == r["read_successes"] and
            r["insert_successes"] + r["read_successes"] == 200_000 and
            r["end_count"] == 100_000 + r["insert_successes"] and
            r["errors"] == {} and r["read_misses"] == 0, "Operation accounting mismatch")
    if r["scheme"] == "sequential":
        for field, expected in (("preload_sequential_range", {"min": 1, "max": 100_000}),
                                ("insert_sequential_range", {"min": 100_001, "max": r["end_count"]}),
                                ("end_sequential_range", {"min": 1, "max": r["end_count"]})):
            require(r[field] == expected, f"Invalid {field}")
    phases = r["phase_seconds"]
    require({"mixed", "preload", "total", "verify_end", "verify_preload"} <= phases.keys(),
            "Missing phases")
    require(all(type(v) in (int, float) and math.isfinite(v) and v > 0 for v in phases.values()),
            "Nonpositive/nonfinite phase duration")
    require(type(r["throughput"]) in (int, float) and r["throughput"] > 0, "Invalid throughput")
    near(r["throughput"], 200_000 / phases["mixed"], "Throughput/duration mismatch")
    times = [timestamp(r[k]) for k in ("started", "mixed_started", "mixed_ended", "ended")]
    require(times == sorted(times) and times[1] < times[2], "Invalid timestamp order")
    for phase, start, end in (("mixed", 1, 2), ("total", 0, 3)):
        require(abs(phases[phase] - (times[end] - times[start]).total_seconds()) < 0.00001,
                f"Invalid {phase} timestamps")
    require(sum(v for k, v in phases.items() if k != "total") <= phases["total"] + .001,
            "Phases exceed total")


def validate_record(r):
    """Validate accepted measurement plus the launcher's outer lifecycle."""
    validate_measurement(r)
    times = [timestamp(r[k]) for k in ("wall_started", "started", "ended", "wall_ended")]
    require(times == sorted(times), "Invalid launcher timestamp order")
    require(math.isfinite(r["wall_seconds"]) and r["wall_seconds"] > 0 and
            abs(r["wall_seconds"] - (times[3] - times[0]).total_seconds()) < .05,
            "Invalid wall duration")


def pg_scripts(scheme):
    """Exact scripts in the selected protocol, independent of producer imports."""
    require(scheme in SCHEMES, "Invalid PostgreSQL scheme")
    table = "bench_" + scheme
    payload = "decode(repeat('41', 1024), 'hex')"
    generators = {"uuidv1": "uuid_generate_v1()", "uuidv4": "gen_random_uuid()",
                  "uuidv7": "uuidv7()", "ulid": "gen_ulid()", "ulid_monotonic": "gen_monotonic_ulid()"}
    if scheme == "sequential":
        insert = f"INSERT INTO {table} (data) VALUES ({payload}) RETURNING id \\gset inserted_\n"
    else:
        insert = f"INSERT INTO {table} (id, data) VALUES ({generators[scheme]}, {payload}) RETURNING id \\gset inserted_\n"
    read = f"\\set rn random(1, 100000)\nSELECT * FROM {table} WHERE id = (SELECT id FROM {table}_ids WHERE rn = :rn) \\gset read_\n"
    return insert, read


def validate_pg_protocol(directory, record):
    insert, read = pg_scripts(record["scheme"])
    require(inside(directory, "pgbench/insert.sql").read_text() == insert and
            inside(directory, "pgbench/read.sql").read_text() == read,
            "PostgreSQL scripts differ from the fixed-pool/one-row protocol")
    text = inside(directory, "pgbench/mixed.stdout").read_text()
    # script_no 0/1 in the log must mean insert/read, weighted 70/30.
    mapping = re.findall(r"^SQL script (\d+): ([^\n]+)\n - weight: (\d+) \(targets ([\d.]+)% of total\)$",
                         text, re.M)
    require(mapping == [("1", "/tmp/ih/insert.sql", "70", "70.0"),
                        ("2", "/tmp/ih/read.sql", "30", "30.0")],
            "Invalid PostgreSQL script mapping/weights")


def validate_pg(directory, record, evidence_paths):
    validate_pg_protocol(directory, record)
    logs = [p for p in evidence_paths if p.startswith("pgbench/mixed_log.")]
    require(len(logs) == 1, "Expected one full PostgreSQL transaction log")
    counts = [0, 0]
    lower = timestamp(record["mixed_started"]).timestamp()
    upper = timestamp(record["mixed_ended"]).timestamp()
    with inside(directory, logs[0]).open() as handle:
        for i, line in enumerate(handle, 1):
            fields = line.split()
            require(len(fields) == 6 and all(v.isdigit() for v in fields), "Invalid PG log row")
            client, txn, latency, script, epoch, micros = map(int, fields)
            require(client == 0 and txn == i and script in (0, 1) and 0 <= micros < 1_000_000,
                    "Invalid PG transaction identity")
            require(lower <= epoch + micros / 1e6 <= upper, "PG transaction outside mixed window")
            counts[script] += 1
    require(counts == [record["insert_successes"], record["read_successes"]], "PG log/count mismatch")
    text = inside(directory, "pgbench/mixed.stdout").read_text()
    for expected in ("number of transactions actually processed: 200000/200000",
                     "number of failed transactions: 0 (0.000%)", "number of clients: 1",
                     "number of threads: 1", "maximum number of tries: 1"):
        require(expected in text, f"Missing pgbench confirmation: {expected}")


def load_ih(results):
    """Return engine -> scheme -> five raw throughputs, after all validations."""
    root = Path(results).resolve()
    try:
        return _load_ih(root)
    except (OSError, KeyError, TypeError, OverflowError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid IH bundle {root}: {exc}") from exc


def _load_ih(root):
    manifest = load_json(inside(root, "manifest.json"))
    complete = load_json(inside(root, "complete.json"))
    selection = load_json(inside(root, "selection.json"))
    require(manifest["protocol"] == PROTOCOL and manifest["runs"] == 125 and
            manifest["groups"] == 25 and manifest["runs_per_group"] == 5 and
            manifest["original_runs_selected"] == 122 and manifest["repeat_runs_selected"] == 3,
            "Invalid selected dataset dimensions")
    raw = csv_rows(checked_digest(root, "runs.csv", complete["raw_sha256"]))
    summaries = csv_rows(checked_digest(root, "summary.csv", complete["summary_sha256"]))
    expected = {f"main-{e}-b{b}-{s}" for e in ENGINES for s in schemes(e) for b in range(1, 6)}
    require(len(selection) == len(raw) == 125 and
            {r["logical_run_id"] for r in selection} == expected, "Invalid selected slots")
    require({p.name for p in inside(root, "runs").iterdir()} == expected, "Unexpected run directories")
    by_id = {r["run_id"]: r for r in raw}
    require(len(by_id) == 125 and set(by_id) == expected, "Duplicate/missing raw slots")
    campaigns = {}
    for field in ("original_campaign", "repeat_campaign"):
        campaign = manifest[field]
        path = inside(root, f"provenance/{campaign}/manifest.json")
        m = load_json(path)
        require(m["protocol"] == PROTOCOL and m["campaign"] == campaign, "Invalid campaign identity")
        schedule = {r["run_id"]: r for r in m["schedule"]}
        require(len(schedule) == len(m["schedule"]), "Duplicate scheduled identity")
        require(set(schedule) == (expected if field == "original_campaign" else REPEATS),
                "Invalid campaign schedule")
        campaigns[campaign] = (m, schedule)
    original = manifest["original_campaign"]
    repeat = manifest["repeat_campaign"]
    rm = campaigns[repeat][0]
    require(set(rm["repeat"]["requested_run_ids"]) == REPEATS, "Invalid replacement selection")
    checked_digest(root, f"provenance/{original}/manifest.json", rm["repeat"]["source_manifest_sha256"])
    data = {e: {s: [] for s in schemes(e)} for e in ENGINES}
    blocks = {(e, s): [] for e in ENGINES for s in schemes(e)}
    identities = set()
    for entry in selection:
        logical = entry["logical_run_id"]
        repeated = logical in REPEATS
        campaign, stage = (repeat, "repeat") if repeated else (original, "main")
        require(entry["selected_campaign"] == campaign and entry["selected_run_id"] == logical and
                entry["selected_stage"] == stage, f"Wrong selected identity: {logical}")
        identity = (campaign, entry["selected_run_id"])
        require(identity not in identities, "Duplicate source identity")
        identities.add(identity)
        directory = inside(root, f"runs/{logical}")
        r = load_json(checked_digest(directory, "accepted.json", entry["accepted_sha256"]))
        evidence = load_json(checked_digest(directory, "evidence.json", entry["evidence_sha256"]))
        evidence_paths = {item["path"] for item in evidence}
        required = {"spec.json", "compose.json", "container.json", "result.json", "result.csv",
                    "versions-before.txt", "versions-after.txt", "container.log"}
        if r["engine"] == "postgres":
            required |= {f"pgbench/{p}" for p in ("insert.sql", "read.sql", "preload.sql", "mixed.stdout",
                                                 "mixed.stderr", "preload.stdout", "preload.stderr")}
        require(len(evidence_paths) == len(evidence) and required <= evidence_paths,
                "Missing/duplicate evidence inventory entries")
        for item in evidence:
            checked_digest(directory, item["path"], item["sha256"])
        spec = load_json(inside(directory, "spec.json"))
        require(spec == campaigns[campaign][1][logical], "Specification differs from selected schedule")
        result = load_json(inside(directory, "result.json"))
        validate_measurement(result)
        require(all(r.get(k) == v for k, v in result.items()) and
                all(r.get(k) == v for k, v in spec.items()), "Accepted/result/spec mismatch")
        require(csv_rows(inside(directory, "result.csv")) == [flat(result)], "Run CSV/JSON mismatch")
        e, s, b = r["engine"], r["scheme"], r["block"]
        require(e in ENGINES and s in schemes(e) and type(b) is int and b in range(1, 6),
                "Unknown engine/scheme/block")
        require(logical == f"main-{e}-b{b}-{s}" and r["stage"] == stage, "Invalid logical slot")
        for k in ("engine", "scheme", "block", "seed"):
            require(entry[k] == r[k] == campaigns[original][1][logical][k], "Selection metadata mismatch")
        validate_record(r)
        if repeated:
            require(timestamp(rm["created"]) < timestamp(r["wall_started"]), "Repeat predates decision")
        info = load_json(inside(directory, "container.json"))
        service = load_json(inside(directory, "compose.json"))["services"][e]
        require(info["HostConfig"]["Memory"] == 8 * 1024**3 and
                info["HostConfig"]["NanoCpus"] == 4 * 10**9 and
                info["State"]["Running"] and not info["State"]["OOMKilled"] and not info["State"]["Error"],
                "Invalid container configuration/status")
        require(service["image"] == info["Image"] == campaigns[campaign][0]["images"][e]["Id"],
                "Container image mismatch")
        if e == "postgres":
            validate_pg(directory, r, evidence_paths)
        row = dict(r, selected_campaign=campaign, source_campaign=r.get("source_campaign", original),
                   source_run_id=r.get("source_run_id", logical), selected_source_stage=stage,
                   selection="repeat_after_external_load_overlap" if repeated else "original",
                   evidence_directory=f"runs/{logical}")
        require(row["selection"] == entry["selection"] and flat(row) == by_id[logical],
                "Raw CSV does not match selected evidence")
        data[e][s].append(float(by_id[logical]["throughput"]))
        blocks[e, s].append(b)
    require(all(sorted(bs) == [1, 2, 3, 4, 5] for bs in blocks.values()), "Invalid repetition count")
    summary = {(r["engine"], r["scheme"]): r for r in summaries}
    require(len(summaries) == len(summary) == 25 and set(summary) == set(blocks), "Invalid summary groups")
    for e in ENGINES:
        baseline = st.median(data[e]["sequential"])
        for s, values in data[e].items():
            row = summary[e, s]
            require(row["n"] == "5" and json.loads(row["raw_throughput"]) == values,
                    "Summary raw observations mismatch")
            near(float(row["median"]), st.median(values), "Summary median mismatch")
            near(float(row["percent_of_new_sequential"]), 100 * st.median(values) / baseline,
                 "Summary normalization mismatch")
    require(all(complete[k] == v for k, v in
                {"runs": 125, "groups": 25, "operations": 25_000_000, "errors": 0, "read_misses": 0}.items()),
            "Completion totals mismatch")
    return data


def ih_macros(data):
    """25 table cells; no obsolete anomaly gains or cross-engine prose range."""
    result = []
    for e, word in ENGINES.items():
        baseline = st.median(data[e]["sequential"])
        for s, suffix in schemes(e).items():
            value = f"{baseline:,.0f}" if s == "sequential" else f"{100 * st.median(data[e][s]) / baseline:.1f}"
            result.append((f"SingleInsertHeavy{word}{suffix}", value))
    return result
