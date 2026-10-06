import csv
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import ih_campaign as ih


def valid_result(spec):
    n, ops = spec["requested_preload"], spec["requested_ops"]
    ins = ops * 7 // 10
    r = {key: spec[key] for key in ("engine", "scheme", "seed", "requested_preload", "requested_ops")}
    r.update(protocol=ih.PROTOCOL, valid=True, preload_count=n, unique_read_targets=n,
             insert_attempts=ins, insert_successes=ins, read_attempts=ops-ins, read_successes=ops-ins,
             completed_ops=ops, end_count=n+ins, read_misses=0, errors={}, throughput=ops/10,
             latency_p50_us=10, latency_p95_us=10, latency_p99_us=10,
             started="2026-01-01T00:00:01Z", ended="2026-01-01T00:00:15Z",
             mixed_started="2026-01-01T00:00:03Z", mixed_ended="2026-01-01T00:00:13Z",
             phase_seconds=dict(preload=1, verify_preload=1, mixed=10, verify_end=1, total=14),
             preload_sequential_range=dict(min=1, max=n), insert_sequential_range=dict(min=n+1, max=n+ins),
             end_sequential_range=dict(min=1, max=n+ins))
    return r


def pg_archive(directory, r):
    directory.mkdir()
    table = "bench_"+r["scheme"]
    if r["scheme"] == "sequential":
        insert = f"INSERT INTO {table} (data) VALUES (decode(repeat('41', 1024), 'hex')) RETURNING id \\gset inserted_\n"
    else:
        insert = f"INSERT INTO {table} (id, data) VALUES (gen_random_uuid(), decode(repeat('41', 1024), 'hex')) RETURNING id \\gset inserted_\n"
    read = f"\\set rn random(1, {r['requested_preload']})\nSELECT * FROM {table} WHERE id = (SELECT id FROM {table}_ids WHERE rn = :rn) \\gset read_\n"
    for name, value in {"preload.sql": "BEGIN; INSERT; COMMIT;", "insert.sql": insert, "read.sql": read,
                        "preload.stdout": "preload complete", "preload.stderr": "", "mixed.stderr": "",
                        "mixed.stdout": f"number of transactions actually processed: {r['completed_ops']}/{r['requested_ops']}\n"}.items():
        (directory / name).write_text(value)
    with (directory / "mixed_log.1").open("w") as f:
        for i in range(r["completed_ops"]):
            f.write(f"0 {i} 10 {int(i >= r['insert_successes'])} 100 1\n")


def pilot_evidence(directory):
    runs = ih.schedule("full", 42, ih.ENGINES)
    accepted = []
    for spec in runs[:8]:
        d = directory / spec["run_id"]
        d.mkdir()
        r = valid_result(spec)
        for name, obj in (("result.json", r), ("spec.json", spec), ("compose.json", {}), ("container.json", {})):
            ih.write_json(d / name, obj)
        for name in ("versions-before.txt", "versions-after.txt", "container.log"):
            (d / name).write_text("database version / log evidence")
        ih.export_csv(d / "result.csv", [r])
        if spec["engine"] == "postgres":
            pg_archive(d / "pgbench", r)
        ih.write_json(d / "evidence.json", ih.verify_run_evidence(d, spec, r))
        r.update({key: spec[key] for key in ("run_id", "stage", "block")})
        r.update(wall_started="2026-01-01T00:00:00Z", wall_ended="2026-01-01T00:00:16Z", wall_seconds=16)
        ih.write_json(d / "accepted.json", r)
        accepted.append(r)
    return runs, accepted


class CampaignTests(unittest.TestCase):
    def test_schedule_and_gate_order(self):
        runs = ih.schedule("full", 42, ih.ENGINES)
        self.assertEqual(runs, ih.schedule("full", 42, ih.ENGINES))
        self.assertEqual(len(runs), 133)
        self.assertEqual(len({r["run_id"] for r in runs}), 133)
        self.assertEqual([r["stage"] for r in runs[:8]], ["pilot"] * 8)
        main = runs[8:]
        for engine in ih.ENGINES:
            for block in range(1, 6):
                expected = ih.SCHEMES + (["objectid"] if engine == "mongodb" else [])
                actual = [r["scheme"] for r in main if r["engine"] == engine and r["block"] == block]
                self.assertCountEqual(actual, expected)
        self.assertNotEqual(runs, ih.schedule("full", 43, ih.ENGINES))

    def test_dry_run_never_touches_docker_or_files(self):
        with mock.patch.object(sys, "argv", ["ih_campaign.py", "--seed=1", "--preload=100", "--ops=200"]), mock.patch.object(ih, "execute_campaign") as execute, mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            ih.main()
        execute.assert_not_called()
        self.assertEqual(len(json.loads(out.getvalue())["runs"]), 8)

    def test_explicit_pilot_bypass_keeps_main_order_and_counts(self):
        original = ih.schedule("full", 42, ih.ENGINES)
        bypass = ih.schedule("full", 42, ih.ENGINES, skip_pilot=True)
        self.assertEqual(bypass, original[8:])
        self.assertEqual(len(bypass), 125)
        with mock.patch.object(sys, "argv", ["ih_campaign.py", "--mode=full", "--skip-pilot", "--seed=42"]), mock.patch.object(ih, "execute_campaign") as execute, mock.patch("sys.stdout", new_callable=io.StringIO) as out, mock.patch("sys.stderr", new_callable=io.StringIO) as warning:
            ih.main()
        execute.assert_not_called()
        self.assertTrue(json.loads(out.getvalue())["skip_pilot"])
        self.assertEqual(json.loads(out.getvalue())["runs"], bypass)
        self.assertIn("bypass", warning.getvalue())
        for mode in ("smoke", "pilot"):
            with self.assertRaises(ValueError):
                ih.schedule(mode, 42, ih.ENGINES, skip_pilot=True)

    def test_bypass_records_not_passed_and_default_still_requires_gate(self):
        runs = ih.schedule("full", 42, ih.ENGINES, skip_pilot=True)
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with mock.patch.object(ih, "evaluate_pilot", side_effect=ValueError("pilots required")) as gate:
                with self.assertRaisesRegex(ValueError, "pilots required"):
                    ih.main_start_gate(d, [], runs, 0)
                gate.assert_called_once()
                gate.reset_mock()
                assessment = ih.main_start_gate(d, [], runs, 0, skip_pilot=True)
                gate.assert_not_called()
            self.assertFalse(assessment["passed"])
            self.assertTrue(assessment["skipped"])
            self.assertTrue(assessment["per_run_validation_required"])
            self.assertFalse(assessment["budget_assessed"])
            self.assertEqual(json.loads((d/"pilot-gate.json").read_text()), assessment)
            with self.assertRaises(ValueError):
                ih.main_start_gate(d, [{}], runs, 0, skip_pilot=True)

    def test_full_cannot_shrink(self):
        with mock.patch.object(sys, "argv", ["ih_campaign.py", "--mode=full", "--seed=1", "--preload=100"]), mock.patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit):
                ih.main()

    def test_isolated_compose(self):
        resolved = {"services": {"pg": dict(image="tag", build=".", container_name="old", ports=["5432"], networks=["shared"], environment={}, volumes=["old:/data"], deploy={"resources": {"limits": {"cpus": "4", "memory": "8G"}}})}}
        result = ih.compose_config("postgres", "sha256:abc", "ih-test-001", Path("/snapshot"), resolved)
        svc = result["services"]["postgres"]
        self.assertEqual(svc["image"], "sha256:abc")
        self.assertEqual(svc["container_name"], "ih-test-001")
        self.assertEqual(svc["pull_policy"], "never")
        self.assertNotIn("ports", svc)
        self.assertNotIn("build", svc)
        self.assertNotIn("networks", svc)
        self.assertNotIn("name", result["volumes"]["data"])
        with self.assertRaises(ValueError):
            ih.verify_owned({"Name": "/uuid-bench-postgres", "Config": {"Labels": {}}}, "ih-test-001")

    def test_csv_nested_evidence_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runs.csv"
            ih.export_csv(path, [dict(engine="postgres", insert_successes=7, errors={"pgbench_failed": 1}, phase_seconds={"mixed": 1.2})])
            with path.open() as f:
                row = next(csv.DictReader(f))
            self.assertEqual(json.loads(row["errors"]), {"pgbench_failed": 1})
            self.assertEqual(json.loads(row["phase_seconds"]), {"mixed": 1.2})
            self.assertEqual(int(row["insert_successes"]), 7)
            with self.assertRaises(FileExistsError):
                ih.export_csv(path, [])

    def test_summary_never_uses_pilot_or_historical_baseline(self):
        rows = [dict(engine="mysql", scheme=scheme, block=i, stage="main", throughput=throughput)
                for scheme, throughput in [("sequential", 10), ("uuidv4", 5)] for i in range(1, 6)]
        rows.append(dict(engine="mysql", scheme="sequential", block=0, stage="pilot", throughput=100000))
        stats = ih.summary(rows)
        self.assertEqual(stats[1]["percent_of_new_sequential"], 50)
        with self.assertRaises(ValueError):
            ih.summary(rows[1:])

    def test_result_rejects_upsert_and_miss(self):
        spec = dict(engine="postgres", scheme="sequential", seed=1, requested_preload=100, requested_ops=10)
        r = valid_result(spec)
        ih.validate_result(r, spec)
        for key, value in [("end_count", 100), ("read_misses", 1), ("insert_attempts", 8), ("unique_read_targets", 99)]:
            with self.assertRaises(ValueError):
                ih.validate_result(dict(r, **{key: value}), spec)

    def test_timing_is_mandatory_and_reconciles(self):
        spec = ih.schedule("smoke", 1, ["mysql"], 100, 10)[0]
        r = valid_result(spec)
        ih.validate_result(r, spec)
        for key in ("started", "ended", "mixed_started", "mixed_ended", "phase_seconds"):
            incomplete = dict(r)
            del incomplete[key]
            with self.assertRaises((ValueError, KeyError)):
                ih.validate_result(incomplete, spec)
        for value in (float("nan"), float("inf"), 0, -1, 999):
            bad = dict(r, phase_seconds=dict(r["phase_seconds"], mixed=value))
            with self.assertRaises(ValueError):
                ih.validate_result(bad, spec)
        with self.assertRaises(ValueError):
            ih.validate_result(dict(r, throughput=99), spec)

    def test_pg_archive_requires_all_files_and_reconciles(self):
        spec = ih.schedule("smoke", 1, ["postgres"], 100, 10)[0]
        r = valid_result(spec)
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / "pgbench"
            pg_archive(d, r)
            ih.verify_pg_archive(d, r)
            with self.assertRaises(ValueError):
                ih.verify_pg_archive(d, dict(r, read_successes=99))
            (d / "read.sql").write_text("SELECT 1;\n")
            with self.assertRaises(ValueError):
                ih.verify_pg_archive(d, r)

    def test_pilot_gate_requires_evidence_exports_and_budget(self):
        for failure in (None, "missing-evidence", "missing-timing", "export", "budget", "changed-archive", "duplicate-pilot"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                d = Path(tmp)
                runs, accepted = pilot_evidence(d)
                deadline = time.monotonic() + 20000
                if failure == "missing-evidence":
                    # Alter a fixture only, never a campaign/user artifact.
                    (d / accepted[0]["run_id"] / "pgbench/mixed.stdout").write_text("")
                if failure == "missing-timing":
                    del accepted[0]["wall_seconds"]
                if failure == "changed-archive":
                    (d / accepted[0]["run_id"] / "container.log").write_text("changed")
                if failure == "duplicate-pilot":
                    accepted[1] = accepted[0]
                if failure == "budget":
                    deadline = time.monotonic() + 10
                if failure == "export":
                    with mock.patch.object(ih, "export_csv", side_effect=OSError("disk full")):
                        with self.assertRaises(OSError):
                            ih.evaluate_pilot(d, accepted, runs, deadline)
                elif failure:
                    with self.assertRaises((ValueError, KeyError)):
                        ih.evaluate_pilot(d, accepted, runs, deadline)
                else:
                    assessment = ih.evaluate_pilot(d, accepted, runs, deadline)
                    self.assertTrue(assessment["passed"])
                    self.assertEqual(assessment["estimated_main_seconds"], 2*16*125+600)
                    with (d / "pilot-runs.csv").open() as f:
                        self.assertEqual(len(list(csv.DictReader(f))), 8)
                if failure:
                    gate = d / "pilot-gate.json"
                    self.assertTrue(not gate.exists() or json.loads(gate.read_text())["passed"] is False)

    def test_pg_copy_failure_preserves_container_and_stops_sequence(self):
        for failed_stop in (False, True):
            with self.subTest(failed_stop=failed_stop), tempfile.TemporaryDirectory() as tmp:
                d = Path(tmp)
                commands = mock.Mock(password="secret", env={})
                project = "ih-test-000"
                info = dict(Id="id", Name="/"+project, Image="sha256:abc", Config={"Labels": {"uuid-ih.campaign": project}},
                            HostConfig=dict(Memory=8*1024**3, NanoCpus=4*10**9), Mounts=[], State=dict(Running=True, Restarting=False))
                calls = []
                spec = ih.schedule("smoke", 1, ["postgres"], 100, 10)[0]
                def command(args, **kwargs):
                    calls.append(args)
                    if args[:2] == ["docker", "inspect"]:
                        return subprocess.CompletedProcess(args, 0, json.dumps([info]), "")
                    if args[:2] == ["docker", "stop"]:
                        info["State"]["Running"] = failed_stop
                        return subprocess.CompletedProcess(args, int(failed_stop), "", "")
                    if args[:2] == ["docker", "cp"] and args[2] == project+":/tmp/ih" and not kwargs.get("preserve"):
                        self.assertTrue(kwargs.get("check", True))
                        raise RuntimeError("copy failed")
                    if "--op=ih-corrected" in args:
                        return subprocess.CompletedProcess(args, 0, json.dumps(valid_result(spec)), "")
                    return subprocess.CompletedProcess(args, 0, "version evidence", "")
                commands.run.side_effect = command
                resolved = {"services": {"postgres": {"environment": {}}}}
                next_run = mock.Mock()
                with self.assertRaisesRegex(RuntimeError, "STOP UNCONFIRMED" if failed_stop else "copy failed"):
                    ih.run_one(spec, d, d, "sha256:abc", resolved, commands, d/"binary", 0, "test")
                    next_run()
                next_run.assert_not_called()
                self.assertFalse(any("down" in args for args in calls))
                self.assertFalse((d / spec["run_id"] / "accepted.json").exists())
                status = json.loads((d / spec["run_id"] / "stop-status.json").read_text())
                self.assertEqual(status["stopped_confirmed"], not failed_stop)
                self.assertTrue(any(args[:2] == ["docker", "stop"] for args in calls))


if __name__ == "__main__":
    unittest.main()
