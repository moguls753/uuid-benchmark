import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import ih_campaign as ih
import ih_repeat
from test_ih_campaign import valid_result


class RepeatTests(unittest.TestCase):
    def source(self, directory):
        specs = [r for r in ih.schedule("full", 42, ih.ENGINES) if r["stage"] == "main"]
        manifest = dict(protocol=ih.PROTOCOL, mode="full", campaign="ih-corrected-original", order_seed=42,
                        schedule=specs, images={e: {"Id": "sha256:"+e} for e in ih.ENGINES},
                        files=[dict(path="cmd/workload/main.go", sha256="code"), dict(path="go.mod", sha256="modules")])
        ih.write_json(directory / "manifest.json", manifest)
        ih.write_json(directory / "complete.json", dict(mode="full", runs=125))
        for name in ("workload", "uuid-benchmark"):
            (directory / name).write_bytes(b"exact archived binary")
        for spec in specs[:3]:
            d = directory / spec["run_id"]
            d.mkdir()
            r = valid_result(spec)
            r.update({k: spec[k] for k in ("run_id", "stage", "block")})
            ih.write_json(d / "accepted.json", r)
        return specs

    def test_preserves_original_order_seeds_and_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            specs = self.source(d)
            before = {str(p.relative_to(d)): p.read_bytes() for p in d.rglob("*") if p.is_file()}
            manifest, runs, provenance = ih_repeat.selection(d, [specs[2]["run_id"], specs[0]["run_id"]], "documented build overlap")
            self.assertEqual([r["run_id"] for r in runs], [specs[0]["run_id"], specs[2]["run_id"]])
            for r, original in zip(runs, [specs[0], specs[2]]):
                self.assertEqual(r["stage"], "repeat")
                for k in ("seed", "engine", "scheme", "block", "requested_ops", "requested_preload"):
                    self.assertEqual(r[k], original[k])
                self.assertEqual(r["source_run_id"], original["run_id"])
            self.assertFalse(provenance["automatic_replacement"])
            self.assertEqual(provenance["binaries"]["workload"], hashlib.sha256(b"exact archived binary").hexdigest())
            self.assertEqual(before, {str(p.relative_to(d)): p.read_bytes() for p in d.rglob("*") if p.is_file()})

    def test_rejects_unknown_duplicate_invalid_source_or_missing_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            specs = self.source(d)
            run_id = specs[0]["run_id"]
            for ids, reason in [(["unknown"], "overlap"), ([run_id, run_id], "overlap"), ([run_id], " "), ([], "overlap")]:
                with self.assertRaises(ValueError):
                    ih_repeat.selection(d, ids, reason)
            target = d / run_id / "accepted.json"
            r = json.loads(target.read_text())
            r["end_count"] -= 1
            target.write_text(json.dumps(r))
            with self.assertRaises(ValueError):
                ih_repeat.selection(d, [run_id], "overlap")


if __name__ == "__main__":
    unittest.main()
