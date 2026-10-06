#!/usr/bin/env python3
"""Explicit, provenance-preserving repeats of named runs; never replace originals.
Default prints a plan only. Use --execute --host-ready after external-load clearance.
"""
import argparse
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import ih_campaign as ih


def selection(source, run_ids, reason):
    source = source.resolve()
    manifest = json.loads((source / "manifest.json").read_text())
    complete = json.loads((source / "complete.json").read_text())
    if manifest["protocol"] != ih.PROTOCOL or manifest["mode"] != "full" or complete["mode"] != "full":
        raise ValueError("repeat source must be a completed full campaign")
    if not reason.strip() or not run_ids or len(set(run_ids)) != len(run_ids):
        raise ValueError("explicit reason and unique run IDs required")
    specs = [r for r in manifest["schedule"] if r["stage"] == "main"]
    if not set(run_ids) <= {r["run_id"] for r in specs}:
        raise ValueError("unknown/non-main source run ID")
    runs = []
    for spec in specs:
        if spec["run_id"] not in run_ids:
            continue
        if Path(spec["run_id"]).name != spec["run_id"]:
            raise ValueError("unsafe source run ID")
        original = json.loads((source / spec["run_id"] / "accepted.json").read_text())
        ih.validate_result(original, spec)
        if any(original.get(k) != spec[k] for k in ("run_id", "stage", "block")):
            raise ValueError("source acceptance identity mismatch")
        runs.append(dict(spec, stage="repeat", source_campaign=manifest["campaign"], source_run_id=spec["run_id"]))
    sources = {f["path"]: f["sha256"] for f in manifest["files"]
               if Path(f["path"]).parts[0] in ("cmd", "internal", "docker") or f["path"] in ("go.mod", "go.sum")}
    if "cmd/workload/main.go" not in sources or "go.mod" not in sources:
        raise ValueError("missing archived measurement source identity")
    binaries = {}
    for name in ("workload", "uuid-benchmark"):
        data = (source / name).read_bytes()
        if not data:
            raise ValueError("empty archived binary")
        binaries[name] = hashlib.sha256(data).hexdigest()
    provenance = dict(source_directory=str(source), source_campaign=manifest["campaign"],
                      reason=reason, requested_run_ids=run_ids, automatic_replacement=False,
                      selection_basis="operator-specified external-load overlap, not measured performance",
                      source_manifest_sha256=hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest(),
                      images=manifest["images"], binaries=binaries, measurement_sources=sources)
    return manifest, runs, provenance


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--runs", nargs="+", required=True)
    p.add_argument("--reason", required=True)
    p.add_argument("--campaign", required=True)
    p.add_argument("--timeout-hours", type=float, default=1)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--host-ready", action="store_true")
    args = p.parse_args()
    if not args.campaign.startswith("ih-corrected-") or not all(c.isalnum() or c in "-_" for c in args.campaign):
        p.error("campaign must be a plain ih-corrected-* directory name")
    if not 0 < args.timeout_hours <= 48:
        p.error("invalid timeout")
    manifest, runs, provenance = selection(args.source, args.runs, args.reason)
    if not args.execute:
        print(json.dumps(dict(campaign=args.campaign, repeat=provenance, runs=runs), indent=2))
        return
    if not args.host_ready:
        p.error("execution requires --host-ready and no concurrent builds/sync/benchmark")
    engines = [e for e in ih.ENGINES if any(r["engine"] == e for r in runs)]
    campaign_args = SimpleNamespace(campaign=args.campaign, timeout_hours=args.timeout_hours,
                                   mode="repeat", engines=engines, seed=manifest["order_seed"],
                                   skip_pilot=False, repeat=provenance)
    ih.execute_campaign(campaign_args, runs)


if __name__ == "__main__":
    main()
