# Handoff Prompt: Local Cross-Machine Validation

Copy everything between `===BEGIN PROMPT===` and `===END PROMPT===` into a
fresh Claude Code session running from inside the `uuid-benchmark` repo on
the laptop.

===BEGIN PROMPT===

You're continuing the uuid-benchmark project from inside the repo's
working directory (whatever path it's been cloned to on this machine —
do NOT assume an absolute path). The Cassandra `remote-cluster` mode
has been built and code-reviewed end-to-end but has never been run
against actually separate hosts. **Your job: validate it on the local
machine, then report back.** After this passes, the user will run it
for real on the Taurus HPC cluster.

## Preflight (do this first, before anything else)

The multi-node work lives on the **`multi_node` branch**, NOT on `main`.
If you skip this you will see files like `internal/cluster/` or
`docs/plans/2026-04-17-multi-node-cassandra-plan.md` reported as missing.

```bash
git fetch origin
git status                         # confirm clean tree
git checkout multi_node            # switch if not already there
git pull --ff-only origin multi_node
```

Then verify the expected files exist:

```bash
ls internal/cluster/remote_cluster.go \
   docs/plans/2026-04-17-multi-node-cassandra-plan.md \
   CLAUDE.md
grep -q "Cluster Modes (Cassandra)" CLAUDE.md && echo "CLAUDE.md OK"
```

If any of those fail, stop and tell the user — the branch state is wrong
and you can't proceed.

## Goal

Prove that

```
./uuid-benchmark -database=cassandra -cluster-mode=remote-cluster \
  -nodes=<host1>,<host2>,<host3> -ssh-user=<user> \
  -scenario=insert-performance -num-records=10000 \
  -replication-factor=3 -consistency=local_quorum
```

forms a ring across three separate hosts, completes the insert workload
for all 6 UUID types, aggregates metrics across the cluster, and tears
down cleanly.

## Setup — your call

You need three "hosts" each with:
- Their own L3 network namespace (separate IP, separate Docker bridge —
  not just three containers on one daemon)
- SSH from your user
- Docker installed
- Hostname resolvable from the other two

This is required because `CASSANDRA_BROADCAST_ADDRESS=<hostname>` relies
on per-host DNS, and the bug it guards against literally cannot manifest
on a shared Docker bridge.

Options, simplest to fullest:

1. **Three Docker daemons / nested DinD** with separate networks. Fast
   to spin up; tests the orchestration code but the kernel is shared.
2. **Three incus VMs** (Arch has `incus` in `extra`). Heavier (~12 GB
   RAM, ~5 min boot) but topologically identical to Taurus.

The user is on Arch + sway with 32 GB RAM. Either works. **Pick the
simplest option that meets the isolation requirement.** Briefly state
your choice and why before setting up.

## Run

The invocation example is in `CLAUDE.md` under "Cluster Modes
(Cassandra) — remote-cluster". Adapt the hostnames to your three hosts.
Expect ~10-15 min total wall-clock (ring forms ~2 min × 6 fresh
containers; insert work is fast).

Build the benchmark first:

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
```

Run in foreground; tee output to `/tmp/cross-machine-smoke.log` so you
can grep it later.

## Success criteria

Declare success if **all** hold:

1. All 6 UUID types complete with non-zero throughput in the comparison
   table.
2. No `ssh session timed out`, no `gossip did not reach 'running'`, no
   `ring did not stabilize`.
3. Three nodes appear in `nodetool status` mid-run (sanity-check from
   any one host).
4. After the run, `docker ps` on every host shows no `cassandra`
   container — Stop ran cleanly.

SSTable Count = 0 across all types is **expected** at 10K records
(memtable doesn't flush). Not a regression. UUID-type ordering deltas
being within ~10% with p > 0.05 is also **expected** at this small
scale.

## If it fails

Stop, do NOT try to fix the codebase. Report:

- The error from the log
- `nodetool status` from each host
- `docker logs cassandra` from the failing host(s)

Most likely culprits (in rough order):
- Hostname not resolvable from inside a Cassandra container on a peer
  host → run the equivalent of `docker run --rm cassandra:5 getent
  hosts hostM` from inside hostN to verify
- SSH key not in authorized_keys on a host → first error from the
  benchmark will name the host
- Port 7000 firewalled between your hosts → less likely on a laptop,
  but check with `nc -zv hostM 7000` from another host

## Cleanup

Tear down whatever you spun up. Don't leave VMs/containers running.

## Constraints

- **Never run git commands beyond the preflight checkout above.** The
  user handles git.
- **Don't refactor anything mid-validation.** Note observations, don't
  edit code. If you find a bug, report it; the user decides whether to
  fix.
- **Ask before destructive actions** beyond the VM/container lifecycle
  you set up here.
- **No CLAUDE.md / README.md / source edits.**

## Orientation

After the preflight, read these in order:
- `CLAUDE.md` — architecture + cluster modes + flag reference
- `internal/cluster/remote_cluster.go` — the code you're validating
- `docs/plans/2026-04-17-multi-node-cassandra-plan.md` — methodology

## Report back

When done:
1. Setup choice (DinD/VMs/other) and why.
2. Verdict: success / partial / failed.
3. Wall-clock duration.
4. Throughput rec/sec per UUID type (one number each from the comparison
   table).
5. Anything unexpected, including bugs to surface for the user.

===END PROMPT===
