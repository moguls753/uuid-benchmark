# Handoff Prompt: Cross-Machine Validation on Local Incus VMs

Copy everything between the `===BEGIN PROMPT===` and `===END PROMPT===` markers
below into a fresh Claude Code session running from
`/home/era/projects/uuid-benchmark/` on your laptop.

This prompt is **self-contained** — it embeds the full project context, every
relevant review finding, every recent fix, the exact incus setup, the
validation invocation, the success criteria, and a diagnostic playbook keyed
to the specific bugs the agent should expect (and rule out) if anything
fails.

---

===BEGIN PROMPT===

You are continuing work on a bachelor-thesis benchmark project at
`/home/era/projects/uuid-benchmark/`. The Cassandra multi-node code path has
just been hardened across three layers (metrics correctness, cluster backend
reliability, CLI/workload UX) and you are running the **cross-machine
validation step**: prove the `remote-cluster` mode actually works end-to-end
against three separate hosts before the user takes it to a real HPC cluster
(Taurus, FernUniversität Hagen) where time and admin coordination are
expensive.

The local machine is **Arch Linux + sway, 32 GB RAM, 8+ vCPU**. You will use
`incus` (Arch's official VM/container manager, package in `extra`) to spin up
three Ubuntu VMs that simulate the Taurus topology faithfully (each with its
own IP, kernel, Docker daemon, SSH).

Read `CLAUDE.md`, `docs/plans/2026-04-17-multi-node-cassandra-plan.md`, and
the controller-summary block below before doing anything. Do not skim.

---

## Project context (read in full)

**What this project is.** Bachelor thesis benchmarking UUID variants (UUIDv1,
UUIDv4, UUIDv7, ULID, ULID monotonic) vs. sequential integer keys across
four databases (PostgreSQL, MySQL, MongoDB, Cassandra). Measures page splits
/ compaction, fragmentation, cache/buffer-pool hit ratios, disk usage,
throughput, latency percentiles, cgroup-v2 I/O. The thesis was single-node;
the **paper extension** adds multi-node Cassandra to address the literature
gap on multi-host UUID behavior under LSM-tree storage with realistic
partition distribution.

**Cluster modes (Cassandra only — PG/MySQL/Mongo stay single-node):**
- `local-single` — one container, thesis baseline (untouched).
- `local-cluster` — three containers on one Docker network, code-validation
  only (only `cassandra-1` publishes 9042; queries route through it as
  coordinator — useless for performance numbers).
- `remote-cluster` — N real hosts over SSH, the paper-extension topology.
  This is what you are validating.

**Bucketed schema change (paper extension).** Thesis used `bucket = 1`
constant — single wide partition, single node. Paper extension keeps the
exact DDL `PRIMARY KEY ((bucket), id)` but computes
`bucket = FNV-1a(id_bytes) mod N` per row (default N=1000) so writes
distribute across the ring. Within each bucket the UUID is still the
clustering column and byte-sorted; across buckets, Cassandra's
Murmur3Partitioner spreads them across nodes.

**Why local cross-machine validation matters.** `local-cluster` (3
containers on one Docker bridge) **cannot catch** the class of bug that
matters for the paper-extension topology — same-bridge containers share an
IP namespace, so things like `broadcast_address` resolution and per-host
cgroup-via-SSH paths are not exercised. Three VMs with separate kernels and
IPs structurally mirror Taurus.

---

## What just landed (8 implementer rounds, 3 review rounds, 13 important
findings fixed)

### Task 8.1: cross-machine RemoteCluster hardening

Before this task, `remote-cluster` had never been pointed at separate hosts.
A structural bug was hiding: `cassandra:5`'s default `broadcast_address` is
the container's Docker-bridge IP (e.g. `172.17.0.2`), which is unroutable
from peer hosts. The ring would silently never form.

Three changes in `internal/cluster/remote_cluster.go`:

1. **`-e CASSANDRA_BROADCAST_ADDRESS=<hostname>`** added to docker run.
   Cassandra container resolves the hostname via its inherited DNS view and
   advertises a routable address to peers. The escape hatch (pre-resolve to
   an IP) is documented in the file's comments if your VM hostnames don't
   resolve inside the container — verify with `incus exec cassnode1 --
   docker run --rm cassandra:5 getent hosts cassnode2`.
2. **`docker pull cassandra:5`** added per host before `docker run`. Missing
   image now produces a clean host-named error, not an opaque docker-run
   failure.
3. **`nodetool statusgossip` polling per node** replaces the empirical
   `time.Sleep(45s/15s)` boot stagger. Per-host budget: 5 min. SSH/exec
   errors during boot are treated as "not ready, keep polling" (because
   nodetool legitimately exits non-zero before Cassandra opens its JMX
   port).

### Task 8.2: three parallel reviewers (cluster + metrics + CLI layers)

Reviewers A/B/C audited everything with ultrathink. Findings spanned all
three layers. The headline blocker was paper-validity-threatening; the rest
were operational-reliability and config-safety issues.

### Task 8.3: fixes applied across three parallel implementers

**Metrics & paper-correctness (Implementer 1):**

- 🟢 **BLOCKER fixed:** `fetchCassandraIDs` now samples uniformly across
  buckets via `PER PARTITION LIMIT` iteration. Before this fix the
  unfiltered `LIMIT N` scan returned ids in token order, concentrating
  reads on a tiny slice of buckets — which would have erased the
  read-amplification signal that's the whole point of measuring random vs.
  time-ordered UUID behavior on LSM at scale. Tests pin a ≥50% bucket-spread
  contract.
- 🟢 Per-node clamping in `buildBenchmarkResultPerNode` — compaction on one
  node during the snapshot window no longer cancels out workload-induced
  SSTable growth on another. Was: `max(0, sum_after - sum_before)`. Now:
  `sum(max(0, after_i - before_i))`.
- 🟢 Dead `CompactionBytesTotal` field removed (was declared and summed but
  never populated by `parseTableStats`). CLAUDE.md metric table updated.
- 🟢 `CaptureMetricsBeforeAll` fail-loud — any per-node nodetool error now
  fails the scenario run instead of silently leaving `metricsBefore=nil` and
  producing zero deltas in the CSV.
- 🟢 Per-node snapshot collection parallelized (`sync.WaitGroup` +
  `errors.Join`). Before: 3 nodes × 2 nodetool calls sequential, ~300ms
  snapshot drift. After: ~1 RTT total.

**Cluster backend reliability (Implementer 2):**

- 🟢 **SSH per-call timeout** via `Client.Timeout` field (default 2 min) +
  `runSession` wrapper around `ssh.Session.Run`. On timeout: `SIGKILL` +
  `Close` + clear "ssh session timed out" error. **This was the
  highest-leverage fix** — without it, a hung SSH session (PAM stall, NFS,
  wedged docker daemon) would silently wedge the whole benchmark run for
  the full `gossipBudget` and beyond, with no diagnostic. `Copy` also now
  uses `exec.CommandContext` so a stuck scp is killed.
- 🟢 `WaitForRing` polls **every** node (not just node 0) and requires all
  views to agree on "all UN". The previous single-node poll could declare
  success during a split-view bootstrap window where node 0 thinks all are
  UN but node 2 still thinks node 1 is DN — causing first-CQL-query retry
  storms.
- 🟢 `ClusterConfig.Validate` now rejects: empty hostname strings in slice
  (`hostnames[1] is empty`), duplicate hostnames (`duplicate hostname "a"`),
  single-host remote-cluster (`remote-cluster requires at least 2 hostnames;
  for a single host use -cluster-mode=local-single`).
- 🟢 `LocalCluster.Start` is transactional: on `docker compose up` failure
  it calls `Stop` (best-effort) before returning the error.

**CLI / workload UX (Implementer 3):**

- 🟢 Cluster flags rejected with `-database != cassandra` — no more
  silently-ignored `-cluster-mode=remote-cluster` running a local postgres
  container.
- 🟢 `currentDB.stop()` called on Start failure in the run loop — leak-free
  CLI lifecycle alongside the backend-layer transactional Start.
- 🟢 Native exec returns explicit error if `BinaryPath` is empty and the
  package-level `binaryPath` is empty — no more silent `./workload`
  fallback.

13 new tests pin every behavior. `go build ./...`, `go test ./...`, and
`go test -race ./internal/cluster/... ./internal/remote/...
./internal/benchmark/cassandra/... ./cmd/workload/...` are all green.

### Explicitly deferred (NITS — do NOT get distracted by these during validation)

- Error-message phrasing polish (e.g. RF-vs-host-count message could
  mention `-replication-factor`).
- Help-text formatting for mode-conditional defaults.
- `ContactPoints` cross-validation with `Hostnames` (currently masked
  because both come from the same `-nodes` flag).
- `-cluster-nodes` not rejected when used outside `local-cluster`.
- `cassandraMixed` no longer swallows `fetchCassandraIDs` errors (this
  one's already fixed).
- `nodetool info` regex case-sensitivity for "Key Cache" string.
- `/tmp/workload` cleanup hardening on remote hosts.
- `mysqlInsertBatch` quadratic string concatenation.

If you encounter any of these, note them but DO NOT fix mid-validation.

---

## Your task

In order:

1. **Set up three incus VMs** that simulate the Taurus topology.
2. **Build the benchmark binary on the host** (you do not need to install Go
   in the VMs — the binary runs on the host orchestrator and talks to the
   VMs over SSH/CQL).
3. **Run the cross-cluster validation invocation** below.
4. **Verify the success criteria** below.
5. **Report back** to the user with: what worked, what (if anything)
   failed, and which of the recent fixes were exercised.

You are NOT being asked to run the full thesis sweep. This is a
10K-record `insert-performance` smoke that exercises the cross-machine code
path end-to-end. The real production runs happen later on Taurus.

---

## Step 1: Set up three incus VMs

These commands are tested against `incus` from the Arch `extra` repo. If any
prerequisite is missing the agent should pause and ask the user, not
guess. Run them one at a time, verify each before the next.

```bash
# Install incus if not already present
sudo pacman -S --needed incus

# One-time daemon init (will prompt for storage pool, network, etc.)
# Accept the minimal defaults: dir storage (~30 GB), incusbr0 bridge.
sudo systemctl enable --now incus
sudo incus admin init --minimal

# Add your user to the incus-admin group; log out + back in or run newgrp.
sudo usermod -aG incus-admin $USER
newgrp incus-admin   # in current shell; subsequent shells auto-pick-up

# Launch three Ubuntu 24.04 VMs.
for n in 1 2 3; do
  incus launch images:ubuntu/24.04 cassnode$n --vm \
    -c limits.memory=4GiB -c limits.cpu=2
done

# Wait for cloud-init + DHCP (~30s)
sleep 45
incus list   # confirm all three RUNNING with IPs on incusbr0
```

You should see something like:

```
+-----------+---------+----------------------+-----------------+
|   NAME    |  STATE  |         IPV4         |      TYPE       |
+-----------+---------+----------------------+-----------------+
| cassnode1 | RUNNING | 10.x.x.A (enp5s0)    | VIRTUAL-MACHINE |
| cassnode2 | RUNNING | 10.x.x.B (enp5s0)    | VIRTUAL-MACHINE |
| cassnode3 | RUNNING | 10.x.x.C (enp5s0)    | VIRTUAL-MACHINE |
+-----------+---------+----------------------+-----------------+
```

### Inject the user's SSH key, install Docker, pre-pull image

```bash
# Add the user's public key to each VM's ubuntu user
for n in 1 2 3; do
  incus exec cassnode$n -- bash -c "mkdir -p /home/ubuntu/.ssh && chmod 700 /home/ubuntu/.ssh" 
  cat ~/.ssh/id_ed25519.pub | incus exec cassnode$n -- tee -a /home/ubuntu/.ssh/authorized_keys
  incus exec cassnode$n -- chown -R ubuntu:ubuntu /home/ubuntu/.ssh
  incus exec cassnode$n -- chmod 600 /home/ubuntu/.ssh/authorized_keys
done

# Install Docker in each VM and add ubuntu to the docker group
for n in 1 2 3; do
  incus exec cassnode$n -- bash -c "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io && usermod -aG docker ubuntu"
done

# Pre-pull cassandra:5 on each VM. The benchmark also pulls it (idempotent),
# but doing it now confirms outbound internet works in each VM and warms
# the image cache.
for n in 1 2 3; do
  echo "pulling on cassnode$n..."
  incus exec cassnode$n -- docker pull cassandra:5
done
```

### Make the VMs resolvable from the host by name

The benchmark passes hostnames in `-nodes=cassnode1,cassnode2,cassnode3`,
which means **both** the host (SSH client) and **each VM's cassandra
container** (gossip broadcast) must resolve those names.

Grab the IPs and add them to `/etc/hosts` on the host:

```bash
# Get the IPs
incus list -c n,4 --format csv | awk -F, '/^cassnode/{split($2,a," "); print a[1]" "$1}'
# Example output:
#   10.x.x.A cassnode1
#   10.x.x.B cassnode2
#   10.x.x.C cassnode3

# Append to /etc/hosts (sudo). Verify entries first.
# If you already have lines for cassnode1/2/3, replace them, do not duplicate.
```

The VMs already see each other by name via the incus DNS resolver in
`incusbr0` (incus auto-registers VM names in dnsmasq). Verify with:

```bash
incus exec cassnode1 -- getent hosts cassnode2 cassnode3
# both should resolve to the corresponding 10.x.x.* IPs
```

Critically, verify hostname resolution **inside a Cassandra container** on a
VM (this is what `CASSANDRA_BROADCAST_ADDRESS=cassnode1` relies on at
container startup):

```bash
incus exec cassnode1 -- docker run --rm cassandra:5 getent hosts cassnode2
# Must return a 10.x.x.* IP. If it returns 127.0.0.1 or empty, the
# CASSANDRA_BROADCAST_ADDRESS=hostname fix won't work and you need to
# escalate to the user (the escape hatch in remote_cluster.go talks about
# pre-resolving to an IP).
```

### Verify SSH from host to each VM

```bash
for n in 1 2 3; do
  echo "--- cassnode$n ---"
  ssh -o StrictHostKeyChecking=accept-new ubuntu@cassnode$n docker ps
done
```

Each should print an empty `docker ps` (no running containers — image is
just pulled, not running). If any fails: stop and diagnose before
proceeding.

---

## Step 2: Build the benchmark on the host

```bash
cd /home/era/projects/uuid-benchmark
go build -o uuid-benchmark cmd/benchmark/main.go
# Verify the binary
./uuid-benchmark -h 2>&1 | head -40
# Should show the cluster flags: -cluster-mode, -nodes, -ssh-user, -ssh-key, etc.
```

You do NOT need to install Go in the VMs. The orchestrator binary runs on
the host. The workload binary is built locally and runs natively on the
host in `remote-cluster` mode (talks to the ring over CQL).

---

## Step 3: Run the cross-cluster validation invocation

This is the smoke test:

```bash
cd /home/era/projects/uuid-benchmark
./uuid-benchmark \
  -database=cassandra \
  -cluster-mode=remote-cluster \
  -nodes=cassnode1,cassnode2,cassnode3 \
  -ssh-user=ubuntu \
  -ssh-key=$HOME/.ssh/id_ed25519 \
  -scenario=insert-performance \
  -num-records=10000 \
  -batch-size=100 \
  -connections=4 \
  -replication-factor=3 \
  -consistency=local_quorum \
  2>&1 | tee /tmp/cross-machine-smoke.log
```

Expected runtime: **~8-15 minutes** for all 6 UUID types. Cassandra ring
formation alone takes ~2-3 min × 6 fresh starts = ~15 min worth of just
bringup. Insert work itself is fast (10K records, ~10 sec per type).

Run this in the foreground so you can watch. If you need to background it
for some reason, use `run_in_background: true` and monitor via
`tail -f /tmp/cross-machine-smoke.log`.

---

## Step 4: Success criteria

Declare success if **all** of the following hold:

1. **Ring forms within ~3 min per UUID type.** Look for the absence of
   "gossip did not reach 'running'" or "ring did not stabilize" errors. The
   per-node gossip-running poll is logged implicitly (no error = success).
2. **All 6 UUID types complete.** The comparison table at the end shows
   SEQUENTIAL, UUIDV1, UUIDV4, UUIDV7, ULID, ULID_MONOTONIC each with a
   non-zero throughput.
3. **All three nodes participate in metrics.** SSTable Count, Space Used,
   key cache hit rate columns should reflect cluster-summed values, not
   one node's view. (Hard to verify without instrumentation, but: if all
   three nodes are healthy in `nodetool status` mid-run, aggregation is
   doing its job.)
4. **No SSH session timeouts.** The 2-min `Client.Timeout` should never
   fire on a healthy local network — if it does, that's a bug worth
   reporting.
5. **No "duplicate hostname" / "hostnames[N] is empty" / "remote-cluster
   requires at least 2 hostnames" errors.** Those are the new Validate
   guards; if they trip on a clearly-valid `-nodes=cassnode1,cassnode2,cassnode3`,
   that's a regression.
6. **Clean teardown.** After the run completes, `incus exec cassnode1 --
   docker ps` shows no `cassandra` container running (Stop ran cleanly on
   every node).

If all 6 hold, the cross-machine code path is validated and ready for the
real Taurus run once the user gets the other nodes provisioned.

---

## Step 5: Diagnostic playbook (if something fails)

Each failure mode below is keyed to a specific recent fix or known
limitation. **Use this table BEFORE jumping into general debugging** — the
fixes are recent and the symptoms are predictable.

| Symptom | Likely cause | Verify |
|---|---|---|
| `gossip did not reach 'running' within 5m0s` on every node | Image-pull issue OR container started but Cassandra crashed | `incus exec cassnodeN -- docker logs cassandra` → look for OOM, port conflict, schema error |
| `gossip did not reach 'running'` on one node only | Per-node resource constraint, or `CASSANDRA_BROADCAST_ADDRESS=cassnodeN` failed to resolve inside that VM's container | Run `incus exec cassnodeN -- docker run --rm cassandra:5 getent hosts cassnodeN` |
| `ring did not stabilize, view from node X: UN=2,DN=1 [...]` | The new all-views WaitForRing is doing its job — gossip is asymmetric. Wait longer OR check network between VMs | `incus exec cassnode1 -- nc -zv cassnode2 7000` (gossip port) |
| `start node 1 (cassnode2): ssh session timed out` | SSH session-level timeout fired (2 min default). VM is up but unresponsive | `ssh ubuntu@cassnode2 uptime` from host — if hangs, the VM itself is in trouble |
| `pull image on node 1 (cassnode2): ... exit status 1` | `docker pull` failed inside the VM | `incus exec cassnode2 -- docker pull cassandra:5` to see the raw error (network? daemon down?) |
| `cluster flags ... are only valid with -database=cassandra` | You forgot `-database=cassandra` | Add it |
| `remote-cluster requires at least 2 hostnames` | Trimmed `-nodes` to one. Use `-cluster-mode=local-single` for one-host runs | — |
| `duplicate hostname "cassnode1"` | Typo in `-nodes` | Fix the comma-separated list |
| `hostnames[1] is empty` | Trailing comma or double comma in `-nodes` | Trim whitespace, no empty entries |
| `native mode requires BinaryPath ... or a prior workload.BuildBinary() call` | You're calling Execute from a non-CLI entry point | Should never happen via the standard CLI path |
| Workload completes but SSTable Count == 0 for every UUID type | EXPECTED at 10K records (memtable doesn't flush). Same caveat as Task 6.3 / 7.3. NOT a bug. | — |
| Comparison table shows UUIDv4 faster than UUIDv7 by < 10% with all p-values n.s. | EXPECTED at 10K records / 1000 buckets = ~10 rows/bucket. UUID-ordering effect is diluted at this scale. The thesis-strength ordering signal needs 100M+ records. NOT a regression. | — |
| `nodetool tablestats` errors AND scenario run fails | New fail-loud behavior is correct. Diagnose the per-node nodetool failure (likely transient SSH or container restart) | Re-run the smoke; if persistent, file a bug |
| Comparison table missing SSTable / cache columns entirely | Metric capture errored out and scenario failed early. New fail-loud behavior. | Look upstream in the log for the per-node error |

### If you have to escalate to the user

If diagnosis runs more than ~30 minutes or the failure doesn't fit any row
above, **stop and ask the user** rather than restarting the smoke
indefinitely. Bring:

- The relevant slice of `/tmp/cross-machine-smoke.log`.
- `incus list` output.
- For the failing node: `incus exec cassnodeN -- docker logs cassandra | tail -100`.
- `ssh ubuntu@cassnodeN docker ps` output.

---

## Step 6: Report back

When done (success or partial), report:

1. **One-line verdict.** Success / Partial / Failed.
2. **Wall-clock duration.**
3. **Which of the recent fixes were exercised in the log.** Specifically:
   - Did the per-node gossip-running poll succeed (i.e. no "did not reach
     running" errors)?
   - Did the all-views WaitForRing produce a "view from node X" diagnostic
     at any point (means split-view was caught and tolerated)?
   - Were `CASSANDRA_BROADCAST_ADDRESS=cassnodeN` env vars in the
     orchestrator's effective docker-run calls? (Verify by `grep
     CASSANDRA_BROADCAST cross-machine-smoke.log` — won't be there unless
     the orchestrator logs the docker-run argv, which it currently
     doesn't; alternative: `ssh ubuntu@cassnode1 docker inspect cassandra
     --format '{{range .Config.Env}}{{println .}}{{end}}'` while the
     container is still running.)
4. **Throughput summary** for the 6 UUID types, one number each
   (rec/sec, from the comparison table). The user wants a sanity check,
   not statistical analysis.
5. **Anything unexpected.**
6. **Teardown status** — confirm no leftover containers in any VM.

---

## Cleanup (after success OR failure)

Always tear down the VMs after the smoke; they cost RAM and the user wants
a clean state:

```bash
incus stop cassnode1 cassnode2 cassnode3
incus delete cassnode1 cassnode2 cassnode3
# Verify
incus list
# Should not list any cassnode*
```

Also remove the `/etc/hosts` entries you added.

---

## Hard constraints (non-negotiable)

These come from project memory (`~/.claude/projects/-home-era-projects-uuid-benchmark/memory/MEMORY.md`):

- **NEVER run git commands.** No `git add`, `git commit`, `git push`,
  `git status`, etc. The user handles all git themselves.
- **NEVER fix nits during validation.** The deferred-items list above is
  explicitly off-limits. If you notice something else, note it for the
  user but do not edit.
- **Ask before any destructive action** beyond the VM lifecycle in this
  prompt (i.e. don't delete benchmark output, don't reset state, don't
  modify CLAUDE.md or any source file). The smoke run is read-only on the
  repo; the only writes should be to `/tmp/cross-machine-smoke.log` (an
  output artifact you create here).
- **No premature optimization, no overengineering.** If the smoke succeeds
  you're done — do not start "improving" anything.
- **If you hit Write/Edit tool permission denials, do not abandon.**
  Report the precise diff/action you wanted to take and let the user
  apply it.

---

## Files for orientation (read these in order)

1. `CLAUDE.md` — project root, full architecture
2. `docs/plans/2026-04-17-multi-node-cassandra-plan.md` — multi-node
   plan and methodology
3. `internal/cluster/remote_cluster.go` — the layer you are validating
4. `internal/cluster/ring.go` — the new all-views WaitForRing
5. `internal/remote/ssh.go` — the new per-call timeout
6. `internal/benchmark/cassandra/metrics.go` — per-node clamping +
   parallel snapshot
7. `cmd/workload/main.go` — `fetchCassandraIDs` (search for it)
8. `cmd/benchmark/main.go` — CLI dispatch and `buildClusterConfig`

You should not need to modify any of these during validation.

===END PROMPT===
