# RaftKV Project Summary

## What Was Built

A pure-Java Raft-based distributed key-value store with:

- leader election
- log replication
- majority commit
- state-file persistence and node restart recovery
- snapshot and log compaction
- follower catch-up through AppendEntries or InstallSnapshot
- client redirect to leader
- benchmark/failover harness with returned-value and replica-state checks
- dependency-free regression tests and a JDK 17/21 CI configuration
- Java 17-compatible JAR packaging and a shared local/CI verification script
- a three-independent-JVM forced-leader-exit and persisted-state recovery check
- a configured workflow for verified JAR delivery, without automatic deployment

The KV state machine is an in-memory map. Persistence retains the original Java
serialization format and rewrites the whole state file, including the snapshot
and remaining log. This is a compact systems prototype, not a production storage
engine.

## Correctness And Recovery Changes

- Persist newly observed higher terms even when rejecting a request or handling
  an empty heartbeat, and retain the vote on same-term step-down.
- Commit only the prefix matched by the follower's current AppendEntries request;
  leave its existing log unchanged when the previous-entry check fails.
- Allow replication retries to reach the InstallSnapshot path, with a single
  outgoing replication stream per peer.
- Ignore stale snapshots and reject conflicting boundaries that would replace
  committed state; keep a later log suffix only when its snapshot boundary matches.
- Match pending results by index, term, and request ID so overwritten commands
  cannot receive their replacements' results. Timeouts still have uncertain
  outcomes; exactly-once deduplication is not implemented.
- Count the leader's own entry when advancing the commit index, including in a
  single-node cluster.
- Force the temporary state file, atomically replace the destination, and force
  the parent directory. Unsupported operations fail explicitly; persistence
  failure stops further successful Raft protocol acknowledgments.

The persistence operations were exercised on the local filesystem. Common
Windows Java providers may not support directory synchronization. Hardware and
filesystem behavior during power loss was not tested; newly created ancestor
directories are not synchronized recursively.

## How To Run

Run the full local verification entry point:

```bash
bash scripts/verify.sh
```

It runs the regression suite, packages the JAR, and checks that same JAR in
three separate JVMs. To run just the regression suite:

```bash
bash scripts/test.sh
```

Run the short cluster smoke check:

```bash
bash scripts/benchmark.sh --duration 1 --clients 2 --keys 100 --snapshot-threshold 10
```

Manual 3-node cluster:

```bash
bash scripts/start-node.sh n1 8001
bash scripts/start-node.sh n2 8002
bash scripts/start-node.sh n3 8003
```

Client:

```bash
bash scripts/client.sh put course raft
bash scripts/client.sh get course
bash scripts/client.sh status
```

## Validation And Its Scope

All 23 checks passed locally: eight persistence checks, twelve protocol
regression checks, and three real-HTTP integration scenarios. The integration
scenarios cover single-node CRUD/restart, follower recovery through a snapshot
after compaction and re-election, and rejection of new writes without a
majority. Local execution in this revision used JDK 24. Production and test
sources also compiled using that JDK's `--release 17` compatibility checks;
this does not substitute for executing tests on JDK 17. JDK 17 and 21 are
configured in the Ubuntu CI matrix; no hosted CI success is claimed here.

The default benchmark (5 seconds, 6 clients), a short run with snapshot threshold
10, and a short run with threshold 100000 all completed their value and recovery
checks. The high-threshold run correctly reported no snapshot because its
threshold was not reached. The updated
harness verifies:

- PUT, update, GET, DELETE, and missing-key response values.
- Stable canary values and absence of deleted keys before and after failover.
- Successful new writes after the elected leader is stopped, with another 80
  writes while it remains offline.
- After restart, equality of all three complete KV maps with the captured leader
  state and applied indexes reaching the captured target.
- Snapshot progress when the configured threshold is reached, and write
  throughput calculated from measured elapsed time.

These nodes share one JVM and communicate over real HTTP. The harness uses
explicit node stop/start; it does not kill JVM processes or simulate power loss.
Its value and convergence assertions are not a full concurrent-operation
linearizability check. The smoke run is a small correctness sample, so its QPS
and latency numbers are intentionally omitted from resume claims.

The additional independent-process check starts three JVMs from the delivery
JAR, verifies values, forcibly kills the elected leader, and makes another 25
writes on the remaining majority. It waits for snapshots beyond the killed
node's previous progress, restarts that node using its old directory, and checks
that all three persisted maps, applied indexes, and recovery snapshot progress
agree. Child processes are stopped afterwards; logs remain in local
`work/process-smoke-*` directories.

The delivery workflow is configured to run the same verification on manual or
version-tag triggers, then provide the tested JAR as a downloadable artifact.
It has not been executed on GitHub in this session and does not deploy a service.

## Remaining Boundaries

- No exactly-once request deduplication: a timed-out operation may commit later,
  and a retry may execute again.
- No ReadIndex, read leases, or dynamic membership changes.
- No independent append-only WAL or LSM engine; full-state serialization has
  growing write costs as the map and retained log grow.
- No real power-loss tests, arbitrary network-partition fault injection, or
  partition histories validated by a linearizability checker.
- No Docker deployment, operational metrics, shard router, multiple Raft groups,
  or data migration yet. These follow the local-first
  [learning path](../docs/scalability-and-delivery.md).

## Resume And Interview Use

Use the English and Chinese candidate bullets in
[the interview guide](../docs/interview-guide.md). Describe the concrete fixes
and checks you can explain. Include the command, workload, and environment when
discussing measured performance.

