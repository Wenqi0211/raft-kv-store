# Interview Guide

## Resume Bullets

These statements describe the implemented behavior and tests. Choose the points
you can explain in detail and reproduce during a project discussion.

- Improved a pure-Java Raft KV prototype by correcting term/vote persistence,
  matched-prefix commitment, request-result matching, and snapshot recovery
  boundaries; added focused protocol regression tests.
- Strengthened state-file persistence with forced file writes, atomic replacement,
  directory synchronization, and tests for restart round trips, corrupt files,
  and failed saves.
- Extended a real-HTTP cluster harness to verify CRUD results, leader failover,
  snapshot catch-up, and agreement of all three replicas' KV maps and applied
  indexes; added a JDK 17/21 CI configuration.
- Added a shared verification entry point, a Java 17-compatible JAR, and a
  three-process forced-leader-exit recovery check; configured artifact delivery
  after verification. Container deployment and sharding remain future work.

### Alternative Resume Wording

- Improved a Java Raft distributed key-value store prototype by fixing term and
  vote persistence, log-prefix commitment, and snapshot recovery boundaries, and
  added protocol regression tests.
- Added file forcing, atomic replacement, and directory synchronization for
  state files, covering state recovery, rejection of corrupt files, and
  preservation of the previous state when a save fails.
- Improved a three-node verification flow using real HTTP to check read, write,
  and delete results, leader changes, snapshot catch-up, and replica-state
  convergence, and configured an automated JDK 17/21 test workflow.
- Added a shared verification entry point, a Java 17-compatible runnable package,
  and a forced-leader-exit recovery check using three independent JVMs, and
  configured an automated workflow to deliver the package after verification.

The short local benchmark is a correctness smoke run, not a performance study.
Performance comparisons need reproducible measurements with matching conditions.
A configured CI matrix is also different from a completed green CI run.

## Deep-Dive Talking Points

### Why reads go through Raft

`GET` is a replicated log command and returns after committed application, rather
than reading a follower's local map. This puts reads and writes in the same log
order, at the cost of replication, persistence, and log-application work for
every read. ReadIndex and leader leases are not implemented. The regression
suite is evidence about its covered cases, not a proof of linearizability under
arbitrary failures.

### What makes an entry committed

The leader commits an entry from its current term when a majority of match
indexes are at or beyond that entry. This follows the Raft rule that prevents a
new leader from incorrectly committing old-term entries by counting replicas.
The leader counts itself, so a one-node cluster can also commit.

A follower bounds its new commit index by the prefix actually matched by the
current AppendEntries request. Its local log may have a longer, conflicting
suffix from an old leader; local log length alone cannot justify committing that
suffix. A failed previous-entry check does not itself delete local entries.

### Why term and vote updates need careful ordering

A node that learns a higher term must persist that change even when it rejects
the vote request or receives an empty heartbeat. Otherwise a restart could make
it forget the term it already observed. Stepping down within the same term must
retain its vote, so it cannot vote for a second candidate in that term.

### What is persisted

One Java-serialized `PersistentState` contains term, vote, log suffix, commit
index, last-applied index, and the latest snapshot. On restart, the KV map is
restored from the snapshot and then rebuilt by replaying the applied log suffix.

Saving writes a unique temporary file in the data directory, flushes the object
stream, forces the file, atomically replaces the state file, and forces the
directory. Unsupported atomic replacement or directory synchronization fails
explicitly. An error after replacement can mean the new state is already
installed but its durability is unconfirmed. A node with a persistence failure
enters a terminal error state instead of continuing to acknowledge Raft RPCs.

This is still whole-state serialization, not an append-only WAL. Its write cost
grows with snapshot and retained-log size. The tested filesystem supports the
required operations; common Windows Java providers may not support directory
channels. File forcing is not an experimentally verified power-loss guarantee,
and newly created ancestor directories are not synchronized recursively.

### How log compaction works

The snapshot stores the full KV map at `lastIncludedIndex` and
`lastIncludedTerm`. After that, log entries up to the snapshot index can be
discarded. A follower whose `nextIndex` falls behind the leader snapshot receives
InstallSnapshot.

The leader's retry position must be allowed to fall into the compacted prefix;
clamping it above the snapshot can make a lagging follower retry AppendEntries
forever. Replication is serialized per peer so overlapping responses do not race
to change that peer's progress.

Installing a snapshot must not roll back applied or committed state. The later
local log suffix is retained only when the snapshot boundary's index and term
match locally; a conflicting boundary requires discarding that suffix. The
snapshot contains the current map, so a previously deleted key remains absent
after restoration.

### Why a pending request needs more than a log index

An old leader's entry can be overwritten by another command at the same index.
Pending results are matched using index, term, and request ID, so the old caller
cannot receive the replacement command's result as its own success.

This is not exactly-once execution. The request ID is used to identify the
pending operation, not as a durable deduplication table. A timeout or leadership
change leaves the outcome uncertain: the original command might still commit,
and a client retry can append it again.

## What The Tests Establish

Run the dependency-free test programs with:

```bash
bash scripts/test.sh
```

The suite defines 23 named checks:

- Eight storage checks cover serialization round trips and replacement, legacy
  file compatibility, stale temporary paths, corrupt/wrong-type files, and
  preservation of the old state or target when a save fails.
- Twelve Raft regression checks exercise higher-term persistence, same-term vote
  retention, matched-prefix commitment, conflict handling, snapshot boundaries,
  request-result identity, and terminal handling of persistence failures.
- Three real-HTTP integration checks exercise single-node CRUD and restart,
  snapshot catch-up after compaction and a new election, and refusal to
  acknowledge/apply a new write without a majority.

All 23 checks passed locally with JDK 24. Production and test sources also compile
with `javac --release 17` on that JDK; this is not execution on JDK 17.
The workflow is configured for
JDK 17 and 21 on Ubuntu; that configuration is not evidence those hosted jobs
have already passed.

For the cluster smoke run:

```bash
bash scripts/benchmark.sh --duration 1 --clients 2 --keys 100 --snapshot-threshold 10
```

The harness checks returned CRUD values, stable canary values and deletion,
stops the elected leader, makes further writes while it is offline, and restarts
it from the same directory. Success requires all three full local maps to match
the observed leader state and all applied indexes to reach the captured target.
The known-value checks give an independent expected result for the canaries and
recovery writes; concurrent benchmark writes are not checked against a complete
linearizable operation history. QPS uses measured elapsed time, not only the
requested duration.

The benchmark and in-process integration tests use one JVM with real HTTP connections. Node stop/start exercises
recovery logic, but does not simulate killing a process, disk power loss, or an
arbitrary network partition. Minority behavior is tested by stopping peers.

The separate `ProcessClusterCheck` starts three independent JVMs and forcibly
terminates the elected leader. It makes another 25 writes, waits for compaction,
and restarts the killed node with the same data. All three persisted maps and
applied indexes must agree, with a newer snapshot installed on the recovered
node. `verify.sh` runs this check from the packaged JAR, which is the artifact
the delivery workflow is configured to upload. This adds a real process-failure
case, while power-loss and arbitrary partition histories remain untested.

The local-first next steps are explained in
[scalability-and-delivery.md](scalability-and-delivery.md). Adding replicas alone
does not demonstrate scalable capacity or throughput; those claims need sharded
work distribution and measurements. Automatic JAR delivery is also distinct from
deploying it to a running environment.

### Known simplifications

- RPC uses Java serialization to keep the project dependency-free.
- There is no dynamic membership change.
- The state machine is an in-memory map restored from snapshots and applied log
  entries.
- There is no independent append-only WAL or LSM storage engine.
- There is no exactly-once request deduplication, ReadIndex, or leader-lease path.
- Real power-loss tests and network-partition histories checked by a
  linearizability checker have not been implemented.

Be ready to explain the difference between a write being accepted, replicated,
committed, applied, and returned to the client. Also explain why a timed-out write
is not necessarily absent, and why passing these tests does not make this a
production database.

