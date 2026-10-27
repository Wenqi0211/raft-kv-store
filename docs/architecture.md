# RaftKV Architecture

## Components

RaftKV has four main layers.

1. HTTP RPC transports Raft messages and client commands.
2. `RaftNode` owns leader election, log replication, commitment, snapshots, and
   durable state.
3. `KVStateMachine` applies committed commands in order.
4. Client and benchmark tools exercise the system as separate HTTP callers.

## Consensus Flow

Client writes are accepted only by the leader. Followers return a leader hint
when they know one. The leader appends the command to its local log, replicates
it to followers with AppendEntries, then commits when a majority has stored the
entry. Only committed entries are applied to the KV map.

A follower advances its commit index only through the prefix confirmed by the
current AppendEntries request. A failed previous-entry check rejects the probe;
it does not truncate the follower's log. Truncation happens only when an actual
incoming entry conflicts with an uncommitted local entry.

Only one replication worker per follower is active at a time. Successful replies
advance confirmed progress monotonically; a rejection can back off into the
leader's compacted prefix so the next attempt uses a snapshot.

`GET` is represented as a log command. This is intentionally conservative:
it avoids stale reads without adding a separate read-index or lease path.

## Persistence

Each node stores a Java-serialized `PersistentState` file under its data
directory. It includes:

- current term
- voted-for candidate
- compacted snapshot metadata
- log suffix after the snapshot
- commit index
- last applied index
- latest snapshot contents

On restart, the node restores the snapshot and replays already-applied log
entries after the snapshot.

Every higher-term transition is saved before replying, including a rejected vote
or a heartbeat with no entries. Stepping down in the same term preserves the
vote already cast in that term.

Saving writes a unique temporary file in the same directory, flushes the object
stream, forces file contents, atomically replaces the state file, and forces the
directory. Unsupported synchronization/atomic replacement fails explicitly.
After a save failure the node rejects all Raft RPCs and client submissions until
a fresh node is constructed from durable state. Corrupt input is an error, not
an excuse to silently start an empty node.

This remains whole-state serialization. File forcing is not a tested guarantee
against every power-loss scenario or storage device behavior.

## Snapshot and Log Compaction

After a node applies enough entries past the current snapshot index, it captures
the KV map as a snapshot. Entries at or before the snapshot index are removed
from the in-memory and persisted log.

If a follower is too far behind and the leader no longer has the needed prefix,
the leader sends InstallSnapshot. The follower restores the KV map from that
snapshot and continues receiving normal AppendEntries after the snapshot index.

The follower retains its later entries only if its entry at the snapshot boundary
has the same index and term. Otherwise the incompatible suffix is discarded.
A delayed snapshot cannot roll back state already applied by the follower.

## Client Results

A waiting client request is associated with its log index, term, and request ID.
Only applying that same committed command can satisfy it successfully. A leader
that steps down or truncates the command fails the waiter with an unknown-outcome
response. The command might still commit later, so a retry is not exactly-once.

## Fault Recovery

The benchmark stops the current leader's HTTP server and node object, waits for
the remaining nodes to elect a new leader, and measures the time until a write
succeeds. It makes 80 additional writes before restarting the stopped node using
the same data directory. Stable known values, deleted keys, complete local maps,
and applied indices must agree before recovery is reported.

The automated tests also cover the stronger case where an offline follower
misses compaction and the active leader is restarted before the follower rejoins.
Without this re-election, an old replication cursor can accidentally hide a
broken snapshot fallback.

All these cluster checks use real HTTP in a single JVM. They do not inject
process kills, arbitrary network partitions, or power loss. See
[recovery-walkthrough.md](recovery-walkthrough.md) for the learning guide and
[the original Raft paper](https://raft.github.io/raft.pdf), Figure 2 and Section 7,
for the protocol rules behind the fixes.

## Independent-Process Verification and Delivery

`ProcessClusterCheck` adds a separate three-JVM check. It launches nodes using
the current classpath, forcibly terminates the elected leader, makes new writes
on the surviving majority, and waits for both survivors to compact past the
killed node's previous applied position. It restarts that node with its old
data directory and reconstructs each node's persisted state to compare complete
maps and applied/snapshot progress. All child processes are stopped on exit;
per-node logs and data remain in an isolated `work/process-smoke-*` directory.

`verify.sh` first runs regressions, then packages a Java 17-compatible JAR and
passes it as the process check's classpath. The parent verifier and child nodes
therefore exercise the same artifact that the delivery workflow uploads.

The independent-process check covers forced process termination, not power loss
or arbitrary network partitions. Delivery currently produces a verified artifact;
container deployment, metrics, routing, and multiple sharded Raft groups are
planned in [scalability-and-delivery.md](scalability-and-delivery.md).

