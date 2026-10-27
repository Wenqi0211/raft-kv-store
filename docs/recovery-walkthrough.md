# Understanding RaftKV Through a Recovery Failure

This walkthrough describes the actual code and regression tests in this revision.
Start with a failure, identify the rule it violates, and then examine how the
tests demonstrate that the fix works.

## Six Terms to Know First

Think of the log as a notebook of operations, with numbered pages.

| Term in the code | How to understand it |
|---|---|
| `index` | The position of an operation record, such as record 80 |
| `term` | The election round number, which helps identify old messages |
| `commitIndex` | The last record confirmed as safe to execute |
| `lastApplied` | The last record actually executed |
| `snapshot` | The complete data state saved after execution reaches a particular record |
| `nextIndex` | The next record the leader plans to send to a particular follower |

Being confirmed as safe to execute and having already executed are separate
states. When a majority has just acknowledged an operation, the background thread
may not yet have applied it to the Map, so these two progress indexes can differ.

## 1. Why a Lagging Node Cannot Receive a Snapshot

Start three nodes, then temporarily stop one follower. The remaining two nodes
write 80 records and compact the old log into a snapshot. Next, restart the leader
so the cluster elects a new one, then bring back the stopped follower.

The new leader no longer has the complete old log, so it cannot catch the follower
up from the first record. It needs to send a snapshot, effectively saying:
"Here is the complete ledger after record 80 was executed. Take this first, then
continue recording the later operations."

The original code sent a snapshot only when `nextIndex <= snapshotIndex`, but
also limited `nextIndex` to a minimum of `snapshotIndex + 1` when replication
failed. This is like a door that requires you to stand below step 80 to enter,
while another rule prevents you from going below step 81.

After the fix, failure backoff can cross the snapshot boundary. On the next
replication attempt, the leader sees that the old log has been compacted and
sends a snapshot instead. Backoff still cannot retreat past the progress already
confirmed as matching during the current leadership term, so it does not erase
real progress.

Why must the test elect a new leader? The original leader might still remember
the follower's much earlier progress. If the follower were simply restarted,
that leader would already send it a snapshot, hiding the bug.

Relevant code: `RaftNode.replicatePeer`.
Relevant test: `ClusterIntegrationTest.snapshotCatchup`.
The test checks the complete Map and applied progress on all three nodes, and
verifies that the recovering node actually installed a snapshot.

## 2. Why a Follower Cannot Simply Trust Its Own Log Suffix

Suppose a follower has log records 1-100, but the new leader has only established
that records 1-64 match in this request. Records 65-100 may have been left by the
old leader.

Even if the new leader reports that it has committed through record 100, the
follower can initially commit only through record 64:

```text
new commit boundary = min(leader's commit boundary, last record matched by this request)
```

The original implementation used the last local record as its upper bound,
potentially executing an unverified suffix too early. The fix uses `match`, the
boundary matched by the current request.

Also, when the previous-record check fails, the follower first rejects that
probe without immediately deleting its log. It may delete the old uncommitted
suffix from the conflict position only when it receives an actual conflicting
new record.

Relevant tests: `appendDoesNotCommitUnmatchedSuffix` and
`mismatchedPreviousEntryDoesNotDeleteLog`.
The first test checks more than index values: it actually executes the committed
prefix and verifies that the incorrect suffix does not enter the Map.

## 3. Why Rejecting a Vote Still Requires Persisting the Term

A node in election term 4 receives a request for term 5. Even if it considers the
requester's log too old to grant a vote, it has already learned that the current
term is now 5.

If that fact stays only in memory, a restart makes the node believe it is in
term 4 again, and it may accept messages that should be stale.

The fix centralizes persistence of higher terms in `becomeFollowerLocked`,
ensuring that early returns for rejected votes, empty heartbeats, and rejected
appends also persist the term. Stepping down within the same term retains the
vote already cast; becoming a follower must not allow the node to vote again.

The tests read the real state file and reconstruct the node, verifying that it
still rejects old-term requests after a restart.

## 4. Why Waiting for a Log Position Alone Is Not Enough

Client A's request is temporarily at log position 10 and has not yet received a
majority acknowledgment. Leadership changes, and the new leader writes a
different request, B, at the same position.

If the code remembers only that A is waiting for position 10, executing B might
incorrectly tell A: "Your request succeeded."

After the fix, a pending request records its position, term, and request ID;
execution must match the same request. When leadership is lost or the record is
overwritten, the old request stops waiting and returns an unknown outcome.

An unknown outcome does not mean that the write definitely did not happen. A
request that timed out may still commit later. This project does not deduplicate
requests across restarts, so it cannot claim exactly-once execution.

Relevant test: `overwrittenRequestCannotSucceed`.
The test leaves the original request waiting, replaces it with a different
request from a new leader, executes that request, and verifies that the original
client does not receive a success response.

## 5. Why Installing a Snapshot Requires Checking the Boundary

A snapshot describes the state after execution reaches a particular position.
Before retaining the old suffix, the node must confirm that its local record at
that position has the same term. Only a matching position and term establish
that they belong to the same history.

If the boundary does not match, the node discards the incompatible uncommitted
suffix. If the conflict reaches a committed record, it rejects the snapshot.
A node that has already applied later records must also avoid rolling its state
back when it receives a delayed older snapshot.

The corresponding tests cover retaining the suffix, discarding the suffix,
rejecting a conflicting committed boundary, and rejecting rollback to older
state. They also append or execute subsequent records to check that the node
remains usable.

## 6. Why Writing the File Also Requires Force and Atomic Replacement

Writing the file involves three layers:

1. The Java object stream may still hold buffered data, so call `flush` first.
2. The operating system may temporarily keep data in its cache, so call
   `force(true)` on the temporary file.
3. Install the new file with an atomic replacement, then synchronize its
   directory to persist the filename replacement.

These steps aim to avoid leaving a partially written state file during normal
saves. Temporary files have unique names and are cleaned up on failure. A corrupt
state file produces an explicit error instead of silently becoming an empty
database.

A failed save introduces another easily missed problem: memory may already have
changed. If the node continues acknowledging later requests, the leader may
count data whose persistence has not been confirmed as a valid replica. A failed
save therefore now stops scheduling and rejects all new Raft RPCs until the node
is reconstructed from disk.

Relevant tests: `PersistentStoreTest` and
`failedPersistenceStopsProtocolResponses`.
Fault injection replaces the target file with a nonempty directory that cannot
be replaced, causing a real write failure. The tests then verify that the node
cannot continue acknowledging data held only in memory.

These calls still depend on the filesystem and device. The current tests do not
cut physical power or synchronize every ancestor of a newly created data
directory. Platforms without directory synchronization or atomic replacement
support fail explicitly. Whole-state serialization also has a substantial write
cost, so this version does not claim high throughput.

## How to Reproduce and Read the Code

Run from the project root:

```bash
bash scripts/test.sh
bash scripts/benchmark.sh --duration 1 --clients 2 --keys 100 --snapshot-threshold 10
```

The tests have three layers:

- Real-file tests check saving, loading, corruption, and failure handling.
- Deterministic protocol tests construct explicit log histories and send RPCs
  directly. They use a small amount of reflection to drive the internal state
  machine, keeping random election timing from affecting assertions.
- HTTP cluster tests start nodes in one JVM, communicate over real HTTP, and
  verify the recovered data.

The HTTP tests use object stop/start operations. They do not kill independent
processes or simulate arbitrary network partitions. The majority-loss test
demonstrates that a minority cannot successfully acknowledge a new write, but it
does not replace comprehensive network-failure testing. GitHub Actions is
configured for JDK 17/21 checks; actual hosted run results determine whether those
checks pass in that environment.

Read `snapshotCatchup` first, then `replicatePeer`, and finally explain to
yourself why successful startup does not prove successful data recovery.

The protocol rules come from Figure 2 and Section 7 of the
[Raft paper](https://raft.github.io/raft.pdf). Implementing and testing these rules
still does not let a finite set of cases prove that every concurrent execution
is correct.

The subsequent first engineering step added `ProcessClusterCheck`, which starts
three independent JVMs, forcibly terminates the leader process, and verifies
recovery. This and the single-JVM tests above provide two different levels of
checking. See [scalability-and-delivery.md](scalability-and-delivery.md) for the
complete packaging and verification flow and the later expansion roadmap.
