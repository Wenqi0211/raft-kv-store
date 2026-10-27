# Resume capability evidence

These capabilities were added or rechecked in this new local copy on
October 24, 2026. They are not evidence that missing code existed in July.
The source project's historical three-node performance numbers are not reused.

| Resume point | Implementation and actual verification | Evidence |
| --- | --- | --- |
| Java, gRPC, JUnit | Java 17 compilation; real unary gRPC/HTTP2 adapter; JUnit 5 | `pom.xml`, `GrpcRpcClient`, `RaftGrpcServer`, `reports/grpc-test.log` |
| Five nodes, tolerance of two failures | Five real-gRPC node endpoints; stop two nodes and continue writing; restore both and compare maps/sessions | `GrpcTransportTest`, final three JUnit cases passed, `reports/network-final/TEST-raftkv.rpc.GrpcTransportTest.xml` |
| Dropped/reordered messages, partitions and crash restart | Fault transport performs real handler execution through serialization, with injected loss/delay/partition and disk-backed node restart. Dedicated delayed-vote and minority-read tests cover reordering and partitions. | `SimulatedNetwork`, `RaftCapabilityTest` |
| Repeated randomized recovery checks | Final buffered source: 100 seeded five-node fault/recovery scenarios plus six focused capability tests, 106 cases passed in 294.486 seconds; 21 supporting cases also passed, totaling 127 with zero failures, errors or skips. | `reports/final-capability-100.log`, `reports/final-capability-100/summary.json`, archived XML and source manifest in that directory |
| Linearizable GET/PUT/DELETE | All operations use the majority-committed replicated log; minority old leader cannot acknowledge a new read. A recorded nine-operation concurrent GET/PUT/DELETE history is checked by exhaustive bounded backtracking respecting real-time precedence. | `RaftNode.submit/applyTick`, `Linearizability`, `RaftCapabilityTest` |
| Client ID and sequence retry deduplication | Latest response per client is replicated, stored in snapshots, transferred and replayed after restart. Duplicate DELETE returns the original result after another PUT without deleting that newer value. Older sequences and same identity with different payload are rejected. | `Command`, `ClientSession`, `KVStateMachine`, `Snapshot`, `GrpcTransportTest` |
| Persistence and snapshot recovery | Prior persistence/protocol corrections retained; original 23 internal checks passed inside three JUnit wrapper cases under Java 17. | `PersistentStore`, `RaftNode`, `reports/network-tests/TEST-raftkv.testing.LegacyRegressionJUnitTest.xml` |
| Five independent gRPC processes and usable CLI | Standalone node/client entry points tested in five JVMs. The checker forcibly killed the elected leader and another node; three survivors completed 25 writes. Both restarted from disk; all five maps, retry sessions and applied indices converged. | `reports/grpc-process-check.log`, `work/grpc-process-smoke-14882319750514686549/process-check.json` |
| Throughput, p99 and leader-stop recovery | Three passing 30-second PUT trials: 15.0–15.6 writes/s, client p99 406.5–420.5 ms, leader-stop to first confirmed PUT 1.271–1.786 s. Four clients, 256 keys, 64-byte values; 1,388 correct acknowledged writes and zero failed logical requests. These are local five-node/one-JVM measurements. | `reports/grpc-formal-run-1/` through `reports/grpc-formal-run-3/`, `reports/study-summary.json` |

The 100 scenarios are not 100 executions of the full JUnit suite. Seeds control
fault choices and probabilities, while OS/thread timing still varies. The
bounded nine-operation history is not an exhaustive proof over arbitrary
failure histories. Retry sessions keep the latest response and require one
logical request at a time per client; they are not an unlimited historical
exactly-once service. gRPC payloads remain Java-only within Protobuf envelopes.
Five nodes in the measurement share one JVM and one computer. Any time quoted
for stop/start failover must be distinguished from the separate forced-JVM-exit
correctness check.

## Storage optimization validation

The first 1,000-key 30-second load diagnostic was invalid because 4,016 of its
4,356 API attempts returned failure. It remains in `reports/grpc-baseline-30s/`
and is not used to claim successful throughput or a percentage improvement.

A separate persistence diagnostic retained identical 75,002-byte serialized
1,000-key snapshots. Unbuffered ObjectOutputStream issued 2,055 writes to the
channel stream; a 64 KiB buffer issued two. Warmup and fsync variability are
visible in the six interleaved trials, so their mixed timing medians are not
reported as a repeatable Raft speedup. Both modes retain directory preflight
force, temporary-file force, atomic replacement and post-replacement directory
force. Raw phases are in `reports/persistence-profile-baseline/phases.csv`.

Only this output buffer was added to PersistentStore. The original source and
hashes remain in `reports/baseline-source/`. Two additional tests verify exact
serialized-byte compatibility and preservation of the original durable state
after a failure following multiple buffer spills. Together with ten seeded
scenarios and six focused capability cases, all 18 tests passed in
`reports/buffered-capability-10.log`. The earlier archived 100-seed result
predates buffering. After the quiet performance study, the final 100-scenario
capability class and 21 supporting tests passed, totaling 127 selected JUnit
cases. Their actual XML and the unchanged tested-source SHA256 manifest are
retained in `reports/final-capability-100/`.

## Operational fixes and final measurement

The buffered 256-key diagnostic in `reports/grpc-run-1/` also failed: 431
successful and 2,124 failed logical requests. A separate process check overlapped
that run, so it was excluded from the formal study even before its failures
were evaluated. Both failed diagnostics used the fast simulation election
timers. Buffering alone did not establish a passing performance result.

The real gRPC client now has an explicit total request deadline, shrinking
per-RPC budgets and jittered backoff; retries retain the original command
identity and expose their attempt counters. Thirteen pure client tests passed.
The three real-gRPC integration cases also passed, including a committed DELETE
whose successful reply is hidden, followed by leader loss and another client's
replacement PUT before the original request is retried. That retry returns the
old DELETE result without deleting the new value. Evidence is in
`reports/client-retry-unit.log` and `reports/grpc-operational-check.log`.

Disk-backed gRPC nodes use an explicit 1,000–2,000 ms election range and 70 ms
heartbeats; fast simulation remains 260–500 ms. The storage forces, quorum rules
and 450 ms consensus RPC timeout were not weakened. New reports record timer
settings, client attempts, retry/backoff counts and Raft terms. The longer
election range also affects leader-stop recovery time; no old sub-second result
is reused. Three quiet sequential formal runs all passed with zero timed retries
and zero term increases. All five maps/session tables converged after recovery
in each run. See the README's actual measurement table and
`docs/grpc-measurement.md` for exact scope.
