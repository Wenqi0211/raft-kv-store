# RaftKV measured implementation

This is a new local copy of the existing Java Raft KV project. The earlier
projects and resume files have not been replaced. It adds the missing transport,
retry-deduplication and fault-testing capabilities before measuring them.

## Implemented behavior

- Leader election, replicated logs and majority commits in a fixed five-node
  cluster. Three survivors can continue committing after two nodes stop.
- Real gRPC over loopback HTTP/2, with a Protobuf envelope carrying the existing
  Java command/Raft DTOs. The original HTTP transport remains compatible.
- GET, PUT and DELETE all pass through the replicated log. Successful client
  operations wait for majority commit, state-machine application and a saved
  durable state. A minority cannot acknowledge a new read or write.
- Client IDs and monotonically increasing sequences prevent repeat execution.
  A retry of the latest operation returns its saved result; an older sequence or
  reuse of the same sequence for a different command is rejected. Each client
  uses one logical request at a time. Session results are replicated and saved
  with snapshots, including transfer to a lagging or restarted node.
- Term, vote, log and snapshot persistence before relevant RPC acknowledgments,
  using file force, atomic replacement and parent-directory synchronization.
- Snapshot compaction and follower recovery through AppendEntries or
  InstallSnapshot. The existing protocol and persistence corrections remain.
- A simulated network that drops, delays, reorders and partitions messages, with
  node stop/restart from separate state directories. Receiver execution is
  isolated from sender-thread cancellation.
- JUnit checks for durable retry deduplication, minority rejection, deliberately
  reordered votes, a bounded concurrent-operation linearizability history, and
  configurable seeded five-node loss/restart/snapshot-recovery scenarios.

## Build and check

Use JDK 17 or later and Maven 3.9+. The scripts honor `MAVEN_CMD` and
`MAVEN_REPO`. In this workspace they can also find the already-installed Maven
and dependency cache in the sibling Order project, so another installation is
not required here.

```bash
bash scripts/build.sh
bash scripts/test.sh
RAFT_SEED_COUNT=100 bash scripts/test.sh
bash scripts/verify.sh
```

The verification command runs JUnit, packages the original HTTP node entry
point, then uses that package in three independent JVMs. That separate check
forcibly terminates a leader process, makes writes on the surviving majority,
restarts the old node from disk and checks snapshot/state convergence.

`target/surefire-reports/` contains the latest JUnit results. Study-specific
reports under `reports/` retain separate runs, including failures. One hundred
seeded scenarios are not one hundred executions of the entire JUnit suite.

The final buffered-source verification passed 127 selected JUnit cases with
zero failures, errors or skips: 100 seeded five-node fault/recovery scenarios,
six focused capability cases and 21 storage/client/timer/configuration cases.
The capability class took 294.486 seconds. Exact XML, a summary, and an archive
of the 64 tested source/build/script files with matching SHA256 hashes are in
`reports/final-capability-100/`. For English-only source presentation, one
Unicode test literal is written with equivalent Java escapes. The original and
published archive hashes are recorded in that directory's
`publication-normalization.json`; measured results are unchanged.
The three real-gRPC integration cases passed
separately; their XML is retained in `reports/network-final/`.

## Measure five-node gRPC writes

```bash
bash scripts/grpc-benchmark.sh --duration 30 --clients 4 --keys 256 \
  --value-bytes 64 --seed 20261024 --report-dir reports/my-local-run-1
```

Repeat with new report directories and seeds. A run refuses to replace existing
raw samples or a summary. It preloads the complete keyspace, then measures a
closed-loop PUT workload. Clients own disjoint key ranges, allowing final values
to be checked without guessing concurrent commit order. A successful response's
returned value is checked against its requested value.

Every request has a raw CSV row. JSON reports include actual elapsed time,
successful and failed requests, successful write throughput, p99 for successful
and all requests, and the workload/environment. The measured phase includes the
initial client/channel setup and final in-flight completion; preloading and
recovery checks are outside that phase. Any failed or incorrect response makes
the run invalid rather than silently disappearing from the result.

After measurement, the harness stops the elected node, measures time to a
confirmed write on a replacement leader, stops a second node, verifies more
writes with only three survivors, then restarts both nodes and compares all five
complete KV maps and client-session maps. This failover measurement is explicit
node/server stop/start in one JVM, not a process kill. The independent-JVM check
above provides a separate forced-process-exit correctness test.

Aggregate real samples only after all three reports are valid:

```bash
python3 scripts/summarize-study.py reports/my-local-run-1 reports/my-local-run-2 \
  reports/my-local-run-3 --output reports/my-local-study-summary.json
```

The aggregator independently verifies request counts, throughput denominators,
nearest-rank p99 values and CSV hashes before summarizing the trials. It refuses
to overwrite an existing summary. See `docs/grpc-measurement.md` for details.

## Actual local measurements, October 24, 2026

Three sequential trials passed on Apple Silicon macOS 15.4.1 with JDK 17.0.17.
Each used five nodes in one JVM, four closed-loop clients, 256 preloaded keys,
64-byte values and a 30-second PUT phase. The election range was 1,000–2,000 ms
with 70 ms heartbeats; forced persistence remained enabled.

| Trial | Confirmed PUTs | Failed logical requests | PUT/s | Client p99 | Leader stop to confirmed PUT |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 453 | 0 | 15.0 | 410.3 ms | 1.541 s |
| 2 | 472 | 0 | 15.6 | 406.5 ms | 1.271 s |
| 3 | 463 | 0 | 15.3 | 420.5 ms | 1.786 s |

All 1,388 measured logical requests succeeded with matching values. The timed
phases recorded zero retry RPCs and no term increases. After each measured
phase, the separate recovery checks confirmed 25 writes with two nodes stopped
and equality of all five complete KV/session maps after both restarted. The
timing column measures stopping one leader and confirming the first write on
its replacement; the second node is stopped later and is not part of that
timing. The three recovery observations are not a failover percentile.

Raw CSV and JSON are retained in `reports/grpc-formal-run-1/` through
`reports/grpc-formal-run-3/`; the independently checked aggregate is
`reports/study-summary.json`. Earlier failed diagnostics remain available and
are excluded. These are small local durability-focused measurements, not
production-scale throughput or multi-machine/cloud performance results. The
whole-state persistence approach remains a known throughput limitation.

## Run a manual gRPC cluster

Use `scripts/grpc-node.sh` and `scripts/grpc-client.sh` for independently running
JVMs. See [the manual walkthrough](docs/manual-grpc-cluster.md) for five-terminal
startup, CRUD, two-node failure/restart and an explicit retry-deduplication demo.

## Measurement boundaries

The five measured nodes share one JVM and one computer, with separate durable
state directories and real gRPC channels. This is not a multi-host cloud
performance result. The bounded history checker examines a recorded nine-
operation concurrent history; it is not a formal proof over arbitrary histories.
Persistence still serializes the complete state rather than using an incremental
WAL/LSM storage engine. The gRPC DTO payload is Java-only and the default local
transport is plaintext; no public deployment or production traffic is claimed.

Local performance values belong to the exact source, JDK, workload and reports
that produced them. Historical three-node numbers from the earlier project are
not reused for this version.
