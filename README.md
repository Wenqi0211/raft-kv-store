# RaftKV

A Java 17 distributed key-value store implementing Raft consensus, durable
recovery, and client retry deduplication. Its fixed five-node configuration can
commit with three survivors after two nodes stop. The project includes real
gRPC and HTTP transports, fault-injection tests, independent-process recovery
checks, and reproducible local measurements.

## Architecture and guarantees

- **Consensus:** leader election, replicated logs, and majority commits.
  `GET`, `PUT`, and `DELETE` all pass through the replicated log. Successful new
  operations wait for majority commit, state-machine application, and durable
  state persistence; a minority cannot acknowledge a new read or write.
- **Durability:** term, vote, log, and snapshot state are persisted before the
  relevant RPC acknowledgments. Saves force file contents, atomically replace
  the state file, and synchronize the parent directory.
- **Recovery:** snapshot compaction bounds the retained log prefix. Followers
  catch up through `AppendEntries` or `InstallSnapshot`, and restarted nodes
  restore their own durable state directories.
- **Retry deduplication:** client IDs and increasing sequence numbers identify
  logical requests. A retry of the latest request returns its saved result;
  older sequences and conflicting reuse of a sequence are rejected. Each client
  uses one logical request at a time. Session results are replicated, persisted,
  and transferred with snapshots.
- **Transport:** real unary gRPC over loopback HTTP/2 with Protobuf envelopes
  carrying serialized Java DTOs. The HTTP transport remains available.

Core implementation: [RaftNode](src/main/java/raftkv/raft/RaftNode.java),
[PersistentStore](src/main/java/raftkv/raft/PersistentStore.java),
[KVStateMachine](src/main/java/raftkv/kv/KVStateMachine.java).

## Quick start

Prerequisites: JDK 17+, Maven 3.9+, and Bash; Python 3 for report aggregation.
Run commands from the project root. `MAVEN_CMD` selects a Maven executable;
`MAVEN_REPO` selects its dependency cache.

Build and prepare the gRPC runtime classpath:

```bash
bash scripts/build.sh
bash scripts/maven.sh -DskipTests \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt -DincludeScope=runtime
```

Start one node per terminal:

```bash
bash scripts/grpc-node.sh --id n1 --data-dir work/manual-grpc/n1
bash scripts/grpc-node.sh --id n2 --data-dir work/manual-grpc/n2
bash scripts/grpc-node.sh --id n3 --data-dir work/manual-grpc/n3
bash scripts/grpc-node.sh --id n4 --data-dir work/manual-grpc/n4
bash scripts/grpc-node.sh --id n5 --data-dir work/manual-grpc/n5
```

The default endpoints are `127.0.0.1:8101` through `127.0.0.1:8105`. From another
terminal:

```bash
bash scripts/grpc-client.sh status
bash scripts/grpc-client.sh put example raft
bash scripts/grpc-client.sh get example
bash scripts/grpc-client.sh delete example
```

The [manual cluster guide](docs/manual-grpc-cluster.md) covers configuration,
two-node failure and restart, and explicit retry deduplication.

## Verification

```bash
bash scripts/test.sh                         # JUnit; three seeded scenarios by default
RAFT_SEED_COUNT=100 bash scripts/test.sh      # Increase to 100 seeded scenarios
bash scripts/grpc-process-smoke.sh           # Five independent gRPC node JVMs
bash scripts/verify.sh                       # JUnit, HTTP JAR packaging, three-JVM recovery
```

The retained [selected run](reports/final-capability-100/summary.json) passed
**127 JUnit cases**: 100 seeded five-node fault/recovery scenarios, six focused
capability cases, and 21 storage/client/timer/configuration cases, with zero
failures, errors, or skips. The [three-case gRPC transport suite](reports/network-final/)
passed separately, covering two network integration cases and one schema check.
Seed count controls scenarios, not full-suite executions.

- **Simulated faults:** message loss, delay, reordering, partitions, node
  stop/restart, and snapshot recovery using separate durable directories.
  Focused tests cover minority rejection, durable deduplication, reordered
  votes, and a bounded nine-operation concurrent linearizability history.
- **Real gRPC:** loopback integration checks exercise CRUD, two-node failure,
  restart, and retry behavior after a committed reply is lost.
- **Process failures:** the gRPC process check forcibly terminates two of five
  node JVMs, confirms 25 writes with three survivors, restarts both nodes, and
  compares all five durable KV/session maps. The HTTP check in `verify.sh`
  separately tests a forced leader exit and recovery using the packaged JAR.

## Measured results

Three sequential local trials on October 24, 2026 used Apple Silicon,
macOS 15.4.1, and JDK 17.0.17. Each ran five nodes **in one JVM** over real gRPC,
with separate durable directories, four closed-loop clients, 256 preloaded keys,
64-byte values, and a requested 30-second PUT phase. Forced persistence remained
enabled; election timeouts were 1,000–2,000 ms with 70 ms heartbeats.

| Trial | Confirmed PUTs | Failed requests | PUT/s | Client p99 | Leader stop to confirmed PUT |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 453 | 0 | 15.0 | 410.3 ms | 1.541 s |
| 2 | 472 | 0 | 15.6 | 406.5 ms | 1.271 s |
| 3 | 463 | 0 | 15.3 | 420.5 ms | 1.786 s |

All **1,388** measured requests succeeded with matching values; the timed phases
recorded zero retry RPCs and no term increases. Throughput uses actual elapsed
time, including client/channel setup and final in-flight completion. Latency
measures client API calls, including redirects or retries. Preload and recovery
checks are outside the timed phase.

After each trial, recovery checks confirmed 25 writes with two nodes stopped and
equality of all five complete KV/session maps after restart. The timing column
measures an explicit leader/server stop through the first confirmed replacement
write in the same JVM. These three observations are not a failover percentile;
forced process exits are tested separately.

Evidence: [trial 1](reports/grpc-formal-run-1/),
[trial 2](reports/grpc-formal-run-2/), [trial 3](reports/grpc-formal-run-3/), and
the [verified aggregate](reports/study-summary.json).

## Reproduce the benchmark

```bash
bash scripts/grpc-benchmark.sh --duration 30 --clients 4 --keys 256 \
  --value-bytes 64 --seed 20261024 --report-dir reports/my-local-run-1
```

Repeat with seeds `20261025` and `20261026` and new report directories, then:

```bash
python3 scripts/summarize-study.py reports/my-local-run-1 reports/my-local-run-2 \
  reports/my-local-run-3 --output reports/my-local-study-summary.json
```

The harness records every request in CSV and saves workload, environment,
latency, throughput, retry, and recovery results in JSON. Failed or incorrect
responses invalidate a run. The aggregator checks counts, elapsed-time
denominators, and nearest-rank p99 values, and records input hashes when
summarizing valid trials. Both tools preserve existing reports. See the
[measurement guide](docs/grpc-measurement.md) for methodology and runtime options.

## Technical boundaries

Measurements cover a local durability-focused workload on one machine, without
multi-host or production-scale validation. Whole-state serialization limits
throughput; storage does not use an incremental WAL/LSM engine. Membership is
fixed within one Raft group. The gRPC payload is Java-only and transport is
plaintext. The bounded history check is not a formal proof over arbitrary
histories, and process-crash checks do not establish power-loss durability.
