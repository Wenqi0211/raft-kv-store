# gRPC measurement and its scope

The added transport is real unary gRPC over HTTP/2 using protobuf `BytesValue`
envelopes. Envelopes contain the existing serialized Java DTOs so the improved
Raft core can retain its protocol regression checks. This is a Java-only,
trusted-loopback protocol. It has no TLS, authentication, public-service
hardening, or cross-language typed protobuf messages.

Run a fresh measurement with JDK 17 or later and Maven:

```bash
bash scripts/grpc-benchmark.sh --duration 30 --clients 4 --keys 256 \
  --value-bytes 64 --seed 20261024 --report-dir reports/my-local-run-1
```

Repeat with seeds 20261025 and 20261026 in separate report directories. Existing
reports are never replaced by this harness. `RAFTKV_MAVEN` may point to a local
Maven binary; `RAFTKV_MAVEN_REPOSITORY` can select a writable dependency cache.

Each run starts five independently persisted Raft node objects in one JVM, each
with its own Netty gRPC server and outgoing channel pool. Their RPCs use real
loopback sockets. The example preloads all 256 keys with 64-byte ASCII values before the
measured phase; preloading, CRUD checks, failover and restart are excluded from
throughput and latency statistics.

The measured phase runs four sequential, closed-loop clients for a requested
30 seconds. Every client owns a disjoint key subset so its final successful PUT
for each key is unambiguous. Latency measures the entire client API call,
including client redirects/retries. Successful throughput uses observed
monotonic elapsed time through the last in-flight completion, rather than the
requested duration. Both successful-request and all-request p99 are reported;
all failures remain in the CSV. A failed response or wrong acknowledged value
marks the study invalid instead of being silently excluded.

`requests.csv` contains every measured request's sequence, client, key,
monotonic start offset, latency, success, response status, and message.
It also records application RPC attempts, retry attempts and time spent in retry
backoff for each logical request. A request succeeds only if its acknowledged
value is correct. Retried successes remain in end-to-end latency; exhausted
requests remain failures. The JSON reports the same counters and the term
before and after the measured phase so leader churn is visible.
`summary.json` records environment, workload, denominators and validation.

After the timed phase the harness checks sampled GETs, stable canaries, and
equality of all five complete local KV maps and deduplication session maps. It
then explicitly stops the elected leader, waits for a unique replacement, and
measures time from the stop invocation through the first confirmed PUT using
that replacement endpoint. This includes 20 ms polling and is not a complete
end-user reconnect/discovery benchmark.

It writes 81 values while that leader is offline, stops one additional follower,
and confirms another 25 writes with only three of five nodes available. It
restarts both stopped nodes from their existing directories and checks all five
maps, session tables, and applied indexes. These failures use Java node/server
stop and restart in one JVM. They do not kill the JVM, cut power, simulate
cross-machine networking, or prove production availability. Independent-process
crash checks and bounded concurrent-history tests are separate evidence.

Whole-state serialization, forced local writes, storage hardware, JVM warmup,
and other programs on the machine affect results. Report the tested workload
and local environment with any resume number. Never reuse the old three-node
prototype's performance numbers for this version.

## Progress and bounded runtime

The harness prints its current phase and preload progress every 100 keys or
10 seconds. `--max-runtime-seconds` limits the owned run (default at least
600 seconds). Expiry interrupts the benchmark thread, records an invalid
summary with its phase, retains partial measured CSV rows, and stops its own
node objects. Cleanup still runs after a time budget expires. An invalid or
interrupted run must never be included in the passing study aggregate.

## Client deadlines and operational election timing

The gRPC client has a 10-second budget per logical request. Each individual RPC
uses the smaller of its remaining budget and a 6-second cap. After a failed
round it uses a capped 20–200 ms exponential backoff with jitter. Retries retain
the same request ID, client ID, sequence and payload; they cannot turn an
ambiguous DELETE into a new operation. A timed-out request has an unknown
outcome and is reported as failure, even if an earlier attempt later commits.
The 6-second RPC cap permits the existing server's 5-second commit wait to
finish; it does not change the 450 ms consensus RPC timeout.

The gRPC command-line nodes and measurement default to election timeouts
randomized from 1,000 ms inclusive to 2,000 ms exclusive, with 70 ms heartbeats.
All three values are explicit options: `--election-min-ms`,
`--election-max-ms`, and `--heartbeat-ms`, and the report records their values.
These settings trade a longer failure-detection delay for fewer unnecessary
elections under synchronous durable storage. They leave quorum, commit rules,
fsync and atomic replacement unchanged. Fast fault-simulation tests retain
their 260–500 ms election range and 70 ms heartbeat.

This change follows the Raft paper's requirement that broadcast time be much
smaller than election timeout, accounting for RPC processing and persistent
storage: [Raft paper, section 5.6](https://raft.github.io/raft.pdf).
The two earlier load diagnostics showed term increases and failed logical
requests with the fast simulation timers; the longer range is an operational
choice based on that evidence, not a measured broadcast-time formula. Any new
failover result must use the actual reported election settings. Earlier invalid
reports are retained and excluded from the aggregate.
