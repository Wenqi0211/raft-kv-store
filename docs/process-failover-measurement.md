# Independent-process failure measurement

This harness keeps four application clients active while force-killing a
last-observed leader JVM. It measures client retry behavior across an actual
process exit, rather than waiting for a replacement leader before starting a
write. The original single-JVM benchmark and five-process smoke check remain
available; their workloads and timing definitions are different.

## Reproduce

Use JDK 17+, Maven 3.9+, Bash, Python 3, and permission to bind local sockets:

```bash
bash scripts/grpc-process-benchmark.sh --duration 20 --kill-after 5 \
  --clients 4 --keys 64 --value-bytes 64 --seed 20261029 \
  --report-dir reports/my-process-run
python3 scripts/verify-process-study.py reports/my-process-run \
  --output reports/my-process-run/verified.json
python3 scripts/test_verify_process_study.py -v
```

`MAVEN_CMD` and `MAVEN_REPO` select the Maven executable and dependency cache.
Each run requires a new report directory; even an existing empty directory is
rejected before any process starts. The verifier also refuses to overwrite its
output. Node state and original stdout/stderr stay in the local report's
`data/` directory and are ignored by Git. JSON/CSV evidence contains generated
workload data and process IDs, without local account paths.

The five child JVMs use separate durable directories, 32 MiB initial/256 MiB
maximum heaps, and two reported active processors per child. They communicate
over real plaintext loopback gRPC. The driver reserves distinct ephemeral ports
at selection time, then releases them before child startup; another local
listener can still win that handoff race. A binding failure invalidates the run
and retains node logs. This is a local test harness, not a multi-host launcher.

## Workload and fault sequence

1. Start five node JVMs, preload all generated keys plus two canaries, and
   compare all durable KV/session maps before measurement. Snapshot threshold
   is 10 applied entries. Election timeouts remain 1,000–2,000 ms with 70 ms
   heartbeats; forced persistence remains enabled.
2. Run sequential, closed-loop PUT clients with disjoint key subsets. Each
   measured PUT has a 10-second logical budget and a 6-second RPC cap. Redirects,
   retries, and backoff are included in its latency.
3. At the requested fault time, a separate client issues DELETE on a prepared
   key. A test transport receives the committed response but withholds it from
   that client. Before killing its serving JVM, the harness requires matching
   response leader metadata and a fresh running/LEADER status with a positive
   term. A known step-down or inconsistent observation invalidates the trial.
   The status and observation timestamp are retained. A leader can still change
   between observation and OS termination, so the metric names the
   **last-observed leader**.
4. Invoke `Process.destroyForcibly`, wait for that process to terminate, and
   start a replacement PUT through a normal client using the original endpoint
   list. That client discovers a replacement without status polling. Once the
   replacement PUT succeeds, inject one `IOException` to discard the earlier
   DELETE reply. The original DELETE client then retries the exact request and
   must receive its cached old result while preserving the newer value. The
   DELETE fault probe has a 20-second logical budget. Failed probe/control
   operations propagate their original failure; they are not disguised as the
   intentional retryable reply loss.
5. After traffic completes, require all four survivors to compact beyond the
   stopped node's durable applied index. Restart its JVM from the same directory
   and require complete equality of all five KV/session maps, sufficient applied
   indexes, and an advanced snapshot on the restarted node. Replicated canary
   reads are followed by another complete convergence check.

The reply fault is deliberately injected **after receipt by the test transport**;
it does not claim that the network actually lost a packet. A timed-out operation
has an unknown outcome and invalidates a run even if it later commits.

## Evidence and interpretation

- `requests.csv`: every measured PUT, with start/latency nanoseconds, response
  status/message, correctness, RPC/retry counts, and backoff time.
- `events.csv`: leader observation, kill invocation, process termination,
  replacement PUT start/confirmation, DELETE retry confirmation, restart, and
  complete convergence, on the same monotonic clock as request samples.
- `states.csv`: all five final process IDs, applied/snapshot indexes, key/session
  counts, and matching SHA-256 fingerprints. The live check compares complete
  maps; fingerprints provide compact retained evidence.
- `summary.json`: environment, workload, budgets, outcomes, and timing definitions.
  Exceptions retain `valid: false` plus partial requests/events. The five-process
  fixture owns cleanup; process launch/registration and shutdown share a lock,
  and starts after shutdown are rejected.

Throughput divides confirmed measured PUTs by actual monotonic elapsed time,
including client setup, fault-probe completion, and final in-flight completion.
The separate DELETE/replacement probe is excluded from PUT counts. Preload and
restart validation are outside the timed workload. Kill-to-replacement time
starts at forced-termination invocation and ends at the confirmed PUT that began
after process termination. Restart-to-convergence includes the final canary reads
and their replicated session convergence.

The verifier independently recomputes counts, key ownership, elapsed-time
denominators, nearest-rank p50/p99, retry/backoff counters, traffic on both sides
of the kill, event order and intervals, leader evidence, restarted process
identity, snapshot advancement, and all five state fingerprints. It reconstructs
the expected final KV map from preload parameters and raw successful requests,
then checks its fingerprint against every node. It records
hashes of its four input files. Invalid runs, inconsistent workloads, and duplicate
trial directories cannot enter a passing aggregate. Ten regression checks
replay retained real evidence and reject deliberate metric/evidence corruption.

## Observed failure and targeted election fix

The strict-format attempts on October 27, 2026 used macOS 15.4.1,
Apple Silicon, JDK 17.0.17, and the command above, followed by seed `20261030`.

| Attempt | Seed | Outcome | Measured PUTs | Kill to replacement PUT |
| --- | ---: | --- | ---: | ---: |
| [4](../reports/grpc-process-failover-trial-4/) | 20261029 | Passed all checks | 311/311 | 1.136 s |
| [5](../reports/grpc-process-failover-trial-5/) | 20261030 | Invalid: replacement PUT exceeded its 10-second budget | See retained partial CSV | No confirmed replacement |

Attempt 4 recorded 15.46 PUT/s and 1,232.3 ms client p99 **including the forced
process failure**; four measured logical PUTs retried. Its
[verified report](../reports/grpc-process-failover-trial-4/verified.json) is one
passing observation, not an availability rate or a failover percentile. The
failed attempt remains visible and these pre-fix observations are not pooled
with measurements after the election fix.

Earlier development [attempt 3](../reports/grpc-process-failover-trial-3/)
also failed with seed `20261029`: four measured PUTs exhausted their 10-second
budgets while survivor terms rose from 1 to 11. Its replacement PUT never
confirmed or appeared in the retained log/session evidence. The probe originally
wrapped that timeout in a retryable exception, obscuring the original error until
a later GET returned no replacement. The harness now reports the original probe
failure directly. The retained [diagnosis](../reports/grpc-process-failover-trial-3/diagnosis.json)
and sanitized node output explain that failure. Earlier passing development
attempts 1–2 are retained separately and are not accepted by the strict verifier.

The retained [vote/log metadata](../reports/grpc-process-failover-trial-5/election-log-evidence.csv)
show a concrete stale-candidate condition in attempt 5: candidate `n1` and voter
`n5` ended term 9 with last index 153, while `n3` and `n4` had index 154 and had
not granted a vote. All those last entries were from term 1. The shorter-log
candidate could obtain only two of five votes. The implementation nevertheless
reset the newer-log voters' election deadlines when rejecting each higher-term
request, postponing their chance to run.

`handleRequestVote` now retains the prior election deadline on a higher-term
step-down until a vote is actually granted. Term persistence, vote clearing,
step-down, and pending-request failure still happen; a granted vote and current
leader contact still reset the deadline. This matches the follower election
condition in [the Raft paper, Figure 2](https://raft.github.io/raft.pdf).

Three deterministic tests exercise repeated stale-candidate requests, durable
step-down with an unchanged deadline, and timer resets for granted votes and
heartbeats. Two failed against the original core and all three passed after the
fix; the [before-fix evidence](../reports/election-vote-timer-before-fix/) is
retained. The failed process observations motivated the regression; the
deterministic test establishes the specific timer defect without inferring that
every delay has the same cause.

After the fix, the two originally failing seeds were replayed with unchanged
workload, election timing, heartbeat, and client budgets:

| Replay | Seed | Confirmed PUTs | PUT/s | Client p99 including crash | Kill to replacement PUT |
| --- | ---: | ---: | ---: | ---: | ---: |
| [6](../reports/grpc-process-failover-trial-6/) | 20261029 | 336/336 | 16.71 | 1,546.5 ms | 1.449 s |
| [7](../reports/grpc-process-failover-trial-7/) | 20261030 | 327/327 | 16.18 | 1,484.9 ms | 1.458 s |

Both passed full snapshot/deduplication convergence. The independent
[two-replay aggregate](../reports/process-failover-study.json) verifies 663/663
measured PUTs, 16.45 pooled PUT/s, and 1,485.0 ms pooled client p99. Four logical
PUTs per replay retried; the separate DELETE/replacement probe is outside those
counts. The two kill-to-confirmation observations are not a percentile.

Repeating a seed does not reproduce OS scheduling or storage latency. Local
replays establish neither multi-host availability, a general failover bound, nor
power-loss durability.
