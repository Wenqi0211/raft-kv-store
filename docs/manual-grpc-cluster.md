# Run five independent gRPC nodes

Nodes default to 1,000–2,000 ms randomized election timeouts and 70 ms
heartbeats. Override these consistently across the fixed membership with
`--election-min-ms`, `--election-max-ms`, and `--heartbeat-ms` when storage or
network conditions require a different operational setting. Startup prints the
chosen values. Longer election timeouts also lengthen failure detection.

Use JDK 17 and the local Maven installation selected by `scripts/maven.sh`.
Build once before opening five terminal windows:

```bash
bash scripts/maven.sh -DskipTests compile \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt -DincludeScope=runtime
```

In five terminals, start one command per window:

```bash
bash scripts/grpc-node.sh --id n1 --data-dir work/manual-grpc/n1
bash scripts/grpc-node.sh --id n2 --data-dir work/manual-grpc/n2
bash scripts/grpc-node.sh --id n3 --data-dir work/manual-grpc/n3
bash scripts/grpc-node.sh --id n4 --data-dir work/manual-grpc/n4
bash scripts/grpc-node.sh --id n5 --data-dir work/manual-grpc/n5
```

Each is a separate JVM and durable state directory. Default fixed membership is
n1 through n5 at `grpc://127.0.0.1:8101` through port 8105. If those ports are
unavailable, pass the identical explicit `--cluster` list to every node. For
example, `--cluster 'n1=grpc://127.0.0.1:9101,n2=grpc://127.0.0.1:9102,...'` must
contain every actual member with its real URL, not a literal ellipsis. An
optional `--url` must match the selected member's URL. Changing the member list
is not a supported live reconfiguration; use a fresh cluster/state directory.

From a sixth terminal:

```bash
bash scripts/grpc-client.sh status
bash scripts/grpc-client.sh put course raft
bash scripts/grpc-client.sh get course
bash scripts/grpc-client.sh delete course
bash scripts/grpc-client.sh get course
```

The last GET prints `(null)`. Each successful new command goes through the
replicated log. Inspect status to identify the leader. Stop its terminal with
Ctrl-C; the other four nodes elect a replacement. Stop one additional follower;
three remaining nodes can still commit. Restart both with exactly their original
commands and state directories, then confirm that their applied/snapshot indices
catch up. With only two survivors, new commands must not be acknowledged.

A logical request can be retried across separate client invocations with an
explicit stable ID and sequence:

```bash
bash scripts/grpc-client.sh --client-id demo --sequence 1 put retry-key first
bash scripts/grpc-client.sh put retry-key newer
bash scripts/grpc-client.sh --client-id demo --sequence 1 put retry-key first
bash scripts/grpc-client.sh get retry-key
```

The duplicate returns `first` but the final GET remains `newer`, because the
first logical PUT is not executed again. New requests from `demo` must use a
larger sequence. Keep one logical request at a time per client. Using the same
ID/sequence for a different operation is rejected; an older sequence is rejected
rather than replayed. `--nodes` can select explicit `grpc://host:port` endpoints
before the command when not using the defaults.

These manual terminals are separate processes, while the reported performance
harness deliberately uses five nodes in one JVM. Do not describe their benchmark
numbers as independent-process or multi-host measurements.

## Automatic independent-process check

`bash scripts/grpc-process-smoke.sh` automates this flow with five independent
JVMs on random loopback ports, an actual standalone client process, two forced
process exits, 25 writes with three survivors and both nodes restarting from
their old state directories. It also checks cached DELETE retry behavior before
and after recovery and compares all five persisted KV/session maps. Every child
process is stopped at the end. Its stdout/stderr, PIDs and `process-check.json`
remain in a new `work/grpc-process-smoke-*` directory. This is a correctness
check, not a separate-process throughput measurement.
