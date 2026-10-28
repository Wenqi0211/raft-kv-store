package raftkv.tool;

import raftkv.client.GrpcKvClient;
import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.rpc.GrpcRpcClient;
import raftkv.rpc.RaftTransport;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/** Continuous client traffic across an actual leader JVM exit, with verifiable raw evidence. */
public final class GrpcProcessBenchmarkMain {
    private GrpcProcessBenchmarkMain() { }

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        // A nonempty directory can contain valuable evidence or stale durable state.
        // Require a new directory rather than silently reusing either.
        Files.createDirectories(config.report().getParent());
        Files.createDirectory(config.report());
        Map<String,Object> report = initialReport(config);
        List<Sample> samples = Collections.synchronizedList(new ArrayList<>());
        List<Event> events = new ArrayList<>();
        try (GrpcProcessCluster cluster = new GrpcProcessCluster(config.report().resolve("data"),
                config.seed(), config.maximumSeconds())) {
            Files.createDirectories(cluster.directory);
            run(config, cluster, report, samples, events);
            report.put("valid", true);
        } catch (Exception | AssertionError failure) {
            report.put("failure", failure.toString());
            throw failure;
        } finally {
            report.put("completed_at_utc", Instant.now().toString());
            writeSamples(config.report().resolve("requests.csv"), samples);
            writeEvents(config.report().resolve("events.csv"), events);
            writeJson(config.report().resolve("summary.json"), report);
        }
        System.out.printf("PASS: %s PUTs across a forced leader-process exit; %.2f PUT/s, p99 %.1f ms%n",
                report.get("successful_requests"), report.get("successful_writes_per_second"),
                report.get("latency_p99_success_ms"));
        System.out.println("Evidence: " + config.report());
    }

    private static void run(Config config, GrpcProcessCluster cluster, Map<String,Object> report,
                            List<Sample> samples, List<Event> events) throws Exception {
        cluster.announce("1/5", "Start five independent durable gRPC JVMs");
        for (GrpcProcessCluster.Child child : cluster.children) cluster.start(child);
        GrpcProcessCluster.Child initialLeader = cluster.awaitLeader();
        List<String> urls = new ArrayList<>();
        urls.add(initialLeader.url);
        cluster.children.stream().filter(child -> child != initialLeader).forEach(child -> urls.add(child.url));
        Map<String,String> expected = new ConcurrentHashMap<>();
        try (GrpcKvClient client = new GrpcKvClient(urls)) {
            for (int i = 0; i < config.keys(); i++) {
                cluster.checkProcesses();
                String key = "k" + i, value = value(i, config.valueBytes());
                require(client.put(key, value), value);
                expected.put(key, value);
            }
            require(client.put("retry-delete", "before-crash"), "before-crash");
            expected.put("retry-delete", "before-crash");
            require(client.put("canary", "stable"), "stable");
            expected.put("canary", "stable");
        }
        cluster.awaitState(expected, 0, Map.of());
        report.put("initial_leader", initialLeader.id);
        report.put("preload_in_timed_statistics", false);

        cluster.announce("2/5", "Run closed-loop PUT clients; discard a committed DELETE reply and kill its leader JVM");
        long started = System.nanoTime(), stopAt = started + TimeUnit.SECONDS.toNanos(config.duration());
        var pool = Executors.newFixedThreadPool(config.clients() + 1);
        AtomicInteger sequence = new AtomicInteger();
        CountDownLatch active = new CountDownLatch(config.clients());
        List<Future<?>> writers = new ArrayList<>();
        Fault fault;
        long elapsed;
        try {
            for (int worker = 0; worker < config.clients(); worker++) {
                final int clientNumber = worker;
                writers.add(pool.submit(() -> {
                    int local = 0, owned = (config.keys() - 1 - clientNumber) / config.clients() + 1;
                    try (GrpcKvClient client = new GrpcKvClient(urls, new GrpcRpcClient(),
                            Duration.ofSeconds(10), Duration.ofSeconds(6), config.seed() + clientNumber)) {
                        active.countDown();
                        while (System.nanoTime() < stopAt && !Thread.currentThread().isInterrupted()) {
                            int number = sequence.incrementAndGet();
                            String key = "k" + (clientNumber + config.clients() * (local++ % owned));
                            String value = value(number + config.keys(), config.valueBytes());
                            var before = client.stats();
                            long began = System.nanoTime();
                            KvRpc.ClientCommandResponse response = client.put(key, value);
                            long completed = System.nanoTime();
                            var after = client.stats();
                            boolean correct = response.success() && Objects.equals(response.value(), value);
                            samples.add(new Sample(number, clientNumber, key, began - started, completed - began,
                                    correct, response.success(), response.message(), after.rpcAttempts() - before.rpcAttempts(),
                                    after.retryRpcAttempts() - before.retryRpcAttempts(),
                                    after.backoffNanos() - before.backoffNanos()));
                            if (correct) expected.put(key, value);
                        }
                    }
                }));
            }
            Future<Fault> injection = pool.submit(() -> {
                if (!active.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Clients did not start");
                long wait = started + TimeUnit.SECONDS.toNanos(config.killAfter()) - System.nanoTime();
                if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                return injectLostReplyAndKill(cluster, urls, expected, started, events, config.seed(), report);
            });
            fault = injection.get(cluster.remaining(Duration.ofSeconds(config.maximumSeconds())).toMillis(), TimeUnit.MILLISECONDS);
            for (Future<?> writer : writers) {
                writer.get(cluster.remaining(Duration.ofSeconds(config.maximumSeconds())).toMillis(), TimeUnit.MILLISECONDS);
            }
            elapsed = System.nanoTime() - started;
        } finally {
            pool.shutdownNow();
            if (!pool.awaitTermination(12, TimeUnit.SECONDS)) throw new IllegalStateException("Workload did not terminate");
        }
        List<Sample> complete = List.copyOf(samples);
        long successful = complete.stream().filter(Sample::success).count();
        report.put("actual_elapsed_seconds", elapsed / 1e9);
        report.put("attempted_requests", complete.size());
        report.put("successful_requests", successful);
        report.put("failed_requests", complete.size() - successful);
        report.put("successful_writes_per_second", successful / (elapsed / 1e9));
        report.put("latency_p50_success_ms", percentile(complete, 50));
        report.put("latency_p99_success_ms", percentile(complete, 99));
        report.put("client_rpc_attempts", complete.stream().mapToLong(Sample::attempts).sum());
        report.put("client_retry_rpc_attempts", complete.stream().mapToLong(Sample::retries).sum());
        report.put("client_backoff_total_ms", complete.stream().mapToLong(Sample::backoff).sum() / 1e6);
        report.put("successful_requests_with_retry", complete.stream().filter(s -> s.success() && s.retries() > 0).count());
        report.put("kill_offset_nanos", fault.killOffset());
        report.put("killed_leader", fault.child().id);
        report.put("killed_leader_pid", fault.pid());
        report.put("pre_kill_leader_term", fault.term());
        report.put("pre_kill_leader_status_json", fault.status());
        report.put("leader_kill_to_confirmed_replacement_put_ms", fault.replacementPutMillis());
        report.put("discarded_committed_delete_replies", 1);
        report.put("delete_rpc_attempts", fault.deleteAttempts());
        report.put("delete_request_identity_preserved", true);
        report.put("delete_returned_original_value", true);
        report.put("replacement_survived_duplicate_delete", true);
        report.put("pre_kill_successful_requests", complete.stream().filter(s -> s.success()
                && s.start() + s.latency() < fault.killOffset()).count());
        report.put("post_kill_started_successful_requests", complete.stream().filter(s -> s.success()
                && s.start() >= fault.killOffset()).count());
        if (successful != complete.size() || successful == 0) throw new IllegalStateException("Workload had failed or incorrect PUTs");
        if (((Number) report.get("pre_kill_successful_requests")).longValue() == 0
                || ((Number) report.get("post_kill_started_successful_requests")).longValue() == 0) {
            throw new IllegalStateException("Need successful traffic before and after the actual kill");
        }

        cluster.announce("3/5", "Confirm the four survivors compact beyond the killed leader's durable applied index");
        long stoppedApplied = cluster.readState(fault.child()).applied();
        cluster.awaitSnapshotsBeyond(stoppedApplied);
        long target = cluster.readState(cluster.awaitLeader()).applied();
        cluster.awaitState(expected, target, Map.of());
        cluster.announce("4/5", "Restart the killed JVM from its existing directory and compare every KV/session map");
        long restarted = System.nanoTime();
        events.add(new Event("restart_invoked", restarted - started, fault.child().id, fault.pid()));
        cluster.start(fault.child());
        Map<String,GrpcProcessCluster.AppliedState> states = cluster.awaitState(expected, target,
                Map.of(fault.child().id, stoppedApplied));
        report.put("restarted_leader_pid", fault.child().process.pid());
        report.put("all_five_maps_and_sessions_match", true);
        report.put("restarted_node_snapshot_advanced", states.get(fault.child().id).snapshot() > stoppedApplied);
        report.put("verified_final_key_count", expected.size());
        report.put("verified_final_session_count", states.get(fault.child().id).sessions().size());
        report.put("verified_target_applied_index", target);
        report.put("killed_node_applied_index", stoppedApplied);
        report.put("restarted_node_snapshot_index", states.get(fault.child().id).snapshot());
        try (GrpcKvClient client = new GrpcKvClient(urls)) {
            require(client.get("retry-delete"), "after-crash");
            require(client.get("canary"), "stable");
        }
        // GETs are replicated commands too; check their final session tables as well.
        target = cluster.readState(cluster.awaitLeader()).applied();
        states = cluster.awaitState(expected, target, Map.of());
        report.put("verified_target_applied_index", target);
        report.put("verified_final_session_count", states.get(fault.child().id).sessions().size());
        long converged = System.nanoTime();
        report.put("restart_to_complete_convergence_ms", (converged - restarted) / 1e6);
        report.put("restarted_node_snapshot_index", states.get(fault.child().id).snapshot());
        events.add(new Event("maps_and_sessions_converged", converged - started,
                fault.child().id, fault.child().process.pid()));
        writeStates(config.report().resolve("states.csv"), cluster, states);
        cluster.announce("5/5", "Persist raw request evidence and stop every owned child JVM");
    }

    private static Fault injectLostReplyAndKill(GrpcProcessCluster cluster, List<String> urls,
                                               Map<String,String> expected, long started,
                                               List<Event> events, long seed, Map<String,Object> report) throws Exception {
        LostReply transport = new LostReply(cluster, urls, expected, started, events, report);
        try (GrpcKvClient client = new GrpcKvClient(urls, transport, Duration.ofSeconds(20),
                Duration.ofSeconds(6), seed)) {
            // The transport returns only the retry result to this client. Its first successful
            // response is deliberately discarded after receipt, before the application sees it.
            require(client.delete("retry-delete"), "before-crash");
        }
        events.add(new Event("delete_retry_confirmed", System.nanoTime() - started,
                transport.killed == null ? "none" : transport.killed.id, transport.pid));
        if (transport.killed == null || transport.attempts < 2) throw new IllegalStateException("Lost-reply fault did not execute");
        try (GrpcKvClient verifier = new GrpcKvClient(urls)) {
            require(verifier.get("retry-delete"), "after-crash");
        }
        return new Fault(transport.killed, transport.pid, transport.killOffset,
                transport.replacementPutMillis, transport.attempts, transport.leaderTerm, transport.leaderStatus);
    }

    private static final class LostReply implements RaftTransport {
        private final GrpcRpcClient delegate = new GrpcRpcClient();
        private final GrpcProcessCluster cluster;
        private final List<String> urls;
        private final Map<String,String> expected;
        private final long started;
        private final List<Event> events;
        private final Map<String,Object> report;
        private Command identity;
        private GrpcProcessCluster.Child killed;
        private long pid, killOffset, attempts;
        private long leaderTerm;
        private String leaderStatus;
        private double replacementPutMillis;

        LostReply(GrpcProcessCluster cluster, List<String> urls, Map<String,String> expected,
                  long started, List<Event> events, Map<String,Object> report) {
            this.cluster = cluster; this.urls = urls; this.expected = expected; this.started = started;
            this.events = events;
            this.report = report;
        }
        @Override public <T> T postObject(String url, String path, Serializable payload, Class<T> type,
                                         Duration timeout) throws IOException, InterruptedException {
            Command command = ((KvRpc.ClientCommandRequest) payload).command();
            if (identity == null) identity = command;
            if (!identity.equals(command)) throw new IOException("Retry changed the DELETE request identity");
            attempts++;
            T reply = delegate.postObject(url, path, payload, type, timeout);
            if (killed == null && reply instanceof KvRpc.ClientCommandResponse response && response.success()) {
                if (!Objects.equals(response.value(), "before-crash")) throw new IOException("First DELETE returned wrong value");
                var candidate = cluster.children.stream().filter(child -> child.url.equals(url)).findFirst().orElseThrow();
                try {
                    leaderStatus = delegate.postObject(url, "/status", "", String.class, Duration.ofMillis(700));
                } catch (IOException unavailable) {
                    throw new IllegalStateException("Cannot verify the serving leader before the kill", unavailable);
                }
                leaderTerm = validateLeaderObservation(response, candidate.id, candidate.url, leaderStatus);
                killed = candidate;
                pid = killed.process.pid();
                report.put("pre_kill_leader_term", leaderTerm);
                report.put("pre_kill_leader_status_json", leaderStatus);
                report.put("killed_leader", killed.id);
                report.put("killed_leader_pid", pid);
                events.add(new Event("leader_confirmed", System.nanoTime() - started, killed.id, pid));
                long at = System.nanoTime();
                killOffset = at - started;
                report.put("kill_offset_nanos", killOffset);
                events.add(new Event("kill_invoked", killOffset, killed.id, pid));
                try {
                    cluster.forceKill(killed);
                    events.add(new Event("process_terminated", System.nanoTime() - started, killed.id, pid));
                    // A new PUT starts only after process termination. Retrying the old DELETE
                    // must return its cached result without deleting this replacement value.
                    try (GrpcKvClient replacement = new GrpcKvClient(urls)) {
                        events.add(new Event("replacement_put_started", System.nanoTime() - started, killed.id, pid));
                        require(replacement.put("retry-delete", "after-crash"), "after-crash");
                        long confirmed = System.nanoTime();
                        events.add(new Event("replacement_put_confirmed", confirmed - started, killed.id, pid));
                        replacementPutMillis = (confirmed - at) / 1e6;
                    }
                    expected.put("retry-delete", "after-crash");
                } catch (InterruptedException interrupted) { throw interrupted; }
                catch (RuntimeException failedProbe) { throw failedProbe; }
                catch (Exception failure) { throw new IllegalStateException("Process fault failed; do not treat a failed replacement PUT as reply loss", failure); }
                throw new IOException("Injected: discard received committed DELETE reply after killing its serving JVM");
            }
            return reply;
        }
        @Override public void close() { delegate.close(); }
    }

    private static Map<String,Object> initialReport(Config config) {
        Map<String,Object> report = new LinkedHashMap<>();
        report.put("schema_version", 2); report.put("valid", false);
        report.put("started_at_utc", Instant.now().toString());
        report.put("java_version", System.getProperty("java.version"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        report.put("architecture", System.getProperty("os.arch"));
        report.put("node_count", 5); report.put("raft_node_processes", 5);
        report.put("execution", "five independent JVMs on one machine; real loopback gRPC; forced persistence enabled");
        report.put("workload", "closed-loop PUTs; disjoint client key subsets; separate DELETE/replacement fault probe excluded from request CSV");
        report.put("clients", config.clients()); report.put("key_count", config.keys());
        report.put("value_bytes", config.valueBytes()); report.put("seed", config.seed());
        report.put("requested_duration_seconds", config.duration());
        report.put("requested_kill_after_seconds", config.killAfter());
        report.put("maximum_runtime_seconds", config.maximumSeconds());
        report.put("snapshot_threshold", GrpcProcessCluster.SNAPSHOT_THRESHOLD);
        report.put("election_minimum_ms", 1000); report.put("election_maximum_exclusive_ms", 2000);
        report.put("heartbeat_interval_ms", 70);
        report.put("election_timer_policy", "Rejected higher-term votes retain prior deadline; granted votes and leader contact reset it");
        report.put("client_logical_request_budget_ms", 10000); report.put("client_per_rpc_cap_ms", 6000);
        report.put("fault_probe_logical_request_budget_ms", 20000);
        report.put("forced_process_exits", 1);
        report.put("latency_definition", "entire measured PUT API call including redirect/retry/backoff");
        report.put("throughput_denominator", "monotonic elapsed time including client setup, fault probe and final in-flight PUT completion");
        report.put("failover_definition", "destroyForcibly invocation against the last-observed leader through confirmation of a replacement PUT begun after process termination; client discovers replacement without status polling");
        report.put("reply_fault", "test transport discards a received committed DELETE response before application delivery; real leader process is then force-killed");
        report.put("scope", "one machine; no multi-host, network partition, power-loss, or failover-percentile claim");
        return report;
    }

    private static void require(KvRpc.ClientCommandResponse response, String expected) {
        if (!response.success() || !Objects.equals(response.value(), expected)) {
            throw new IllegalStateException("Wrong response: " + response + "; expected " + expected);
        }
    }
    static long validateLeaderObservation(KvRpc.ClientCommandResponse response, String id, String url, String status) {
        if (!id.equals(response.leaderId()) || !url.equals(response.leaderUrl())
                || !field(status, "id", id) || !field(status, "url", url)
                || !field(status, "role", "LEADER") || !field(status, "leaderId", id)
                || !Pattern.compile("\"running\"\\s*:\\s*true").matcher(status).find()) {
            throw new IllegalStateException("Serving node is no longer the observed leader; invalidate this fault trial");
        }
        var term = Pattern.compile("\"term\"\\s*:\\s*(\\d+)").matcher(status);
        if (!term.find() || Long.parseLong(term.group(1)) < 1) throw new IllegalStateException("Missing positive leader term");
        return Long.parseLong(term.group(1));
    }
    private static boolean field(String json, String name, String value) {
        return Pattern.compile("\"" + name + "\"\\s*:\\s*\"" + Pattern.quote(value) + "\"").matcher(json).find();
    }
    private static String value(int sequence, int bytes) {
        String digits = Integer.toString(sequence);
        return "v" + "0".repeat(Math.max(0, bytes - 1 - digits.length())) + digits;
    }
    private static Double percentile(List<Sample> samples, int percentile) {
        List<Long> sorted = samples.stream().filter(Sample::success).map(Sample::latency).sorted().toList();
        return sorted.isEmpty() ? null : sorted.get(Math.max(0, (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1)) / 1e6;
    }
    private static void writeSamples(Path path, List<Sample> samples) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("sequence,client,key,start_offset_nanos,latency_nanos,success,response_success,message,rpc_attempts,retry_rpc_attempts,backoff_nanos\n");
            for (Sample sample : samples.stream().sorted(Comparator.comparingInt(Sample::sequence)).toList()) {
                writer.write(sample.sequence() + "," + sample.client() + "," + sample.key() + "," + sample.start()
                        + "," + sample.latency() + "," + sample.success() + "," + sample.responseSuccess() + ",\""
                        + (sample.message() == null ? "" : sample.message().replace("\"", "\"\"").replace("\n", " ").replace("\r", " "))
                        + "\"," + sample.attempts()
                        + "," + sample.retries() + "," + sample.backoff() + "\n");
            }
        }
    }
    private static void writeEvents(Path path, List<Event> events) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("event,offset_nanos,node,pid\n");
            for (Event event : events) writer.write(event.name() + "," + event.offset() + ","
                    + event.node() + "," + event.pid() + "\n");
        }
    }
    private static void writeStates(Path path, GrpcProcessCluster cluster,
                                    Map<String,GrpcProcessCluster.AppliedState> states) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("node,pid,applied_index,snapshot_index,key_count,session_count,kv_sha256,sessions_sha256\n");
            for (GrpcProcessCluster.Child child : cluster.children) {
                var state = states.get(child.id);
                writer.write(child.id + "," + child.process.pid() + "," + state.applied() + "," + state.snapshot()
                        + "," + state.values().size() + "," + state.sessions().size() + ","
                        + fingerprint(state.values()) + "," + fingerprint(state.sessions()) + "\n");
            }
        }
    }
    private static String fingerprint(Map<String,?> values) throws Exception {
        // Deterministic for these generated ASCII keys/values and immutable session records.
        byte[] bytes = new TreeMap<>(values).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static void writeJson(Path path, Map<String,Object> report) throws IOException {
        StringBuilder json = new StringBuilder("{\n");
        for (var entry : report.entrySet()) {
            if (json.length() > 2) json.append(",\n");
            Object value = entry.getValue();
            json.append("  \"").append(escape(entry.getKey())).append("\": ");
            if (value == null || value instanceof Number || value instanceof Boolean) json.append(value);
            else json.append('"').append(escape(value.toString())).append('"');
        }
        Files.writeString(path, json.append("\n}\n").toString());
    }
    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    record Config(int duration, int killAfter, int clients, int keys, int valueBytes,
                  long seed, int maximumSeconds, Path report) {
        static Config parse(String[] args) {
            Set<String> allowed = Set.of("--duration", "--kill-after", "--clients", "--keys", "--value-bytes",
                    "--seed", "--max-runtime-seconds", "--report-dir");
            Map<String,String> options = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                if (!allowed.contains(args[i]) || i + 1 == args.length) throw new IllegalArgumentException("Expected known --option value pairs");
                if (options.put(args[i], args[i + 1]) != null) throw new IllegalArgumentException("Duplicate option: " + args[i]);
            }
            int duration = Integer.parseInt(options.getOrDefault("--duration", "20"));
            int killAfter = Integer.parseInt(options.getOrDefault("--kill-after", "5"));
            int clients = Integer.parseInt(options.getOrDefault("--clients", "4"));
            int keys = Integer.parseInt(options.getOrDefault("--keys", "64"));
            int bytes = Integer.parseInt(options.getOrDefault("--value-bytes", "64"));
            int maximum = Integer.parseInt(options.getOrDefault("--max-runtime-seconds", "240"));
            if (duration < 5 || killAfter < 1 || killAfter > duration - 3 || clients < 1 || keys < clients
                    || bytes < 16 || maximum < duration + 30) throw new IllegalArgumentException("Require duration>=5, 1<=kill-after<=duration-3, clients>=1, keys>=clients, value-bytes>=16, max-runtime>=duration+30");
            return new Config(duration, killAfter, clients, keys, bytes,
                    Long.parseLong(options.getOrDefault("--seed", "20261027")), maximum,
                    Path.of(options.getOrDefault("--report-dir", "reports/grpc-process-" + System.currentTimeMillis())).toAbsolutePath());
        }
    }
    private record Sample(int sequence, int client, String key, long start, long latency,
                          boolean success, boolean responseSuccess, String message,
                          long attempts, long retries, long backoff) { }
    private record Fault(GrpcProcessCluster.Child child, long pid, long killOffset,
                         double replacementPutMillis, long deleteAttempts, long term, String status) { }
    private record Event(String name, long offset, String node, long pid) { }
}
