package raftkv.tool;

import raftkv.client.KvClient;
import raftkv.kv.KvRpc;
import raftkv.raft.Peer;
import raftkv.raft.RaftNode;
import raftkv.raft.Role;
import raftkv.server.RaftHttpServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public final class BenchmarkMain {
    private BenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        int durationSeconds = Integer.parseInt(options.getOrDefault("--duration", "5"));
        int clients = Integer.parseInt(options.getOrDefault("--clients", "6"));
        int keys = Integer.parseInt(options.getOrDefault("--keys", "1000"));
        int snapshotThreshold = Integer.parseInt(options.getOrDefault("--snapshot-threshold", "40"));
        if (durationSeconds <= 0 || clients <= 0 || keys <= 0 || snapshotThreshold <= 0) {
            throw new IllegalArgumentException("Duration, clients, keys, and snapshot threshold must be positive");
        }

        Files.createDirectories(Path.of("work"));
        Path dataRoot = Files.createTempDirectory(Path.of("work"), "raftkv-bench-");
        List<Integer> ports = freePorts(3);
        LinkedHashMap<String, Peer> peers = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            String id = "n" + (i + 1);
            peers.put(id, new Peer(id, "http://127.0.0.1:" + ports.get(i)));
        }

        List<ManagedNode> nodes = new ArrayList<>();
        for (Peer peer : peers.values()) {
            nodes.add(new ManagedNode(peer.id(), peer.url(), peers, dataRoot.resolve(peer.id()), snapshotThreshold));
        }

        try {
            for (ManagedNode node : nodes) {
                node.start();
            }
            waitForLeader(nodes, Duration.ofSeconds(8));
            List<String> urls = peers.values().stream().map(Peer::url).toList();

            Map<String, String> expected = verifyBasicOperationsAndWarmup(urls);
            BenchmarkResult result = runWriteBenchmark(urls, durationSeconds, clients, keys);
            verifyKnownValues(urls, expected);
            waitForConvergence(nodes, expected, Duration.ofSeconds(12));

            ManagedNode stoppedLeader = waitForLeader(nodes, Duration.ofSeconds(8));
            long failoverMillis = measureFailover(nodes, stoppedLeader);
            expected.put("verify-failover", "after-leader-stop");
            runRecoveryWrites(urls, expected);
            verifyKnownValues(urls, expected);
            long maxSnapshot = waitForSnapshot(nodes, snapshotThreshold, Duration.ofSeconds(8));

            stoppedLeader.start();
            waitForLeader(nodes, Duration.ofSeconds(8));
            verifyKnownValues(urls, expected);
            RecoveryResult recovery = waitForConvergence(nodes, expected, Duration.ofSeconds(15));
            maxSnapshot = Math.max(maxSnapshot, nodes.stream().mapToLong(node -> node.node.snapshotIndex()).max().orElse(0));

            printReport(clients, result, stoppedLeader.id, failoverMillis, maxSnapshot, recovery, dataRoot);
        } finally {
            for (ManagedNode node : nodes) {
                node.stop();
            }
        }
    }

    private static Map<String, String> verifyBasicOperationsAndWarmup(List<String> urls) {
        Map<String, String> expected = new LinkedHashMap<>();
        try (KvClient client = new KvClient(urls)) {
            requireValue("basic PUT", client.put("verify-deleted", "first"), "first");
            requireValue("basic GET", client.get("verify-deleted"), "first");
            requireValue("basic update", client.put("verify-deleted", "updated"), "updated");
            requireValue("GET after update", client.get("verify-deleted"), "updated");
            requireValue("basic DELETE", client.delete("verify-deleted"), "updated");
            requireValue("GET after DELETE", client.get("verify-deleted"), null);
            requireValue("GET missing key", client.get("verify-never-written"), null);

            for (int i = 0; i < 8; i++) {
                String key = "verify-canary-" + i;
                String value = "stable-value-" + i;
                requireValue("canary PUT " + key, client.put(key, value), value);
                expected.put(key, value);
            }
            for (int i = 0; i < 20; i++) {
                String key = "warmup-" + i;
                String value = "v" + i;
                requireValue("warmup PUT " + key, client.put(key, value), value);
                expected.put(key, value);
            }
        }
        return expected;
    }

    private static void verifyKnownValues(List<String> urls, Map<String, String> expected) {
        try (KvClient client = new KvClient(urls)) {
            for (Map.Entry<String, String> entry : expected.entrySet()) {
                requireValue("GET " + entry.getKey(), client.get(entry.getKey()), entry.getValue());
            }
            requireValue("deleted key must stay absent", client.get("verify-deleted"), null);
            requireValue("missing key must stay absent", client.get("verify-never-written"), null);
        }
    }

    private static void requireValue(String operation, KvRpc.ClientCommandResponse response, String expected) {
        if (!response.success() || !Objects.equals(response.value(), expected)) {
            throw new IllegalStateException(operation + " failed: expected=" + expected
                    + ", actual=" + response.value() + ", success=" + response.success()
                    + ", message=" + response.message());
        }
    }

    private static BenchmarkResult runWriteBenchmark(List<String> urls, int durationSeconds, int clients, int keys)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        LongAdder ok = new LongAdder();
        LongAdder failed = new LongAdder();
        List<Long> latenciesMicros = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger sequence = new AtomicInteger();
        long started = System.nanoTime();
        long deadline = started + TimeUnit.SECONDS.toNanos(durationSeconds);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            futures.add(pool.submit(() -> {
                try (KvClient client = new KvClient(urls)) {
                    while (System.nanoTime() < deadline) {
                        int seq = sequence.incrementAndGet();
                        String key = "k" + (seq % keys);
                        long start = System.nanoTime();
                        String value = "v" + seq;
                        KvRpc.ClientCommandResponse response = client.put(key, value);
                        long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                        latenciesMicros.add(elapsedMicros);
                        if (response.success()) {
                            requireValue("benchmark PUT " + key, response, value);
                            ok.increment();
                        } else {
                            failed.increment();
                        }
                    }
                }
            }));
        }
        try {
            for (Future<?> future : futures) {
                long remaining = Math.max(1, deadline + TimeUnit.SECONDS.toNanos(45) - System.nanoTime());
                future.get(remaining, TimeUnit.NANOSECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        long elapsedNanos = System.nanoTime() - started;
        List<Long> sorted = new ArrayList<>(latenciesMicros);
        sorted.sort(Comparator.naturalOrder());
        return new BenchmarkResult(ok.sum(), failed.sum(), sorted, elapsedNanos);
    }

    private static long measureFailover(List<ManagedNode> nodes, ManagedNode stoppedLeader) throws Exception {
        stoppedLeader.stop();
        long start = System.nanoTime();
        waitForLeader(nodes, Duration.ofSeconds(12));
        List<String> liveUrls = nodes.stream().filter(ManagedNode::isRunning).map(node -> node.url).toList();
        try (KvClient client = new KvClient(liveUrls)) {
            while (Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(12)) < 0) {
                KvRpc.ClientCommandResponse response = client.put("verify-failover", "after-leader-stop");
                if (response.success()) {
                    requireValue("failover PUT", response, "after-leader-stop");
                    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                }
                Thread.sleep(40);
            }
        }
        throw new IllegalStateException("No new leader accepted writes after failover");
    }

    private static void runRecoveryWrites(List<String> urls, Map<String, String> expected) {
        try (KvClient client = new KvClient(urls)) {
            for (int i = 0; i < 80; i++) {
                String key = "verify-recovery-" + i;
                String value = "v" + i;
                requireValue("recovery PUT " + key, client.put(key, value), value);
                expected.put(key, value);
            }
        }
    }

    private static long waitForSnapshot(List<ManagedNode> nodes, int threshold, Duration timeout) throws InterruptedException {
        boolean expected = nodes.stream().filter(ManagedNode::isRunning)
                .anyMatch(node -> node.node.lastAppliedIndex() >= threshold);
        long deadline = System.nanoTime() + timeout.toNanos();
        long max = 0;
        while (System.nanoTime() < deadline) {
            max = nodes.stream()
                    .filter(ManagedNode::isRunning)
                    .mapToLong(node -> node.node.snapshotIndex())
                    .max()
                    .orElse(0);
            if (max > 0 || !expected) {
                return max;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Snapshot threshold was reached, but no snapshot appeared before timeout");
    }

    private static RecoveryResult waitForConvergence(
            List<ManagedNode> nodes, Map<String, String> expected, Duration timeout
    ) throws InterruptedException {
        ManagedNode leader = waitForLeader(nodes, timeout);
        long targetIndex = leader.node.lastAppliedIndex();
        Map<String, String> targetState = leader.node.localState();
        requireKnownState(leader.id, targetState, expected);
        long deadline = System.nanoTime() + timeout.toNanos();
        Map<String, Long> appliedIndices = new LinkedHashMap<>();
        while (System.nanoTime() < deadline) {
            boolean converged = true;
            appliedIndices.clear();
            for (ManagedNode node : nodes) {
                if (!node.isRunning()) {
                    converged = false;
                    continue;
                }
                long applied = node.node.lastAppliedIndex();
                Map<String, String> state = node.node.localState();
                appliedIndices.put(node.id, applied);
                if (applied < targetIndex || !state.equals(targetState)) {
                    converged = false;
                }
            }
            if (converged) {
                return new RecoveryResult(targetIndex, targetState.size(), Map.copyOf(appliedIndices));
            }
            Thread.sleep(25);
        }
        List<String> diagnostics = nodes.stream().map(node -> node.id + ": "
                + (node.isRunning() ? node.node.statusJson() + ", stateMatches="
                + node.node.localState().equals(targetState) : "stopped")).toList();
        throw new IllegalStateException("Nodes did not converge through applied index " + targetIndex
                + " with " + targetState.size() + " matching keys: " + diagnostics);
    }

    private static void requireKnownState(String nodeId, Map<String, String> state, Map<String, String> expected) {
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            if (!Objects.equals(state.get(entry.getKey()), entry.getValue())) {
                throw new IllegalStateException(nodeId + " has incorrect value for " + entry.getKey());
            }
        }
        if (state.containsKey("verify-deleted") || state.containsKey("verify-never-written")) {
            throw new IllegalStateException(nodeId + " contains a deleted or never-written key");
        }
    }

    private static ManagedNode waitForLeader(List<ManagedNode> nodes, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            List<ManagedNode> leaders = nodes.stream()
                    .filter(ManagedNode::isRunning)
                    .filter(node -> node.node.role() == Role.LEADER)
                    .toList();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("No stable leader elected");
    }

    private static void printReport(
            int clients,
            BenchmarkResult result,
            String stoppedLeader,
            long failoverMillis,
            long maxSnapshot,
            RecoveryResult recovery,
            Path dataRoot
    ) {
        double elapsedSeconds = result.elapsedNanos / 1_000_000_000.0;
        double qps = result.ok / elapsedSeconds;
        System.out.println("RaftKV benchmark");
        System.out.println("Cluster: 3 nodes");
        System.out.println("Execution: one JVM, real HTTP RPC; failure uses stop/start, not process kill or power loss");
        System.out.println("Basic checks: PUT/GET/update/DELETE/missing reads and 8 stable canaries passed");
        System.out.printf("Clients: %d%n", clients);
        System.out.printf("Writes: %d ok, %d failed%n", result.ok, result.failed);
        System.out.printf("Measured write duration: %.2f s%n", elapsedSeconds);
        System.out.printf("Throughput: %.2f QPS%n", qps);
        System.out.printf("Latency: p50=%.2f ms p95=%.2f ms p99=%.2f ms%n",
                percentile(result.latenciesMicros, 50) / 1000.0,
                percentile(result.latenciesMicros, 95) / 1000.0,
                percentile(result.latenciesMicros, 99) / 1000.0);
        System.out.printf("Failover: leader %s stopped, new writes accepted after %d ms%n", stoppedLeader, failoverMillis);
        System.out.printf("Recovery: 80 writes made while leader was stopped; all 3 nodes match %d keys through applied index %d%n",
                recovery.keyCount, recovery.targetIndex);
        System.out.println("Verified applied indices: " + recovery.appliedIndices);
        System.out.printf("Snapshot: max snapshot index = %d%s%n", maxSnapshot,
                maxSnapshot == 0 ? " (configured threshold not reached)" : "");
        System.out.println("Data root: " + dataRoot.toAbsolutePath());
    }

    private static long percentile(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static List<Integer> freePorts(int count) throws IOException {
        List<Integer> ports = new ArrayList<>();
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket);
                ports.add(socket.getLocalPort());
            }
            return ports;
        } finally {
            for (ServerSocket socket : sockets) {
                socket.close();
            }
        }
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + key);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + key);
            }
            options.put(key, args[++i]);
        }
        return options;
    }

    private record BenchmarkResult(long ok, long failed, List<Long> latenciesMicros, long elapsedNanos) {
    }

    private record RecoveryResult(long targetIndex, int keyCount, Map<String, Long> appliedIndices) {
    }

    private static final class ManagedNode {
        private final String id;
        private final String url;
        private final Map<String, Peer> peers;
        private final Path dataDir;
        private final int snapshotThreshold;
        private RaftNode node;
        private RaftHttpServer server;
        private boolean running;

        private ManagedNode(String id, String url, Map<String, Peer> peers, Path dataDir, int snapshotThreshold) {
            this.id = id;
            this.url = url;
            this.peers = peers;
            this.dataDir = dataDir;
            this.snapshotThreshold = snapshotThreshold;
        }

        private void start() throws IOException {
            if (running) {
                return;
            }
            node = new RaftNode(id, url, peers, dataDir, snapshotThreshold);
            server = new RaftHttpServer(node);
            server.start();
            node.start();
            running = true;
        }

        private void stop() {
            if (!running) {
                return;
            }
            server.stop();
            node.stop();
            running = false;
        }

        private boolean isRunning() {
            return running;
        }
    }
}

