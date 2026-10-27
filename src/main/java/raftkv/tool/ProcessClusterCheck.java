package raftkv.tool;

import raftkv.kv.Command;
import raftkv.kv.KVStateMachine;
import raftkv.kv.KvRpc;
import raftkv.raft.LogEntry;
import raftkv.raft.PersistentState;
import raftkv.raft.PersistentStore;
import raftkv.rpc.HttpRpcClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** A small end-to-end check with three JVMs and a real forced process exit. */
public final class ProcessClusterCheck implements AutoCloseable {
    private static final Pattern LEADER = Pattern.compile("\"role\"\\s*:\\s*\"LEADER\"");
    private static final int SNAPSHOT_THRESHOLD = 10;
    private final Path directory;
    private final List<Child> children = new ArrayList<>();
    private final List<Process> processes = Collections.synchronizedList(new ArrayList<>());
    private final HttpRpcClient rpc = new HttpRpcClient();
    private final long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
    private final Thread shutdownHook = new Thread(this::stopProcesses, "process-smoke-cleanup");
    private final String cluster;
    private String phase = "startup";

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            throw new IllegalArgumentException("Run without arguments: bash scripts/process-smoke.sh");
        }
        Path work = Path.of("work").toAbsolutePath();
        Files.createDirectories(work);
        Path directory = Files.createTempDirectory(work, "process-smoke-");
        System.out.println("Data and per-node stdout/stderr logs: " + directory);
        try (ProcessClusterCheck check = new ProcessClusterCheck(directory)) {
            check.run();
        } catch (Exception | AssertionError failure) {
            System.err.println("Process cluster check FAILED: " + failure.getMessage());
            System.err.println("Inspect the retained node logs and data in: " + directory);
            throw failure;
        }
    }

    private ProcessClusterCheck(Path directory) throws IOException {
        this.directory = directory;
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (int i = 1; i <= 3; i++) {
                ServerSocket socket = new ServerSocket(0);
                reservations.add(socket);
                children.add(new Child("n" + i, "http://127.0.0.1:" + socket.getLocalPort()));
            }
        } finally {
            for (ServerSocket reservation : reservations) {
                reservation.close();
            }
        }
        cluster = String.join(",", children.stream().map(child -> child.id + "=" + child.url).toList());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void run() throws Exception {
        announce("1/5", "Start three independent JVMs and wait for a leader");
        for (Child child : children) {
            start(child);
        }
        awaitLeader();

        announce("2/5", "Verify PUT, GET, update, DELETE, and stable canaries over HTTP");
        Map<String, String> expected = new LinkedHashMap<>();
        expect(Command.put("course", "raft"), "raft");
        expect(Command.get("course"), "raft");
        expect(Command.put("course", "raft-updated"), "raft-updated");
        expect(Command.get("course"), "raft-updated");
        expected.put("course", "raft-updated");
        expect(Command.put("deleted", "temporary"), "temporary");
        // If a successful DELETE response is lost, retrying may return null.
        // Verify the resulting absence rather than assuming exactly-once results.
        execute(Command.delete("deleted"));
        expect(Command.get("deleted"), null);
        for (int i = 0; i < 3; i++) {
            String key = "canary-" + i;
            String value = "stable-" + i;
            expect(Command.put(key, value), value);
            expect(Command.get(key), value);
            expected.put(key, value);
        }
        awaitState(expected, 0, null, 0);

        Child killed = awaitLeader();
        long beforeKill = readState(killed).applied;
        announce("3/5", "Force-kill leader " + killed.id + " (PID " + killed.process.pid() + ")");
        killed.expectedUp = false;
        killed.process.destroyForcibly();
        if (!killed.process.waitFor(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Leader did not exit after destroyForcibly: " + killed.id);
        }
        Child replacement = awaitLeader();
        System.out.println("New leader: " + replacement.id);
        for (int i = 0; i < 25; i++) {
            String key = "after-kill-" + i;
            String value = "value-" + i;
            expect(Command.put(key, value), value);
            expected.put(key, value);
        }
        expect(Command.get("course"), "raft-updated");
        expect(Command.get("canary-0"), "stable-0");
        expect(Command.get("after-kill-24"), "value-24");
        expect(Command.get("deleted"), null);

        announce("4/5", "Wait for both surviving nodes to compact beyond the killed leader's progress");
        awaitSnapshotsBeyond(beforeKill);
        long target = readState(awaitLeader()).applied;
        awaitState(expected, target, null, 0);

        announce("5/5", "Restart " + killed.id + " using its existing data and verify all three durable states");
        start(killed);
        Map<String, AppliedState> recovered = awaitState(expected, target, killed, beforeKill);
        for (Child child : children) {
            AppliedState state = recovered.get(child.id);
            System.out.printf("  %s: PID=%d applied=%d snapshot=%d keys=%d%n", child.id,
                    child.process.pid(), state.applied, state.snapshot, state.values.size());
        }
        System.out.println("PASS: HTTP CRUD, forced leader exit, majority writes, snapshot recovery, and three matching durable maps");
        System.out.println("This checks independent processes on one machine; it does not simulate power loss or network partitions.");
        System.out.println("Retained logs and data: " + directory);
    }

    private void start(Child child) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        // Inherit the exact tested artifact, including a JAR selected by the script.
        // Child JVMs inherit this working directory, so relative classpaths still work.
        String classes = System.getProperty("java.class.path");
        child.process = new ProcessBuilder(java, "-cp", classes, "raftkv.server.NodeMain",
                "--id", child.id, "--url", child.url, "--cluster", cluster,
                "--data-dir", child.data.toString(), "--snapshot-threshold", Integer.toString(SNAPSHOT_THRESHOLD))
                .redirectOutput(ProcessBuilder.Redirect.appendTo(child.stdout.toFile()))
                .redirectError(ProcessBuilder.Redirect.appendTo(child.stderr.toFile()))
                .start();
        processes.add(child.process);
        child.expectedUp = true;
        System.out.printf("  %s: PID=%d %s%n    stdout: %s%n    stderr: %s%n", child.id,
                child.process.pid(), child.url, child.stdout, child.stderr);
    }

    private Child awaitLeader() throws Exception {
        while (true) {
            checkProcesses();
            List<Child> leaders = new ArrayList<>();
            for (Child child : children) {
                if (!child.expectedUp) {
                    continue;
                }
                try {
                    String status = rpc.getText(child.url, "/status", remaining(Duration.ofMillis(700)));
                    if (LEADER.matcher(status).find()) {
                        leaders.add(child);
                    }
                } catch (IOException notReady) {
                    // Binding and elections happen asynchronously in the child JVMs.
                }
            }
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            pause();
        }
    }

    private void expect(Command command, String expected) throws Exception {
        KvRpc.ClientCommandResponse response = execute(command);
        if (!Objects.equals(expected, response.value())) {
            throw new IllegalStateException(command.type() + " " + command.key()
                    + " returned " + response.value() + "; expected " + expected);
        }
    }

    private KvRpc.ClientCommandResponse execute(Command command) throws Exception {
        while (true) {
            Child leader = awaitLeader();
            try {
                KvRpc.ClientCommandResponse response = rpc.postObject(leader.url, "/client",
                        new KvRpc.ClientCommandRequest(command), KvRpc.ClientCommandResponse.class,
                        remaining(Duration.ofSeconds(6)));
                if (response.success()) {
                    return response;
                }
            } catch (IOException retryAfterLeaderChange) {
                // PUTs use the same value on retries; no exactly-once guarantee is assumed.
            }
            pause();
        }
    }

    private void awaitSnapshotsBeyond(long beforeKill) throws Exception {
        while (true) {
            checkProcesses();
            boolean compacted = true;
            for (Child child : children) {
                if (child.expectedUp && readState(child).snapshot <= beforeKill) {
                    compacted = false;
                }
            }
            if (compacted) {
                return;
            }
            pause();
        }
    }

    private Map<String, AppliedState> awaitState(Map<String, String> expected, long target,
                                                Child recovering, long beforeKill) throws Exception {
        while (true) {
            checkProcesses();
            boolean matched = true;
            Map<String, AppliedState> states = new LinkedHashMap<>();
            for (Child child : children) {
                if (!child.expectedUp) {
                    continue;
                }
                AppliedState state = readState(child);
                states.put(child.id, state);
                if (state.applied < target || !state.values.equals(expected)
                        || (child == recovering && state.snapshot <= beforeKill)) {
                    matched = false;
                }
            }
            if (matched) {
                return states;
            }
            pause();
        }
    }

    // Read one atomic persisted version, then reconstruct only commands marked applied.
    // This deliberately avoids adding a public data-dump endpoint to the server.
    private AppliedState readState(Child child) throws IOException {
        PersistentState saved = new PersistentStore(child.data).load();
        KVStateMachine state = new KVStateMachine();
        state.restore(saved.snapshot.data());
        long applied = saved.snapshotIndex;
        for (LogEntry entry : saved.log) {
            if (entry.index() > saved.snapshotIndex && entry.index() <= saved.lastApplied) {
                if (entry.index() != applied + 1) {
                    throw new IOException(child.id + " has a gap in its applied persisted log");
                }
                state.apply(entry.command());
                applied = entry.index();
            }
        }
        if (applied != saved.lastApplied) {
            throw new IOException(child.id + " has an inconsistent persisted applied index");
        }
        return new AppliedState(applied, saved.snapshotIndex, state.snapshot());
    }

    private Duration remaining(Duration maximum) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new IllegalStateException("Timed out during: " + phase);
        }
        return Duration.ofNanos(Math.min(nanos, maximum.toNanos()));
    }

    private void pause() throws InterruptedException {
        Thread.sleep(Math.min(50, Math.max(1, remaining(Duration.ofMillis(50)).toMillis())));
    }

    private void checkProcesses() {
        remaining(Duration.ofMillis(1));
        for (Child child : children) {
            if (child.expectedUp && !child.process.isAlive()) {
                throw new IllegalStateException(child.id + " exited unexpectedly with code "
                        + child.process.exitValue() + "; inspect " + child.stdout + " and " + child.stderr);
            }
        }
    }

    private void announce(String step, String message) {
        phase = message;
        System.out.println("[" + step + "] " + message);
    }

    private void stopProcesses() {
        List<Process> snapshot;
        synchronized (processes) {
            snapshot = List.copyOf(processes);
        }
        snapshot.forEach(Process::destroy);
        for (Process process : snapshot) {
            try {
                if (!process.waitFor(2, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(2, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                snapshot.forEach(Process::destroyForcibly);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        stopProcesses();
        rpc.close();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException alreadyShuttingDown) {
            // The hook also cleans up when the parent JVM receives Ctrl-C.
        }
    }

    private record AppliedState(long applied, long snapshot, Map<String, String> values) {
    }

    private final class Child {
        private final String id;
        private final String url;
        private final Path data;
        private final Path stdout;
        private final Path stderr;
        private Process process;
        private boolean expectedUp;

        private Child(String id, String url) {
            this.id = id;
            this.url = url;
            data = directory.resolve(id);
            stdout = directory.resolve(id + ".stdout.log");
            stderr = directory.resolve(id + ".stderr.log");
        }
    }
}
