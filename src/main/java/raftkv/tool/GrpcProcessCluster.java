package raftkv.tool;

import raftkv.kv.Command;
import raftkv.kv.ClientSession;
import raftkv.kv.KVStateMachine;
import raftkv.kv.KvRpc;
import raftkv.raft.LogEntry;
import raftkv.raft.PersistentState;
import raftkv.raft.PersistentStore;
import raftkv.rpc.GrpcRpcClient;

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

/** Owned five-process fixture shared by smoke and continuous-load failure measurements. */
class GrpcProcessCluster implements AutoCloseable {
    private static final Pattern LEADER = Pattern.compile("\"role\"\\s*:\\s*\"LEADER\"");
    static final int SNAPSHOT_THRESHOLD = 10;
    final Path directory;
    final List<Child> children = new ArrayList<>();
    final List<Process> processes = Collections.synchronizedList(new ArrayList<>());
    final GrpcRpcClient rpc = new GrpcRpcClient();
    private final long deadline;
    private final long seed;
    private final Thread shutdownHook = new Thread(this::stopProcesses, "grpc-process-smoke-cleanup");
    private final String cluster;
    private String phase = "startup";
    private boolean stopping;

    GrpcProcessCluster(Path directory) throws IOException {
        this(directory, 20261027L, 300);
    }

    GrpcProcessCluster(Path directory, long seed, int maximumSeconds) throws IOException {
        this.seed = seed;
        this.deadline = System.nanoTime() + Duration.ofSeconds(maximumSeconds).toNanos();
        this.directory = directory;
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (int i = 1; i <= 5; i++) {
                ServerSocket socket = new ServerSocket(0);
                reservations.add(socket);
                children.add(new Child("n" + i, "grpc://127.0.0.1:" + socket.getLocalPort()));
            }
        } finally {
            for (ServerSocket reservation : reservations) {
                reservation.close();
            }
        }
        cluster = String.join(",", children.stream().map(child -> child.id + "=" + child.url).toList());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    final void forceKill(Child child) throws Exception {
        child.expectedUp=false;
        System.out.println("  destroyForcibly: "+child.id+" PID="+child.process.pid());
        child.process.destroyForcibly();
        if(!child.process.waitFor(5,TimeUnit.SECONDS))throw new IllegalStateException("Failed to terminate "+child.id);
    }

    final void start(Child child) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        // Inherit the exact tested artifact, including a JAR selected by the script.
        // Child JVMs inherit this working directory, so relative classpaths still work.
        String classes = System.getProperty("java.class.path");
        // Registration and shutdown's snapshot use the same lock, so Ctrl-C during
        // startup cannot leave a just-created JVM outside the cleanup set.
        synchronized (processes) {
            if (stopping) throw new IOException("Cluster is already stopping");
            child.process = launchOwned(new ProcessBuilder(java, "-Xms32m", "-Xmx256m", "-XX:ActiveProcessorCount=2",
                    "-cp", classes, "raftkv.server.GrpcNodeMain",
                    "--id", child.id, "--url", child.url, "--cluster", cluster,
                    "--data-dir", child.data.toString(), "--snapshot-threshold", Integer.toString(SNAPSHOT_THRESHOLD),
                    "--seed", Long.toString(seed + child.id.hashCode()))
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(child.stdout.toFile()))
                    .redirectError(ProcessBuilder.Redirect.appendTo(child.stderr.toFile())));
            child.expectedUp = true;
        }
        System.out.printf("  %s: PID=%d %s%n    stdout: %s%n    stderr: %s%n", child.id,
                child.process.pid(), child.url, child.stdout, child.stderr);
    }

    final Process launchOwned(ProcessBuilder builder) throws IOException {
        synchronized (processes) {
            if (stopping) throw new IOException("Cluster is already stopping");
            Process process = builder.start();
            processes.add(process);
            return process;
        }
    }

    final Child awaitLeader() throws Exception {
        while (true) {
            checkProcesses();
            List<Child> leaders = new ArrayList<>();
            for (Child child : children) {
                if (!child.expectedUp) {
                    continue;
                }
                try {
                    String status = rpc.postObject(child.url, "/status", "", String.class,
                            remaining(Duration.ofMillis(700)));
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

    final void expect(Command command, String expected) throws Exception {
        KvRpc.ClientCommandResponse response = execute(command);
        if (!Objects.equals(expected, response.value())) {
            throw new IllegalStateException(command.type() + " " + command.key()
                    + " returned " + response.value() + "; expected " + expected);
        }
    }

    final KvRpc.ClientCommandResponse execute(Command command) throws Exception {
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
                // Retry exactly the same client ID, sequence, and payload after an uncertain reply.
            }
            pause();
        }
    }

    final void awaitSnapshotsBeyond(long beforeKill) throws Exception {
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

    final Map<String,AppliedState> awaitState(Map<String,String> expected,long target,
                                               Map<String,Long> recovering) throws Exception {
        while(true) {
            checkProcesses();boolean matched=true;
            Map<String,ClientSession> sessions=readState(awaitLeader()).sessions;
            Map<String,AppliedState> states=new LinkedHashMap<>();
            for(Child child:children) {
                if(!child.expectedUp)continue;
                AppliedState state=readState(child);states.put(child.id,state);
                Long before=recovering.get(child.id);
                if(state.applied<target||!state.values.equals(expected)||!state.sessions.equals(sessions)
                        ||(before!=null&&state.snapshot<=before))matched=false;
            }
            if(matched)return states;
            pause();
        }
    }

    // Read one atomic persisted version, then reconstruct only commands marked applied.
    // This deliberately avoids adding a public data-dump endpoint to the server.
    final AppliedState readState(Child child) throws IOException {
        PersistentState saved = new PersistentStore(child.data).load();
        KVStateMachine state = new KVStateMachine();
        state.restore(saved.snapshot.data(),saved.snapshot.sessions());
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
        return new AppliedState(applied, saved.snapshotIndex, state.snapshot(),state.sessionSnapshot());
    }

    final Duration remaining(Duration maximum) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new IllegalStateException("Timed out during: " + phase);
        }
        return Duration.ofNanos(Math.min(nanos, maximum.toNanos()));
    }

    private void pause() throws InterruptedException {
        Thread.sleep(Math.min(50, Math.max(1, remaining(Duration.ofMillis(50)).toMillis())));
    }

    final void checkProcesses() {
        remaining(Duration.ofMillis(1));
        for (Child child : children) {
            if (child.expectedUp && !child.process.isAlive()) {
                throw new IllegalStateException(child.id + " exited unexpectedly with code "
                        + child.process.exitValue() + "; inspect " + child.stdout + " and " + child.stderr);
            }
        }
    }

    final void announce(String step, String message) {
        phase = message;
        System.out.println("[" + step + "] " + message);
    }

    private void stopProcesses() {
        List<Process> snapshot;
        synchronized (processes) {
            stopping = true;
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

    record AppliedState(long applied,long snapshot,Map<String,String> values,Map<String,ClientSession> sessions) {
    }

    final class Child {
        final String id;
        final String url;
        final Path data;
        final Path stdout;
        final Path stderr;
        Process process;
        boolean expectedUp;

        private Child(String id, String url) {
            this.id = id;
            this.url = url;
            data = directory.resolve(id);
            stdout = directory.resolve(id + ".stdout.log");
            stderr = directory.resolve(id + ".stderr.log");
        }
    }
}
