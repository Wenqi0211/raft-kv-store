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

/** A small end-to-end check with five gRPC JVMs, two forced exits, and durable deduplication. */
public final class GrpcProcessClusterCheck implements AutoCloseable {
    private static final Pattern LEADER = Pattern.compile("\"role\"\\s*:\\s*\"LEADER\"");
    private static final int SNAPSHOT_THRESHOLD = 10;
    private final Path directory;
    private final List<Child> children = new ArrayList<>();
    private final List<Process> processes = Collections.synchronizedList(new ArrayList<>());
    private final GrpcRpcClient rpc = new GrpcRpcClient();
    private final long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
    private final Thread shutdownHook = new Thread(this::stopProcesses, "grpc-process-smoke-cleanup");
    private final String cluster;
    private String phase = "startup";
    private long driverSequence;

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            throw new IllegalArgumentException("Run without arguments: bash scripts/grpc-process-smoke.sh");
        }
        Path work = Path.of("work").toAbsolutePath();
        Files.createDirectories(work);
        Path directory = Files.createTempDirectory(work, "grpc-process-smoke-");
        System.out.println("Data and per-node stdout/stderr logs: " + directory);
        try (GrpcProcessClusterCheck check = new GrpcProcessClusterCheck(directory)) {
            check.run();
        } catch (Exception | AssertionError failure) {
            System.err.println("Process cluster check FAILED: " + failure.getMessage());
            System.err.println("Inspect the retained node logs and data in: " + directory);
            throw failure;
        }
    }

    private GrpcProcessClusterCheck(Path directory) throws IOException {
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

    private void run() throws Exception {
        announce("1/6", "Start five independent gRPC JVMs using the standalone GrpcNodeMain entry point");
        for (Child child : children) start(child);
        awaitLeader();

        announce("2/6", "Verify gRPC CRUD, CLI GET, cached duplicate DELETE, and stable canaries");
        Map<String,String> expected=new LinkedHashMap<>();
        expect(command(Command.Type.PUT,"course","raft"),"raft");
        expect(command(Command.Type.GET,"course",null),"raft");
        expect(command(Command.Type.PUT,"course","raft-updated"),"raft-updated");
        expected.put("course","raft-updated");
        checkCliGet("course","raft-updated");
        expect(command(Command.Type.PUT,"deleted","temporary"),"temporary");
        Command duplicateDelete=Command.client(Command.Type.DELETE,"deleted",null,"stable-delete-client",1);
        expect(duplicateDelete,"temporary");
        expect(duplicateDelete,"temporary");
        expect(command(Command.Type.GET,"deleted",null),null);
        expect(command(Command.Type.PUT,"deleted","replacement"),"replacement");
        expected.put("deleted","replacement");
        expect(duplicateDelete,"temporary");
        expect(command(Command.Type.GET,"deleted",null),"replacement");
        expect(command(Command.Type.GET,"never-written",null),null);
        for(int i=0;i<3;i++) {
            String key="canary-"+i,value="stable-"+i;
            expect(command(Command.Type.PUT,key,value),value);expected.put(key,value);
        }
        awaitState(expected,0,Map.of());

        Child killedLeader=awaitLeader();
        Child killedFollower=children.stream().filter(child->child!=killedLeader&&child.expectedUp)
                .findFirst().orElseThrow();
        long failedLeaderPid=killedLeader.process.pid(),failedFollowerPid=killedFollower.process.pid();
        announce("3/6", "Force-kill leader "+killedLeader.id+" and additional node "+killedFollower.id);
        long killedAt=System.nanoTime();
        forceKill(killedLeader);forceKill(killedFollower);
        Map<String,Long> stoppedProgress=Map.of(killedLeader.id,readState(killedLeader).applied,
                killedFollower.id,readState(killedFollower).applied);
        long beforeKillMax=stoppedProgress.values().stream().mapToLong(Long::longValue).max().orElseThrow();
        Child replacement=awaitLeader();
        System.out.println("Surviving leader: "+replacement.id+"; running Raft JVMs: 3");
        double firstWriteMillis=0;
        for(int i=0;i<25;i++) {
            String key="after-two-kills-"+i,value="value-"+i;
            expect(command(Command.Type.PUT,key,value),value);expected.put(key,value);
            if(i==0)firstWriteMillis=(System.nanoTime()-killedAt)/1e6;
        }
        expect(duplicateDelete,"temporary");
        expect(command(Command.Type.GET,"deleted",null),"replacement");
        expect(command(Command.Type.GET,"course",null),"raft-updated");
        expect(command(Command.Type.GET,"canary-0",null),"stable-0");

        announce("4/6", "Check all three survivors compact beyond both killed nodes' saved applied indexes");
        awaitSnapshotsBeyond(beforeKillMax);
        long target=readState(awaitLeader()).applied;
        awaitState(expected,target,Map.of());

        announce("5/6", "Restart both killed JVMs from their existing directories and verify snapshot catch-up");
        start(killedLeader);start(killedFollower);
        awaitState(expected,target,stoppedProgress);
        expect(duplicateDelete,"temporary");
        expect(command(Command.Type.GET,"deleted",null),"replacement");
        expect(command(Command.Type.GET,"after-two-kills-24",null),"value-24");
        Map<String,AppliedState> recovered=awaitState(expected,readState(awaitLeader()).applied,stoppedProgress);
        for(Child child:children) {
            AppliedState state=recovered.get(child.id);
            System.out.printf("  %s: PID=%d applied=%d snapshot=%d keys=%d sessions=%d%n",child.id,
                    child.process.pid(),state.applied,state.snapshot,state.values.size(),state.sessions.size());
        }

        announce("6/6", "Record process evidence and stop every child JVM");
        int sessionCount=recovered.values().iterator().next().sessions.size();
        String report="{\n  \"passed\": true,\n  \"raft_node_processes\": 5,\n"
                +"  \"forced_process_exits\": 2,\n  \"writes_with_three_survivors\": 25,\n"
                +"  \"final_matching_key_count\": "+expected.size()+",\n"
                +"  \"final_matching_session_count\": "+sessionCount+",\n"
                +"  \"killed_leader_pid\": "+failedLeaderPid+",\n"
                +"  \"killed_additional_pid\": "+failedFollowerPid+",\n"
                +"  \"two_process_kills_to_first_confirmed_write_ms\": "+firstWriteMillis+",\n"
                +"  \"measurement_scope\": \"one machine; five independent JVMs; forced OS process exits; no power loss or partition\"\n}\n";
        Files.writeString(directory.resolve("process-check.json"),report);
        System.out.println("PASS: standalone gRPC node/client CLI, five independent Raft JVMs, two forced exits,");
        System.out.println("25 writes with three survivors, snapshot catch-up, cached retry across recovery, and five matching durable KV/session maps");
        System.out.println("Retained stdout/stderr, process IDs, state and process-check.json: "+directory);
    }

    private Command command(Command.Type type,String key,String value) {
        return Command.client(type,key,value,"grpc-process-driver",++driverSequence);
    }

    private void forceKill(Child child) throws Exception {
        child.expectedUp=false;
        System.out.println("  destroyForcibly: "+child.id+" PID="+child.process.pid());
        child.process.destroyForcibly();
        if(!child.process.waitFor(5,TimeUnit.SECONDS))throw new IllegalStateException("Failed to terminate "+child.id);
    }

    private void checkCliGet(String key,String expected) throws Exception {
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        Path stdout=directory.resolve("client.stdout.log"),stderr=directory.resolve("client.stderr.log");
        String urls=String.join(",",children.stream().map(child->child.url).toList());
        Process client=new ProcessBuilder(java,"-Xms16m","-Xmx128m","-XX:ActiveProcessorCount=2",
                "-cp",System.getProperty("java.class.path"),"raftkv.client.GrpcClientMain",
                "--nodes",urls,"--client-id","grpc-cli-smoke","--sequence","1","get",key)
                .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        processes.add(client);
        if(!client.waitFor(remaining(Duration.ofSeconds(25)).toMillis(),TimeUnit.MILLISECONDS)) {
            client.destroyForcibly();throw new IllegalStateException("Standalone gRPC CLI timed out; inspect "+stderr);
        }
        if(client.exitValue()!=0||!Files.readString(stdout).trim().equals(expected))
            throw new IllegalStateException("Standalone gRPC CLI returned unexpected output; inspect "+stdout+" and "+stderr);
        System.out.println("  standalone GrpcClientMain GET passed, PID="+client.pid());
    }

    private void start(Child child) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        // Inherit the exact tested artifact, including a JAR selected by the script.
        // Child JVMs inherit this working directory, so relative classpaths still work.
        String classes = System.getProperty("java.class.path");
        child.process = new ProcessBuilder(java, "-Xms32m", "-Xmx256m", "-XX:ActiveProcessorCount=2",
                "-cp", classes, "raftkv.server.GrpcNodeMain",
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
                // Retry exactly the same client ID, sequence, and payload after an uncertain reply.
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

    private Map<String,AppliedState> awaitState(Map<String,String> expected,long target,
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
    private AppliedState readState(Child child) throws IOException {
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

    private record AppliedState(long applied,long snapshot,Map<String,String> values,Map<String,ClientSession> sessions) {
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
