package raftkv.tool;

import raftkv.kv.Command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** A small end-to-end check with five gRPC JVMs, two forced exits, and durable deduplication. */
public final class GrpcProcessClusterCheck extends GrpcProcessCluster {
    private long driverSequence;

    private GrpcProcessClusterCheck(Path directory) throws IOException { super(directory); }

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
        Map<String,Long> stoppedProgress=Map.of(killedLeader.id,readState(killedLeader).applied(),
                killedFollower.id,readState(killedFollower).applied());
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
        long target=readState(awaitLeader()).applied();
        awaitState(expected,target,Map.of());

        announce("5/6", "Restart both killed JVMs from their existing directories and verify snapshot catch-up");
        start(killedLeader);start(killedFollower);
        awaitState(expected,target,stoppedProgress);
        expect(duplicateDelete,"temporary");
        expect(command(Command.Type.GET,"deleted",null),"replacement");
        expect(command(Command.Type.GET,"after-two-kills-24",null),"value-24");
        Map<String,AppliedState> recovered=awaitState(expected,readState(awaitLeader()).applied(),stoppedProgress);
        for(Child child:children) {
            AppliedState state=recovered.get(child.id);
            System.out.printf("  %s: PID=%d applied=%d snapshot=%d keys=%d sessions=%d%n",child.id,
                    child.process.pid(),state.applied(),state.snapshot(),state.values().size(),state.sessions().size());
        }

        announce("6/6", "Record process evidence and stop every child JVM");
        int sessionCount=recovered.values().iterator().next().sessions().size();
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

    private void checkCliGet(String key,String expected) throws Exception {
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        Path stdout=directory.resolve("client.stdout.log"),stderr=directory.resolve("client.stderr.log");
        String urls=String.join(",",children.stream().map(child->child.url).toList());
        Process client=launchOwned(new ProcessBuilder(java,"-Xms16m","-Xmx128m","-XX:ActiveProcessorCount=2",
                "-cp",System.getProperty("java.class.path"),"raftkv.client.GrpcClientMain",
                "--nodes",urls,"--client-id","grpc-cli-smoke","--sequence","1","get",key)
                .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()));
        if(!client.waitFor(remaining(Duration.ofSeconds(25)).toMillis(),TimeUnit.MILLISECONDS)) {
            client.destroyForcibly();throw new IllegalStateException("Standalone gRPC CLI timed out; inspect "+stderr);
        }
        if(client.exitValue()!=0||!Files.readString(stdout).trim().equals(expected))
            throw new IllegalStateException("Standalone gRPC CLI returned unexpected output; inspect "+stdout+" and "+stderr);
        System.out.println("  standalone GrpcClientMain GET passed, PID="+client.pid());
    }

}
