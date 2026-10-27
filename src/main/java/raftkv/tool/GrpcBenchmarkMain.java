package raftkv.tool;

import raftkv.client.GrpcKvClient;
import raftkv.kv.ClientSession;
import raftkv.kv.KvRpc;
import raftkv.raft.Peer;
import raftkv.raft.ElectionTiming;
import raftkv.raft.RaftNode;
import raftkv.raft.Role;
import raftkv.rpc.GrpcRpcClient;
import raftkv.server.RaftGrpcServer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Reproducible five-node, loopback gRPC measurement; never manufactures results. */
public final class GrpcBenchmarkMain {
    private static final int NODE_COUNT = 5;
    private static final int SNAPSHOT_THRESHOLD = 40;
    private GrpcBenchmarkMain() {}

    public static void main(String[] args) throws Exception {
        Map<String,String> options = options(args);
        int duration = Integer.parseInt(options.getOrDefault("--duration", "30"));
        int clients = Integer.parseInt(options.getOrDefault("--clients", "4"));
        int keys = Integer.parseInt(options.getOrDefault("--keys", "1000"));
        int valueBytes = Integer.parseInt(options.getOrDefault("--value-bytes", "64"));
        long seed = Long.parseLong(options.getOrDefault("--seed", "20261024"));
        int maximumRuntime = Integer.parseInt(options.getOrDefault("--max-runtime-seconds", Integer.toString(Math.max(600,duration+120))));
        ElectionTiming defaults=ElectionTiming.durableGrpc();
        ElectionTiming timing=new ElectionTiming(Integer.parseInt(options.getOrDefault("--election-min-ms",Integer.toString(defaults.minimumMillis()))),
                Integer.parseInt(options.getOrDefault("--election-max-ms",Integer.toString(defaults.maximumExclusiveMillis()))),
                Integer.parseInt(options.getOrDefault("--heartbeat-ms",Integer.toString(defaults.heartbeatMillis()))));
        if (duration < 1 || clients < 1 || keys < clients || valueBytes < 16 || maximumRuntime < duration) {
            throw new IllegalArgumentException("Require duration>=1, clients>=1, keys>=clients, value-bytes>=16 and max-runtime>=duration");
        }
        Path reports = Path.of(options.getOrDefault("--report-dir", "reports/grpc-" + System.currentTimeMillis()))
                .toAbsolutePath();
        if (Files.exists(reports.resolve("summary.json")) || Files.exists(reports.resolve("requests.csv"))) {
            throw new IllegalArgumentException("Refusing to replace an existing report: " + reports);
        }
        Files.createDirectories(reports);
        Map<String,Object> report = new LinkedHashMap<>();
        report.put("started_at_utc", Instant.now().toString());
        report.put("java_version", System.getProperty("java.version"));
        report.put("java_vm", System.getProperty("java.vm.name"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        report.put("architecture", System.getProperty("os.arch"));
        report.put("available_processors", Runtime.getRuntime().availableProcessors());
        report.put("node_count", NODE_COUNT);
        report.put("transport", "gRPC over loopback HTTP/2, plaintext, protobuf envelope with Java DTO payload");
        report.put("execution", "five Raft nodes in one JVM; per-node state directories and outgoing channels");
        report.put("workload", "closed-loop PUT only; disjoint key ownership per sequential client");
        report.put("clients", clients); report.put("key_count", keys); report.put("value_bytes", valueBytes);
        report.put("requested_duration_seconds", duration); report.put("snapshot_threshold", SNAPSHOT_THRESHOLD);
        report.put("seed", seed); report.put("valid", false);
        report.put("maximum_runtime_seconds",maximumRuntime);
        report.put("election_minimum_ms",timing.minimumMillis());
        report.put("election_maximum_exclusive_ms",timing.maximumExclusiveMillis());
        report.put("heartbeat_interval_ms",timing.heartbeatMillis());
        report.put("consensus_rpc_timeout_ms",450);
        report.put("client_logical_request_budget_ms",10000);
        report.put("client_per_rpc_cap_ms",6000);
        report.put("client_backoff_range_ms","20–200, capped exponential with jitter after failed rounds");
        report.put("measured_client_jitter_seed","reported seed plus zero-based client number; OS scheduling remains nondeterministic");
        List<Node> nodes = new ArrayList<>();
        RuntimeBudget budget=new RuntimeBudget(maximumRuntime);
        try {
            budget.phase("cluster startup");
            List<Integer> ports = reservePorts(NODE_COUNT);
            Map<String,Peer> peers = new LinkedHashMap<>();
            for (int i=0;i<NODE_COUNT;i++) {
                String id="n"+(i+1); peers.put(id,new Peer(id,"grpc://127.0.0.1:"+ports.get(i)));
            }
            for (Peer peer:peers.values()) nodes.add(new Node(peer,peers,reports.resolve("data").resolve(peer.id()),seed,timing));
            for (Node node:nodes) node.start();
            Node leader = leader(nodes,Duration.ofSeconds(15));
            List<String> urls = urls(nodes,leader);
            Map<String,String> expected = new ConcurrentHashMap<>();
            try (GrpcKvClient client = new GrpcKvClient(urls)) {
                require("PUT",client.put("verify-deleted","first"),"first");
                require("GET",client.get("verify-deleted"),"first");
                require("update",client.put("verify-deleted","second"),"second");
                require("DELETE",client.delete("verify-deleted"),"second");
                require("missing after DELETE",client.get("verify-deleted"),null);
                require("never-written",client.get("verify-never-written"),null);
                for (int i=0;i<8;i++) {
                    String key="canary-"+i,value="stable-"+i;
                    require("canary",client.put(key,value),value); expected.put(key,value);
                }
                budget.phase("keyspace preload");
                System.out.println("Preloading all "+keys+" keys with "+valueBytes+"-byte values");
                long lastProgress=System.nanoTime();
                for (int i=0;i<keys;i++) {
                    budget.check();
                    String key="k"+i,value=value(i,valueBytes);
                    require("preload "+key,client.put(key,value),value); expected.put(key,value);
                    if((i+1)%100==0||System.nanoTime()-lastProgress>=TimeUnit.SECONDS.toNanos(10)) {
                        System.out.printf(Locale.ROOT,"Preload progress: %d/%d keys; total runtime %.1fs%n",i+1,keys,budget.elapsedSeconds());
                        lastProgress=System.nanoTime();
                    }
                }
            }
            converged(nodes,expected,Duration.ofSeconds(20));
            report.put("preload_keys",keys);
            report.put("preload_in_timed_statistics",false);
            report.put("measured_client_channels","created at timed phase start; connection setup and client initialization included");
            System.out.println("Starting "+duration+"s measured write phase with "+clients+" clients");
            budget.phase("timed write phase");
            long termBefore=maximumTerm(nodes);
            report.put("maximum_term_before_timed_phase",termBefore);
            Result result=measure(urls,clients,duration,keys,valueBytes,expected,budget,reports.resolve("requests.csv"),seed);
            long termAfter=maximumTerm(nodes);
            report.put("maximum_term_after_timed_phase",termAfter);
            report.put("timed_phase_term_increase",termAfter-termBefore);
            List<Sample> successful=result.samples().stream().filter(Sample::success).toList();
            long failed=result.samples().size()-successful.size();
            report.put("attempted_requests",result.samples().size()); report.put("successful_requests",successful.size());
            report.put("failed_requests",failed); report.put("actual_elapsed_seconds",result.elapsedNanos()/1e9);
            report.put("client_rpc_attempts",result.samples().stream().mapToLong(Sample::rpcAttempts).sum());
            report.put("client_retry_rpc_attempts",result.samples().stream().mapToLong(Sample::retryRpcAttempts).sum());
            report.put("client_backoff_total_ms",result.samples().stream().mapToLong(Sample::backoffNanos).sum()/1e6);
            report.put("successful_requests_with_retry",successful.stream().filter(s->s.rpcAttempts()>1).count());
            report.put("retry_statistics_scope","every measured logical request; application client RPCs only, excluding status, preload and validation");
            report.put("successful_writes_per_second",successful.size()/(result.elapsedNanos()/1e9));
            report.put("completed_requests_per_second",result.samples().size()/(result.elapsedNanos()/1e9));
            report.put("latency_p50_success_ms",percentile(successful,50));
            report.put("latency_p95_success_ms",percentile(successful,95));
            report.put("latency_p99_success_ms",percentile(successful,99));
            report.put("latency_p99_all_ms",percentile(result.samples(),99));
            report.put("latency_definition","client API call duration, including redirects/retries; timed phase only");
            report.put("throughput_denominator","measured monotonic elapsed time, including final in-flight completion");
            writeJson(reports.resolve("summary.json"),report);
            if (failed != 0) throw new IllegalStateException("Timed requests had "+failed+" failed responses; not a passing study");
            budget.phase("post-measure value and convergence checks");
            converged(nodes,expected,Duration.ofSeconds(20));
            verifyReadSamples(urls,expected,keys);
            leader=leader(nodes,Duration.ofSeconds(15));
            Node stoppedLeader=leader;
            budget.phase("leader stop/start failover");
            long failureStarted=System.nanoTime();
            stoppedLeader.stop();
            Node replacement=leader(nodes,Duration.ofSeconds(15));
            try (GrpcKvClient client=new GrpcKvClient(urls(nodes,replacement))) {
                require("failover write",client.put("after-failover","accepted"),"accepted");
                expected.put("after-failover","accepted");
                report.put("leader_stop_to_confirmed_write_ms",(System.nanoTime()-failureStarted)/1e6);
                for(int i=0;i<80;i++) {
                    String key="recovery-"+i,value="v"+i;
                    require("offline-leader write",client.put(key,value),value); expected.put(key,value);
                }
            }
            report.put("failure_method","explicit server.stop/node.stop in one JVM; not an OS process kill or power loss");
            report.put("failover_measurement","from stop invocation through first confirmed PUT via newly discovered leader; polling every 20ms");
            report.put("stopped_leader",stoppedLeader.peer.id()); report.put("replacement_leader",replacement.peer.id());
            report.put("writes_while_leader_offline",81);
            budget.phase("two-node outage checks");
            Node second=nodes.stream().filter(n->n.running&&n!=replacement).findFirst().orElseThrow();
            second.stop();
            try (GrpcKvClient client=new GrpcKvClient(urls(nodes,replacement))) {
                for(int i=0;i<25;i++) {
                    String key="two-failures-"+i,value="v"+i;
                    require("two-offline write",client.put(key,value),value); expected.put(key,value);
                }
                for(int i=0;i<8;i++) require("canary after two failures",client.get("canary-"+i),"stable-"+i);
                require("deleted key after two failures",client.get("verify-deleted"),null);
            }
            report.put("simultaneously_stopped_nodes",2); report.put("writes_with_two_nodes_offline",25);
            budget.phase("restarted-node convergence");
            long recoveryStart=System.nanoTime(); stoppedLeader.start(); second.start();
            Convergence convergence=converged(nodes,expected,Duration.ofSeconds(30));
            report.put("restarted_node_convergence_ms",(System.nanoTime()-recoveryStart)/1e6);
            report.put("verified_final_key_count",expected.size());
            report.put("all_five_maps_and_sessions_match",true);
            report.put("verified_target_applied_index",convergence.index());
            report.put("verified_session_count",convergence.sessions());
            report.put("max_snapshot_index",nodes.stream().mapToLong(n->n.node.snapshotIndex()).max().orElse(0));
            report.put("basic_crud_and_sampled_read_checks_passed",true);
            report.put("history_checker_in_this_benchmark",false);
            budget.check();
            report.put("valid",true); report.put("runtime_budget_expired",false);report.put("completed_at_utc",Instant.now().toString());
            writeJson(reports.resolve("summary.json"),report);
            System.out.printf(Locale.ROOT,"PASS: %,d writes, %.1f writes/s, p99 %.1fms, failover %.1fms%n",
                    successful.size(),successful.size()/(result.elapsedNanos()/1e9),percentile(successful,99),
                    ((Number)report.get("leader_stop_to_confirmed_write_ms")).doubleValue());
            System.out.println("Raw samples: "+reports.resolve("requests.csv"));
            System.out.println("Report: "+reports.resolve("summary.json"));
        } catch(Exception|AssertionError failure) {
            report.put("failure",failure.toString()); report.put("failure_phase",budget.currentPhase());
            report.put("runtime_budget_expired",budget.expired());report.put("completed_at_utc",Instant.now().toString());
            writeJson(reports.resolve("summary.json"),report); throw failure;
        } finally {
            budget.close();
            for(Node node:nodes) node.stop();
        }
    }

    private static Result measure(List<String> urls,int clients,int seconds,int keys,int valueBytes,
                                  Map<String,String> expected,RuntimeBudget budget,Path samplePath,long seed) throws Exception {
        ExecutorService pool=Executors.newFixedThreadPool(clients);
        List<Sample> samples=java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger globalSequence=new AtomicInteger();
        long start=System.nanoTime(),deadline=start+TimeUnit.SECONDS.toNanos(seconds);
        List<Future<?>> futures=new ArrayList<>();
        for(int i=0;i<clients;i++) {
            final int worker=i;
            futures.add(pool.submit(()->{
                int ownKeys=(keys-1-worker)/clients+1,local=0;
                try(GrpcKvClient client=new GrpcKvClient(urls,new GrpcRpcClient(),Duration.ofSeconds(10),Duration.ofSeconds(6),seed+worker)) {
                    while(System.nanoTime()<deadline) {
                        budget.check();
                        int sequence=globalSequence.incrementAndGet();
                        String key="k"+(worker+clients*(local++%ownKeys)),value=value(sequence,valueBytes);
                        GrpcKvClient.Stats before=client.stats();
                        long began=System.nanoTime();
                        KvRpc.ClientCommandResponse response;
                        try {response=client.put(key,value);}
                        catch(RuntimeException failure) {
                            GrpcKvClient.Stats after=client.stats();
                            samples.add(new Sample(sequence,worker,key,began-start,System.nanoTime()-began,false,false,
                                    failure.toString(),after.rpcAttempts()-before.rpcAttempts(),
                                    after.retryRpcAttempts()-before.retryRpcAttempts(),after.backoffNanos()-before.backoffNanos()));
                            throw failure;
                        }
                        long elapsed=System.nanoTime()-began;
                        GrpcKvClient.Stats after=client.stats();
                        boolean correct=response.success()&&Objects.equals(response.value(),value);
                        samples.add(new Sample(sequence,worker,key,began-start,elapsed,correct,
                                response.success(),response.message(),after.rpcAttempts()-before.rpcAttempts(),
                                after.retryRpcAttempts()-before.retryRpcAttempts(),after.backoffNanos()-before.backoffNanos()));
                        if(correct) expected.put(key,value);
                    }
                }
            }));
        }
        long elapsed;
        try {
            for(Future<?> future:futures) future.get(Math.max(1,deadline+TimeUnit.SECONDS.toNanos(120)-System.nanoTime()),TimeUnit.NANOSECONDS);
            elapsed=System.nanoTime()-start;
        } finally {
            pool.shutdownNow();
            try { pool.awaitTermination(10,TimeUnit.SECONDS); }
            finally { writeSamples(samplePath,List.copyOf(samples)); }
        }
        return new Result(List.copyOf(samples),elapsed);
    }

    private static void verifyReadSamples(List<String> urls,Map<String,String> expected,int keys) {
        try(GrpcKvClient client=new GrpcKvClient(urls)) {
            for(int i=0;i<32;i++) {
                String key="k"+((long)i*keys/32);
                require("post-measure GET "+key,client.get(key),expected.get(key));
            }
            for(int i=0;i<8;i++) require("canary read",client.get("canary-"+i),"stable-"+i);
            require("missing key",client.get("verify-never-written"),null);
        }
    }

    private static Convergence converged(List<Node> nodes,Map<String,String> expected,Duration timeout) throws Exception {
        long deadline=System.nanoTime()+timeout.toNanos();
        while(System.nanoTime()<deadline) {
            // A newly elected leader may still be applying its committed prefix.
            // Refresh the reference rather than freezing its stale session table.
            List<Node> leaders=nodes.stream().filter(n->n.running&&n.node.role()==Role.LEADER).toList();
            if(leaders.size()==1) {
                Node elected=leaders.get(0);
                long term=elected.node.currentTerm();
                long index=elected.node.lastAppliedIndex();
                Map<String,ClientSession> sessions=elected.node.localSessions();
                boolean referenceReady=elected.node.role()==Role.LEADER&&elected.node.currentTerm()==term
                        &&elected.node.lastAppliedIndex()==index&&elected.node.localState().equals(expected)
                        &&elected.node.localSessions().equals(sessions);
                boolean all=referenceReady;
                if(referenceReady)for(Node node:nodes) {
                    if(!node.running||node.node.lastAppliedIndex()<index||!node.node.localState().equals(expected)
                            ||!node.node.localSessions().equals(sessions)) { all=false;break; }
                }
                if(all&&elected.node.role()==Role.LEADER&&elected.node.currentTerm()==term
                        &&elected.node.lastAppliedIndex()==index&&elected.node.localState().equals(expected)
                        &&elected.node.localSessions().equals(sessions))
                    return new Convergence(index,sessions.size());
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("All node states/sessions did not converge: "+nodes.stream()
                .map(n->n.peer.id()+"="+(n.running?n.node.statusJson():"stopped")+", keys="
                        +(n.running?n.node.localState().size():0)).toList());
    }

    private static Node leader(List<Node> nodes,Duration timeout) throws InterruptedException {
        long deadline=System.nanoTime()+timeout.toNanos();
        while(System.nanoTime()<deadline) {
            List<Node> leaders=nodes.stream().filter(n->n.running&&n.node.role()==Role.LEADER).toList();
            if(leaders.size()==1) return leaders.get(0);
            Thread.sleep(20);
        }
        throw new IllegalStateException("No unique live leader");
    }

    private static List<String> urls(List<Node> nodes,Node leader) {
        List<String> result=new ArrayList<>(); result.add(leader.peer.url());
        nodes.stream().filter(n->n.running&&n!=leader).forEach(n->result.add(n.peer.url())); return result;
    }

    private static long maximumTerm(List<Node> nodes) {
        return nodes.stream().filter(n->n.running).mapToLong(n->n.node.currentTerm()).max().orElse(0);
    }

    private static String value(int sequence,int bytes) {
        String digits=Integer.toString(sequence);
        return "v"+"0".repeat(bytes-1-digits.length())+digits;
    }

    private static void require(String operation,KvRpc.ClientCommandResponse result,String value) {
        if(!result.success()||!Objects.equals(result.value(),value)) throw new IllegalStateException(operation
                +" returned wrong value/status: "+result);
    }

    private static Double percentile(List<Sample> samples,int percentile) {
        if(samples.isEmpty()) return null;
        List<Long> sorted=samples.stream().map(Sample::latencyNanos).sorted().toList();
        return sorted.get(Math.max(0,(int)Math.ceil(percentile/100.0*sorted.size())-1))/1e6;
    }

    private static void writeSamples(Path path,List<Sample> samples) throws IOException {
        try(BufferedWriter out=Files.newBufferedWriter(path,StandardCharsets.UTF_8)) {
            out.write("sequence,client,key,start_offset_nanos,latency_nanos,success,response_success,message,rpc_attempts,retry_rpc_attempts,backoff_nanos\n");
            for(Sample sample:samples.stream().sorted(Comparator.comparingInt(Sample::sequence)).toList()) {
                out.write(sample.sequence()+","+sample.client()+","+sample.key()+","+sample.startOffsetNanos()+","
                        +sample.latencyNanos()+","+sample.success()+","+sample.responseSuccess()+",\""
                        +(sample.message()==null?"":sample.message().replace("\"","\"\"").replace("\n"," "))+"\","
                        +sample.rpcAttempts()+","+sample.retryRpcAttempts()+","+sample.backoffNanos()+"\n");
            }
        }
    }

    private static void writeJson(Path path,Map<String,Object> report) throws IOException {
        StringBuilder text=new StringBuilder("{\n"); int i=0;
        for(Map.Entry<String,Object> entry:report.entrySet()) {
            if(i++>0) text.append(",\n");
            text.append("  \"").append(escape(entry.getKey())).append("\": ");
            Object value=entry.getValue();
            if(value==null) text.append("null");
            else if(value instanceof Number||value instanceof Boolean) text.append(value);
            else text.append('"').append(escape(value.toString())).append('"');
        }
        Files.writeString(path,text.append("\n}\n").toString(),StandardCharsets.UTF_8);
    }
    private static String escape(String value) { return value.replace("\\","\\\\").replace("\"","\\\"")
            .replace("\n","\\n").replace("\r","\\r").replace("\t","\\t"); }

    private static List<Integer> reservePorts(int count) throws IOException {
        List<ServerSocket> reservations=new ArrayList<>(); List<Integer> ports=new ArrayList<>();
        try {for(int i=0;i<count;i++){ServerSocket socket=new ServerSocket(0);reservations.add(socket);ports.add(socket.getLocalPort());}}
        finally {for(ServerSocket socket:reservations)socket.close();} return ports;
    }
    private static Map<String,String> options(String[] args) {
        Map<String,String> result=new LinkedHashMap<>();
        Set<String> allowed=Set.of("--duration","--clients","--keys","--value-bytes","--seed","--report-dir","--max-runtime-seconds",
                "--election-min-ms","--election-max-ms","--heartbeat-ms");
        for(int i=0;i<args.length;i+=2) {
            if(!args[i].startsWith("--")||i+1>=args.length)throw new IllegalArgumentException("Expected --option value pairs");
            if(!allowed.contains(args[i]))throw new IllegalArgumentException("Unknown option "+args[i]);
            if(result.put(args[i],args[i+1])!=null)throw new IllegalArgumentException("Duplicate option "+args[i]);
        } return result;
    }

    /** Caps this owned benchmark run; cleanup remains mandatory after cancellation. */
    private static final class RuntimeBudget implements AutoCloseable {
        private final long started=System.nanoTime();
        private final long deadline;
        private final java.util.concurrent.ScheduledExecutorService watchdog;
        private volatile String phase="initialization";
        private volatile boolean expired;
        RuntimeBudget(int seconds) {
            deadline=started+TimeUnit.SECONDS.toNanos(seconds);
            Thread runner=Thread.currentThread();
            watchdog=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"benchmark-runtime-guard");t.setDaemon(true);return t;});
            watchdog.schedule(()->{expired=true;System.err.println("Runtime budget exhausted during "+phase);runner.interrupt();},seconds,TimeUnit.SECONDS);
        }
        void phase(String value){phase=value;check();System.out.println("Phase: "+value);}
        void check(){if(expired||System.nanoTime()>=deadline||Thread.currentThread().isInterrupted())throw new IllegalStateException("Benchmark runtime budget/cancellation in "+phase);}
        double elapsedSeconds(){return (System.nanoTime()-started)/1e9;}
        String currentPhase(){return phase;}
        boolean expired(){return expired;}
        @Override public void close(){watchdog.shutdownNow();if(expired)Thread.interrupted();}
    }

    private record Result(List<Sample> samples,long elapsedNanos) {}
    private record Sample(int sequence,int client,String key,long startOffsetNanos,long latencyNanos,
                          boolean success,boolean responseSuccess,String message,long rpcAttempts,long retryRpcAttempts,long backoffNanos) {}
    private record Convergence(long index,int sessions) {}
    private static final class Node {
        final Peer peer; final Map<String,Peer> peers; final Path directory; final long seed; final ElectionTiming timing;
        RaftNode node; RaftGrpcServer server; boolean running;
        Node(Peer peer,Map<String,Peer> peers,Path directory,long seed,ElectionTiming timing) {
            this.peer=peer;this.peers=peers;this.directory=directory;this.seed=seed;this.timing=timing;
        }
        void start() throws IOException {
            node=new RaftNode(peer.id(),peer.url(),peers,directory,SNAPSHOT_THRESHOLD,new GrpcRpcClient(),seed+peer.id().hashCode(),timing);
            server=new RaftGrpcServer(node);server.start();node.start();running=true;
        }
        void stop() {if(running){server.stop();node.stop();running=false;}}
    }
}
