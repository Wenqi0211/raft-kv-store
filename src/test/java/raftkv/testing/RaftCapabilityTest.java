package raftkv.testing;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import raftkv.kv.Command;
import raftkv.kv.KVStateMachine;
import raftkv.raft.Peer;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftRpc;
import raftkv.rpc.RaftTransport;
import raftkv.rpc.SimulatedNetwork;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RaftCapabilityTest {
    @TempDir Path directory;

    @Test void duplicateAndStaleCommandsDoNotRepeatEffectsAndSessionsSurviveSnapshot() {
        KVStateMachine machine=new KVStateMachine();
        Command original=Command.client(Command.Type.PUT,"k","first","a",1);
        assertEquals("first",machine.apply(original).value());
        machine.apply(Command.client(Command.Type.PUT,"k","newer","b",1));
        assertEquals("first",machine.apply(original).value());
        assertEquals("newer",machine.snapshot().get("k"));
        KVStateMachine restored=new KVStateMachine();restored.restore(machine.snapshot(),machine.sessionSnapshot());
        assertEquals("first",restored.apply(original).value());
        assertEquals("newer",restored.snapshot().get("k"));
        assertFalse(restored.apply(Command.client(Command.Type.PUT,"k","other","a",1)).success());
        assertEquals("newer",restored.apply(Command.client(Command.Type.DELETE,"k",null,"a",2)).value());
        assertEquals("newer",restored.apply(Command.client(Command.Type.DELETE,"k",null,"a",2)).value());
        assertFalse(restored.apply(original).success());
        assertNull(restored.snapshot().get("k"));
    }

    @Test void delayedOlderVoteIsDeliveredAfterNewerVoteAndCannotRollBackTerm() throws Exception {
        SimulatedNetwork network=new SimulatedNetwork();String url="sim://destination";
        RaftNode node=new RaftNode("n",url,Map.of("n",new Peer("n",url)),directory.resolve("reorder"),8,network.transport(url,1),1);
        network.register(url,node);RaftTransport sender=network.transport("sim://sender",2);
        network.delayNext("sim://sender",url,150);
        var pool=Executors.newFixedThreadPool(2);
        try {
            var older=pool.submit(()->sender.postObject(url,"/rpc/requestVote",new RaftRpc.RequestVoteRequest(10,"old",0,0),RaftRpc.RequestVoteResponse.class,Duration.ofSeconds(1)));
            Thread.sleep(30);
            var newer=sender.postObject(url,"/rpc/requestVote",new RaftRpc.RequestVoteRequest(20,"new",0,0),RaftRpc.RequestVoteResponse.class,Duration.ofSeconds(1));
            assertTrue(newer.voteGranted());assertEquals(20,older.get().term());assertFalse(older.get().voteGranted());
            assertTrue(network.reorderedCount()>0);
            network.dropNext("sim://sender",url,1);
            assertThrows(java.io.IOException.class,()->sender.postObject(url,"/rpc/requestVote",new RaftRpc.RequestVoteRequest(21,"new",0,0),RaftRpc.RequestVoteResponse.class,Duration.ofSeconds(1)));
            assertEquals(1,network.droppedCount());
        } finally {pool.shutdownNow();node.stop();network.close();}
    }

    @Test void minorityCannotAcknowledgeStaleReadWhileMajorityMakesProgress() throws Exception {
        try(SimulatedCluster cluster=new SimulatedCluster(directory.resolve("partition"),9001,5)) {
            cluster.execute(Command.put("k","before"));
            RaftNode old=cluster.leader();String companion=cluster.live.keySet().stream().filter(id->!id.equals(old.id())).findFirst().orElseThrow();
            Set<String> isolated=Set.of(old.url(),cluster.peers.get(companion).url());cluster.network.partition(isolated);
            Set<String> majority=cluster.live.keySet().stream().filter(id->!isolated.contains(cluster.peers.get(id).url())).collect(java.util.stream.Collectors.toSet());
            var pool=Executors.newSingleThreadExecutor();
            try {
                var stale=pool.submit(()->old.submit(Command.get("k")));
                cluster.execute(Command.put("k","after"),majority);
                assertEquals("after",cluster.execute(Command.get("k"),majority).value());
                assertFalse(stale.get(7,TimeUnit.SECONDS).success());
            } finally {pool.shutdownNow();cluster.network.heal();}
            cluster.converged();assertEquals("after",cluster.execute(Command.get("k")).value());
        }
    }

    @Test void concurrentGetPutDeleteHistoryHasAValidLinearization() throws Exception {
        try(SimulatedCluster cluster=new SimulatedCluster(directory.resolve("history"),9010,5)) {
            RaftNode leader=cluster.leader();List<Linearizability.Operation> history=Collections.synchronizedList(new ArrayList<>());
            var pool=Executors.newFixedThreadPool(3);var start=new java.util.concurrent.CountDownLatch(1);
            try {
                List<java.util.concurrent.Future<?>> tasks=new ArrayList<>();
                for(int worker=0;worker<3;worker++) { final int client=worker;
                    tasks.add(pool.submit(()->{start.await();
                        for(int step=0;step<3;step++) {
                            Command.Type type=step==0?Command.Type.PUT:step==1?Command.Type.GET:Command.Type.DELETE;
                            Command command=Command.client(type,"shared",type==Command.Type.PUT?"v"+client:null,"history-"+client,step+1);
                            long invoked=System.nanoTime();var response=leader.submit(command);long completed=System.nanoTime();
                            assertTrue(response.success(),response.message());history.add(new Linearizability.Operation(invoked,completed,type,"shared",command.value(),response.value()));
                        }return null;}));
                }
                start.countDown();for(var task:tasks)task.get(20,TimeUnit.SECONDS);
            }finally {pool.shutdownNow();}
            assertEquals(9,history.size());assertTrue(Linearizability.check(history));
            assertFalse(Linearizability.check(List.of(new Linearizability.Operation(1,2,Command.Type.GET,"never",null,"impossible"))));
        }
    }

    @Test void allFiveNodesRestoreDeduplicationAfterDiskRestartAndCompaction() throws Exception {
        try(SimulatedCluster cluster=new SimulatedCluster(directory.resolve("durable-dedupe"),9020,5)) {
            Command original=Command.client(Command.Type.PUT,"k","first","durable",1);
            cluster.execute(original);
            cluster.execute(Command.client(Command.Type.PUT,"k","newer","other",1));
            for(int i=0;i<12;i++)cluster.execute(Command.client(Command.Type.PUT,"compact-"+i,"v","compact-client",i+1));
            cluster.converged();
            for(String id:new ArrayList<>(cluster.live.keySet()))cluster.stop(id);
            for(String id:cluster.peers.keySet())cluster.start(id);
            assertEquals("first",cluster.execute(original).value());
            assertEquals("newer",cluster.execute(Command.get("k")).value());
            cluster.converged();
        }
    }

    @Test void historyCheckerRejectsRealTimeContradictionsAndAllowsConcurrentReadBeforeWrite() {
        var completedPut=new Linearizability.Operation(1,2,Command.Type.PUT,"k","v","v");
        assertFalse(Linearizability.check(List.of(completedPut,
                new Linearizability.Operation(3,4,Command.Type.GET,"k",null,null))));
        assertTrue(Linearizability.check(List.of(new Linearizability.Operation(1,4,Command.Type.PUT,"k","v","v"),
                new Linearizability.Operation(2,3,Command.Type.GET,"k",null,null))));
    }

    @TestFactory Stream<DynamicTest> seededFiveNodeLossRestartAndSnapshotRecovery() {
        int seeds=Integer.getInteger("raft.seed.count",3);
        return IntStream.range(0,seeds).mapToObj(seed->DynamicTest.dynamicTest("five-node fault recovery seed="+seed,()->runFaultScenario(seed)));
    }

    private void runFaultScenario(int seed) throws Exception {
        try(SimulatedCluster cluster=new SimulatedCluster(directory.resolve("seed-"+seed),seed,5)) {
            cluster.execute(Command.client(Command.Type.PUT,"stable","before","stable-client",1));
            RaftNode leader=cluster.leader();List<String> followers=new ArrayList<>(cluster.live.keySet());followers.remove(leader.id());
            Collections.shuffle(followers,new java.util.Random(seed));String second=followers.get(0);
            cluster.stop(leader.id());cluster.stop(second);
            cluster.network.faults(.06,12);
            RaftNode replacement=cluster.leader();String target=cluster.live.values().stream().filter(n->!n.id().equals(replacement.id())).findFirst().orElseThrow().url();
            cluster.network.dropNext(replacement.url(),target,2);
            Command retry=Command.client(Command.Type.PUT,"stable","after","stable-client",2);
            assertEquals("after",cluster.execute(retry).value());
            cluster.execute(Command.client(Command.Type.PUT,"stable","newer","other-client",1));
            assertEquals("after",cluster.execute(retry).value());
            assertEquals("newer",cluster.execute(Command.client(Command.Type.GET,"stable",null,"stable-client",3)).value());
            assertEquals("newer",cluster.execute(Command.client(Command.Type.DELETE,"stable",null,"stable-client",4)).value());
            assertEquals("newer",cluster.execute(Command.client(Command.Type.DELETE,"stable",null,"stable-client",4)).value());
            for(int i=0;i<12;i++)cluster.execute(Command.client(Command.Type.PUT,"key-"+i,"value-"+seed+"-"+i,"writer",i+1));
            cluster.network.heal();cluster.start(leader.id());cluster.start(second);cluster.converged();
            assertTrue(cluster.live.values().stream().anyMatch(n->n.snapshotIndex()>0));
            assertNull(cluster.execute(Command.get("stable")).value());
            assertEquals("newer",cluster.execute(Command.client(Command.Type.DELETE,"stable",null,"stable-client",4)).value());
            assertTrue(cluster.network.droppedCount()>0);
        }
    }
}
