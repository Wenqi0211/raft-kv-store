package raftkv.rpc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import raftkv.client.GrpcKvClient;
import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.raft.Peer;
import raftkv.raft.ElectionTiming;
import raftkv.raft.RaftNode;
import raftkv.raft.Role;
import raftkv.server.RaftGrpcServer;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Real loopback HTTP/2 checks, separate from the simulated fault-network suite. */
final class GrpcTransportTest {
    @TempDir Path directory;

    @Test
    @Timeout(90)
    void fiveNodeGrpcCrudDeduplicationTwoFailuresAndDurableRecovery() throws Exception {
        Map<String,Peer> peers=new LinkedHashMap<>();
        List<ServerSocket> reservations=new ArrayList<>();
        try {
            for(int i=1;i<=5;i++) {
                ServerSocket socket=new ServerSocket(0);reservations.add(socket);
                String id="n"+i;peers.put(id,new Peer(id,"grpc://127.0.0.1:"+socket.getLocalPort()));
            }
        } finally { for(ServerSocket socket:reservations)socket.close(); }
        List<Slot> nodes=new ArrayList<>();
        for(Peer peer:peers.values())nodes.add(new Slot(peer,peers,directory.resolve(peer.id())));
        try {
            for(Slot node:nodes)node.start();
            Slot elected=leader(nodes);
            Command delete=Command.client(Command.Type.DELETE,"probe",null,"stable-delete-client",1);
            Map<String,String> expected=new LinkedHashMap<>();
            try(GrpcKvClient client=new GrpcKvClient(urls(nodes,elected))) {
                value(client.put("probe","initial"),"initial");
                value(client.get("probe"),"initial");
                value(client.execute(delete),"initial");
                value(client.execute(delete),"initial");
                value(client.get("probe"),null);
                value(client.put("probe","new-value"),"new-value");
                expected.put("probe","new-value");
                value(client.execute(delete),"initial");
                value(client.get("probe"),"new-value");
                for(int i=0;i<15;i++) {
                    value(client.put("seed-"+i,"v"+i),"v"+i);expected.put("seed-"+i,"v"+i);
                }
            }
            converged(nodes,expected);
            assertTrue(nodes.stream().anyMatch(n->n.node.snapshotIndex()>0));

            elected=leader(nodes);elected.stop();
            Slot second=nodes.stream().filter(n->n.up).findFirst().orElseThrow();second.stop();
            Slot replacement=leader(nodes);
            try(GrpcKvClient client=new GrpcKvClient(urls(nodes,replacement))) {
                for(int i=0;i<25;i++) {
                    value(client.put("two-offline-"+i,"v"+i),"v"+i);expected.put("two-offline-"+i,"v"+i);
                }
                value(client.execute(delete),"initial");
                value(client.get("probe"),"new-value");
                assertFalse(client.execute(Command.client(Command.Type.DELETE,"different-key",null,
                        "stable-delete-client",1)).success(),"same client/sequence must not mean a different command");
            }
            elected.start();second.start();converged(nodes,expected);

            for(Slot node:nodes)node.stop();
            for(Slot node:nodes)node.start();
            replacement=leader(nodes);
            try(GrpcKvClient client=new GrpcKvClient(urls(nodes,replacement))) {
                value(client.execute(delete),"initial");
                value(client.get("probe"),"new-value");
                value(client.get("missing"),null);
            }
            converged(nodes,expected);
        } finally { for(Slot node:nodes)node.stop(); }
    }

    @Test
    @Timeout(75)
    void lostCommittedReplyAndLeaderLossRetryTheSameDeleteWithoutDeletingANewerValue() throws Exception {
        Map<String,Peer> peers=new LinkedHashMap<>();
        List<ServerSocket> reservations=new ArrayList<>();
        try {
            for(int i=1;i<=5;i++) {
                ServerSocket socket=new ServerSocket(0);reservations.add(socket);
                String id="retry-n"+i;peers.put(id,new Peer(id,"grpc://127.0.0.1:"+socket.getLocalPort()));
            }
        } finally {for(ServerSocket socket:reservations)socket.close();}
        List<Slot> nodes=new ArrayList<>();
        for(Peer peer:peers.values())nodes.add(new Slot(peer,peers,directory.resolve(peer.id())));
        Command logical=Command.client(Command.Type.DELETE,"probe",null,"lost-reply-delete",1);
        AtomicBoolean injected=new AtomicBoolean();AtomicReference<Slot> stopped=new AtomicReference<>();
        List<Command> attemptedCommands=new ArrayList<>();
        RaftTransport loseCommittedReply=new RaftTransport() {
            final GrpcRpcClient real=new GrpcRpcClient();
            @Override public <T>T postObject(String url,String path,Serializable request,Class<T> type,Duration timeout)
                    throws IOException,InterruptedException {
                Command sent=((KvRpc.ClientCommandRequest)request).command();attemptedCommands.add(sent);
                T response=real.postObject(url,path,request,type,timeout);
                if(response instanceof KvRpc.ClientCommandResponse result&&result.success()
                        &&sent.equals(logical)&&injected.compareAndSet(false,true)) {
                    assertEquals("initial",result.value(),"the original DELETE must commit before losing its reply");
                    Slot answered=nodes.stream().filter(node->node.peer.url().equals(url)).findFirst().orElseThrow();
                    answered.stop();stopped.set(answered);
                    Slot replacement=leader(nodes);
                    try(GrpcKvClient otherClient=new GrpcKvClient(urls(nodes,replacement))) {
                        value(otherClient.put("probe","replacement"),"replacement");
                    }
                    // The real RPC completed successfully. Only its reply is hidden from
                    // the caller; a new value is committed before that caller can retry.
                    throw new IOException("injected lost committed DELETE reply followed by leader loss");
                }
                return response;
            }
            @Override public void close(){real.close();}
        };
        try {
            for(Slot node:nodes)node.start();
            Slot initialLeader=leader(nodes);
            try(GrpcKvClient bootstrap=new GrpcKvClient(urls(nodes,initialLeader))) {
                value(bootstrap.put("probe","initial"),"initial");
            }
            converged(nodes,Map.of("probe","initial"));
            try(GrpcKvClient retrying=new GrpcKvClient(urls(nodes,initialLeader),loseCommittedReply,
                    Duration.ofSeconds(10),Duration.ofSeconds(6),20261024)) {
                value(retrying.execute(logical),"initial");
                assertTrue(injected.get());assertNotNull(stopped.get());
                assertTrue(attemptedCommands.size()>=2,"the lost reply must cause a genuine client retry");
                attemptedCommands.forEach(command->{
                    assertSame(logical,command);assertEquals("lost-reply-delete",command.clientId());
                    assertEquals(1,command.sequence());assertEquals("lost-reply-delete:1",command.requestId());
                });
                GrpcKvClient.Stats stats=retrying.stats();
                assertEquals(1,stats.logicalRequests());assertEquals(1,stats.successfulRequests());assertEquals(0,stats.failedRequests());
                assertTrue(stats.rpcAttempts()>=2);assertEquals(stats.rpcAttempts()-1,stats.retryRpcAttempts());
                try(GrpcKvClient verifier=new GrpcKvClient(urls(nodes,leader(nodes)))) {
                    value(verifier.get("probe"),"replacement");
                }
                stopped.get().start();converged(nodes,Map.of("probe","replacement"));
                value(retrying.execute(logical),"initial");
                try(GrpcKvClient verifier=new GrpcKvClient(urls(nodes,leader(nodes)))) {
                    value(verifier.get("probe"),"replacement");
                }
                converged(nodes,Map.of("probe","replacement"));
            }
        } finally {for(Slot node:nodes)node.stop();loseCommittedReply.close();}
    }

    @Test
    void descriptorsMatchDeclaredUnaryGrpcSchema() {
        assertEquals("raftkv.RaftService/RequestVote",GrpcRpcClient.methodForPath("/rpc/requestVote").getFullMethodName());
        assertEquals("raftkv.RaftService/AppendEntries",GrpcRpcClient.methodForPath("/rpc/appendEntries").getFullMethodName());
        assertEquals("raftkv.RaftService/InstallSnapshot",GrpcRpcClient.methodForPath("/rpc/installSnapshot").getFullMethodName());
        assertEquals("raftkv.RaftService/Client",GrpcRpcClient.methodForPath("/client").getFullMethodName());
        assertThrows(IllegalArgumentException.class,()->GrpcRpcClient.methodForPath("/unknown"));
    }

    private static void value(KvRpc.ClientCommandResponse result,String expected) {
        assertTrue(result.success(),result.message());assertEquals(expected,result.value());
    }
    private static Slot leader(List<Slot> nodes) throws InterruptedException {
        long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
        while(System.nanoTime()<deadline) {
            List<Slot> elected=nodes.stream().filter(n->n.up&&n.node.role()==Role.LEADER).toList();
            if(elected.size()==1)return elected.get(0);
            Thread.sleep(20);
        }
        throw new AssertionError("No unique gRPC leader");
    }
    private static void converged(List<Slot> nodes,Map<String,String> expected) throws InterruptedException {
        Slot elected=leader(nodes);long applied=elected.node.lastAppliedIndex();
        var sessions=elected.node.localSessions();
        long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
        while(System.nanoTime()<deadline) {
            if(nodes.stream().allMatch(n->n.up&&n.node.lastAppliedIndex()>=applied
                    &&n.node.localState().equals(expected)&&n.node.localSessions().equals(sessions)))return;
            Thread.sleep(20);
        }
        fail("Five gRPC replicas did not converge in KV state, dedup sessions, and applied index");
    }
    private static List<String> urls(List<Slot> nodes,Slot leader) {
        List<String> urls=new ArrayList<>();urls.add(leader.peer.url());
        nodes.stream().filter(n->n.up&&n!=leader).forEach(n->urls.add(n.peer.url()));return urls;
    }
    private static final class Slot {
        final Peer peer;final Map<String,Peer> peers;final Path directory;
        RaftNode node;RaftGrpcServer server;boolean up;
        Slot(Peer peer,Map<String,Peer> peers,Path directory){this.peer=peer;this.peers=peers;this.directory=directory;}
        void start() throws IOException {
            node=new RaftNode(peer.id(),peer.url(),peers,directory,5,new GrpcRpcClient(),20261024L+peer.id().hashCode(),ElectionTiming.durableGrpc());
            server=new RaftGrpcServer(node);server.start();node.start();up=true;
        }
        void stop(){if(up){server.stop();node.stop();up=false;}}
    }
}
