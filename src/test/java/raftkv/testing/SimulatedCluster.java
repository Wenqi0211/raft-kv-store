package raftkv.testing;

import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.raft.Peer;
import raftkv.raft.RaftNode;
import raftkv.raft.Role;
import raftkv.rpc.SimulatedNetwork;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

public final class SimulatedCluster implements AutoCloseable {
    public final SimulatedNetwork network=new SimulatedNetwork();
    public final Map<String,Peer> peers=new LinkedHashMap<>();
    public final Map<String,RaftNode> live=new LinkedHashMap<>();
    private final Path directory;
    private final long seed;
    public SimulatedCluster(Path directory,long seed,int count) throws Exception {
        this.directory=directory;this.seed=seed;Files.createDirectories(directory);
        for(int i=1;i<=count;i++) peers.put("n"+i,new Peer("n"+i,"sim://n"+i));
        try { for(String id:peers.keySet()) start(id); }
        catch(Exception failure) { close();throw failure; }
    }
    public void start(String id) throws Exception {
        String url=peers.get(id).url();
        RaftNode node=new RaftNode(id,url,peers,directory.resolve(id),8,network.transport(url,seed+id.hashCode()),seed+id.hashCode());
        network.register(url,node);live.put(id,node);node.start();
    }
    public void stop(String id) {
        RaftNode node=live.remove(id);
        if(node!=null) { network.unregister(node.url());node.stop(); }
    }
    public RaftNode leader() throws Exception { return leader(live.keySet()); }
    public RaftNode leader(Set<String> eligible) throws Exception {
        long deadline=System.nanoTime()+8_000_000_000L;
        while(System.nanoTime()<deadline) {
            List<RaftNode> leaders=live.values().stream().filter(n->eligible.contains(n.id())&&n.role()==Role.LEADER).toList();
            if(leaders.size()==1) return leaders.get(0);
            Thread.sleep(10);
        }
        throw new AssertionError("no leader: "+live.values().stream().map(RaftNode::statusJson).toList());
    }
    public KvRpc.ClientCommandResponse execute(Command command) throws Exception { return execute(command,Set.copyOf(live.keySet())); }
    public KvRpc.ClientCommandResponse execute(Command command,Set<String> eligible) throws Exception {
        long deadline=System.nanoTime()+12_000_000_000L;
        KvRpc.ClientCommandResponse last=null;
        while(System.nanoTime()<deadline) {
            last=leader(eligible).submit(command);
            if(last.success()) return last;
            Thread.sleep(10);
        }
        throw new AssertionError("command failed: "+last);
    }
    public void converged() throws Exception {
        RaftNode leader=leader();Map<String,String> expected=leader.localState();var sessions=leader.localSessions();
        long target=leader.lastAppliedIndex();
        await(()->live.values().stream().allMatch(n->n.lastAppliedIndex()>=target&&n.localState().equals(expected)&&n.localSessions().equals(sessions)),8000);
    }
    public static void await(BooleanSupplier condition,long millis) throws Exception {
        long deadline=System.nanoTime()+millis*1_000_000;
        while(System.nanoTime()<deadline) { if(condition.getAsBoolean())return;Thread.sleep(10); }
        throw new AssertionError("condition not met within "+millis+"ms");
    }
    @Override public void close() {
        for(String id:new ArrayList<>(live.keySet())) stop(id);
        network.close();
    }
}
