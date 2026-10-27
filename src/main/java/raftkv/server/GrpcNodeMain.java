package raftkv.server;

import raftkv.raft.Peer;
import raftkv.raft.ElectionTiming;
import raftkv.raft.RaftNode;
import raftkv.rpc.GrpcRpcClient;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** Standalone, independently persisted gRPC Raft node, suitable for separate JVMs. */
public final class GrpcNodeMain {
    private GrpcNodeMain() { }
    public static void main(String[] args) throws Exception {
        Config config=parse(args);
        RaftNode node=new RaftNode(config.id(),config.url(),config.peers(),config.directory(),
                config.snapshotThreshold(),new GrpcRpcClient(),config.seed(),config.timing());
        RaftGrpcServer server=new RaftGrpcServer(node);
        AtomicBoolean stopped=new AtomicBoolean();
        Runnable cleanup=()->{if(stopped.compareAndSet(false,true)){server.stop();node.stop();}};
        Runtime.getRuntime().addShutdownHook(new Thread(cleanup,"grpc-node-shutdown"));
        try {
            server.start();node.start();
            System.out.println("gRPC Raft node "+config.id()+" listening at "+config.url());
            System.out.println("Fixed membership: "+config.peers().keySet());
            System.out.println("Election timing: "+config.timing());
            System.out.println("Durable state directory: "+config.directory().toAbsolutePath());
            new CountDownLatch(1).await();
        } finally {cleanup.run();}
    }

    static Config parse(String[] args) {
        Map<String,String> options=new LinkedHashMap<>();
        Set<String> allowed=Set.of("--id","--cluster","--url","--data-dir","--snapshot-threshold","--seed","--election-min-ms","--election-max-ms","--heartbeat-ms");
        for(int i=0;i<args.length;i+=2) {
            if(!allowed.contains(args[i])||i+1>=args.length)throw new IllegalArgumentException("Expected known --option value pairs");
            if(options.put(args[i],args[i+1])!=null)throw new IllegalArgumentException("Duplicate option: "+args[i]);
        }
        String id=options.get("--id");
        if(id==null||id.isBlank())throw new IllegalArgumentException("--id is required, for example --id n1");
        String cluster=options.getOrDefault("--cluster",defaultCluster());
        Set<String> identifiers=new HashSet<>();
        for(String member:cluster.split(",")) {
            String identifier=member.trim().split("=",2)[0];
            if(identifier.isBlank()||!identifiers.add(identifier))throw new IllegalArgumentException("Blank/duplicate peer ID");
        }
        Map<String,Peer> peers=ClusterConfig.parse(cluster);
        if(!peers.containsKey(id))throw new IllegalArgumentException("--id must be present in the fixed --cluster membership");
        for(Peer peer:peers.values()) {
            URI uri=URI.create(peer.url());
            if(!"grpc".equals(uri.getScheme())||uri.getHost()==null||uri.getPort()<1||uri.getPort()>65535
                    ||uri.getQuery()!=null||uri.getFragment()!=null||uri.getUserInfo()!=null
                    ||(uri.getPath()!=null&&!uri.getPath().isEmpty()))
                throw new IllegalArgumentException("Peer URLs must be grpc://host:port: "+peer.url());
        }
        String url=options.getOrDefault("--url",peers.get(id).url());
        if(!url.equals(peers.get(id).url()))throw new IllegalArgumentException("--url must match this peer's --cluster URL");
        int threshold=Integer.parseInt(options.getOrDefault("--snapshot-threshold","40"));
        if(threshold<1)throw new IllegalArgumentException("Snapshot threshold must be positive");
        Path directory=Path.of(options.getOrDefault("--data-dir","work/grpc-nodes/"+id));
        long seed=Long.parseLong(options.getOrDefault("--seed",Long.toString(System.nanoTime())));
        ElectionTiming defaults=ElectionTiming.durableGrpc();
        ElectionTiming timing=new ElectionTiming(Integer.parseInt(options.getOrDefault("--election-min-ms",Integer.toString(defaults.minimumMillis()))),
                Integer.parseInt(options.getOrDefault("--election-max-ms",Integer.toString(defaults.maximumExclusiveMillis()))),
                Integer.parseInt(options.getOrDefault("--heartbeat-ms",Integer.toString(defaults.heartbeatMillis()))));
        return new Config(id,url,Map.copyOf(peers),directory,threshold,seed,timing);
    }
    private static String defaultCluster() {
        return "n1=grpc://127.0.0.1:8101,n2=grpc://127.0.0.1:8102,n3=grpc://127.0.0.1:8103,"
                +"n4=grpc://127.0.0.1:8104,n5=grpc://127.0.0.1:8105";
    }
    record Config(String id,String url,Map<String,Peer> peers,Path directory,int snapshotThreshold,long seed,ElectionTiming timing) { }
}
