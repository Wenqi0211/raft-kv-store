package raftkv.rpc;

import raftkv.kv.KvRpc;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftRpc;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** In-process wire-equivalent fault transport. It never bypasses Raft handlers. */
public final class SimulatedNetwork implements AutoCloseable {
    private final Map<String,RaftNode> endpoints = new ConcurrentHashMap<>();
    private final Map<String,AtomicLong> issued = new ConcurrentHashMap<>();
    private final Map<String,AtomicLong> deliveredSequence = new ConcurrentHashMap<>();
    private final Map<String,AtomicInteger> forcedDrops = new ConcurrentHashMap<>();
    private final Map<String,ConcurrentLinkedQueue<Long>> forcedDelays = new ConcurrentHashMap<>();
    private volatile Set<String> isolated = Set.of();
    private volatile double dropProbability;
    private volatile int maxDelayMillis;
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong reordered = new AtomicLong();
    // A stopped sender must not interrupt a different node's durable write.
    private final java.util.concurrent.ExecutorService receivers=java.util.concurrent.Executors.newCachedThreadPool(r->{
        Thread thread=new Thread(r,"simulated-rpc-receiver");thread.setDaemon(true);return thread;
    });

    public void register(String url,RaftNode node) { endpoints.put(url,node); }
    public void unregister(String url) { endpoints.remove(url); }
    public void partition(Set<String> urls) { isolated=Set.copyOf(urls); }
    public void heal() { isolated=Set.of(); dropProbability=0; maxDelayMillis=0; }
    public void faults(double probability,int maxDelay) {
        if(probability<0 || probability>1 || maxDelay<0) throw new IllegalArgumentException();
        dropProbability=probability; maxDelayMillis=maxDelay;
    }
    public void dropNext(String from,String to,int count) { forcedDrops.put(edge(from,to),new AtomicInteger(count)); }
    public void delayNext(String from,String to,long millis) {
        forcedDelays.computeIfAbsent(edge(from,to),k->new ConcurrentLinkedQueue<>()).add(millis);
    }
    public long deliveredCount() { return delivered.get(); }
    public long droppedCount() { return dropped.get(); }
    public long reorderedCount() { return reordered.get(); }

    public RaftTransport transport(String from,long seed) {
        return new RaftTransport() {
            private final Random random = new Random(seed);
            @Override public <T> T postObject(String to,String path,Serializable request,Class<T> type,Duration timeout)
                    throws IOException,InterruptedException {
                String key=edge(from,to);
                long order=issued.computeIfAbsent(key,k->new AtomicLong()).incrementAndGet();
                AtomicInteger remaining=forcedDrops.get(key);
                boolean forceDrop=remaining!=null && remaining.getAndUpdate(v->Math.max(0,v-1))>0;
                double roll;int jitter;
                synchronized(random) { roll=random.nextDouble(); jitter=maxDelayMillis==0?0:random.nextInt(maxDelayMillis+1); }
                if(forceDrop || isolated.contains(from)!=isolated.contains(to) || roll<dropProbability) {
                    dropped.incrementAndGet(); throw new IOException("simulated message loss/partition "+key);
                }
                ConcurrentLinkedQueue<Long> queue=forcedDelays.get(key);
                Long forced=queue==null?null:queue.poll();
                long delay=forced==null?jitter:forced;
                if(delay>=timeout.toMillis()) {
                    Thread.sleep(timeout.toMillis()); dropped.incrementAndGet(); throw new IOException("simulated deadline");
                }
                if(delay>0) Thread.sleep(delay);
                RaftNode node=endpoints.get(to);
                if(node==null) { dropped.incrementAndGet(); throw new IOException("simulated crashed node "+to); }
                if(isolated.contains(from)!=isolated.contains(to)) {
                    dropped.incrementAndGet(); throw new IOException("partition changed while message was delayed");
                }
                long prior=deliveredSequence.computeIfAbsent(key,k->new AtomicLong()).getAndAccumulate(order,Math::max);
                if(order<prior) reordered.incrementAndGet();
                Serializable decoded=(Serializable)BinaryCodec.decode(BinaryCodec.encode(request));
                java.util.concurrent.Future<Serializable> delivery=receivers.submit(()->{
                    try {
                        return switch(path) {
                            case "/rpc/requestVote" -> node.handleRequestVote((RaftRpc.RequestVoteRequest)decoded);
                            case "/rpc/appendEntries" -> node.handleAppendEntries((RaftRpc.AppendEntriesRequest)decoded);
                            case "/rpc/installSnapshot" -> node.handleInstallSnapshot((RaftRpc.InstallSnapshotRequest)decoded);
                            case "/client" -> node.submit(((KvRpc.ClientCommandRequest)decoded).command());
                            default -> throw new IOException("unknown simulated RPC "+path);
                        };
                    } catch(IllegalStateException stopped) { throw new IOException("destination stopped",stopped); }
                });
                Serializable response;
                try {
                    response=delivery.get(timeout.toNanos(),java.util.concurrent.TimeUnit.NANOSECONDS);
                } catch(java.util.concurrent.ExecutionException failure) { throw new IOException("destination RPC failed",failure.getCause()); }
                catch(java.util.concurrent.TimeoutException expired) { throw new IOException("simulated RPC deadline",expired); }
                delivered.incrementAndGet();
                return type.cast(BinaryCodec.decode(BinaryCodec.encode(response)));
            }
            @Override public void close() { }
        };
    }
    @Override public void close() { receivers.shutdownNow(); }
    private static String edge(String from,String to) { return from+" -> "+to; }
}
