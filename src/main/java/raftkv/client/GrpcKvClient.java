package raftkv.client;

import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.rpc.GrpcRpcClient;
import raftkv.rpc.RaftTransport;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Single-flight client with a total logical-request deadline and identity-preserving retries. */
public final class GrpcKvClient implements AutoCloseable {
    private static final Duration DEFAULT_REQUEST_BUDGET=Duration.ofSeconds(10);
    private static final Duration DEFAULT_RPC_CAP=Duration.ofSeconds(6);
    private final List<String> nodeUrls;
    private final RaftTransport rpcClient;
    private final long requestBudgetNanos;
    private final long rpcCapNanos;
    private final Random random;
    private String preferredUrl;
    private final String clientId=UUID.randomUUID().toString();
    private long sequence;
    private boolean closed;
    private long logicalRequests,successfulRequests,failedRequests,rpcAttempts,retryRpcAttempts,backoffNanos;

    public GrpcKvClient(List<String> nodeUrls) {
        this(nodeUrls,new GrpcRpcClient(),DEFAULT_REQUEST_BUDGET,DEFAULT_RPC_CAP,System.nanoTime());
    }

    /** Injectable transport and finite budgets support repeatable tests without network sockets. */
    public GrpcKvClient(List<String> nodeUrls,RaftTransport transport,Duration requestBudget,
                        Duration rpcCap,long seed) {
        Objects.requireNonNull(nodeUrls,"nodeUrls");
        if(nodeUrls.isEmpty())throw new IllegalArgumentException("At least one node URL is required");
        LinkedHashSet<String> validated=new LinkedHashSet<>();
        for(String url:nodeUrls) {
            URI uri;
            try {uri=URI.create(Objects.requireNonNull(url,"node URL"));}
            catch(RuntimeException invalid){throw new IllegalArgumentException("Expected grpc://host:port: "+url,invalid);}
            if(!"grpc".equals(uri.getScheme())||uri.getHost()==null||uri.getPort()<1||uri.getPort()>65535
                    ||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null
                    ||(uri.getPath()!=null&&!uri.getPath().isEmpty()))
                throw new IllegalArgumentException("Expected grpc://host:port: "+url);
            validated.add(url);
        }
        this.nodeUrls=List.copyOf(validated);
        this.rpcClient=Objects.requireNonNull(transport,"transport");
        this.requestBudgetNanos=positiveNanos(requestBudget,"request budget");
        this.rpcCapNanos=positiveNanos(rpcCap,"RPC cap");
        this.preferredUrl=this.nodeUrls.get(0);
        this.random=new Random(seed);
    }

    public synchronized KvRpc.ClientCommandResponse put(String key,String value) {
        return execute(Command.client(Command.Type.PUT,key,value,clientId,++sequence));
    }
    public synchronized KvRpc.ClientCommandResponse get(String key) {
        return execute(Command.client(Command.Type.GET,key,null,clientId,++sequence));
    }
    public synchronized KvRpc.ClientCommandResponse delete(String key) {
        return execute(Command.client(Command.Type.DELETE,key,null,clientId,++sequence));
    }

    /** The same Command object, ID, sequence, and payload are reused for every retry. */
    public synchronized KvRpc.ClientCommandResponse execute(Command command) {
        Objects.requireNonNull(command,"command");
        logicalRequests++;
        final KvRpc.ClientCommandResponse response;
        try {response=executeWithinBudget(command);}
        catch(RuntimeException failure){failedRequests++;throw failure;}
        if(response.success())successfulRequests++;else failedRequests++;
        return response;
    }

    private KvRpc.ClientCommandResponse executeWithinBudget(Command command) {
        long started=System.nanoTime();
        if(closed)return failure("Client is closed; command not attempted");
        String lastError="no RPC attempted";
        long attemptsThisRequest=0;
        int round=0;
        while(remaining(started)>0) {
            if(Thread.currentThread().isInterrupted())return failure("Interrupted; command outcome unknown");
            ArrayDeque<String> candidates=new ArrayDeque<>();
            candidates.add(preferredUrl);
            for(String url:nodeUrls)if(!url.equals(preferredUrl))candidates.add(url);
            Set<String> attemptedThisRound=new LinkedHashSet<>();
            while(!candidates.isEmpty()) {
                long budget=remaining(started);
                if(budget<=0)break;
                if(Thread.currentThread().isInterrupted())return failure("Interrupted; command outcome unknown");
                String url=candidates.removeFirst();
                if(!attemptedThisRound.add(url))continue;
                Duration timeout=Duration.ofNanos(Math.min(budget,rpcCapNanos));
                rpcAttempts++;
                if(attemptsThisRequest++>0)retryRpcAttempts++;
                try {
                    KvRpc.ClientCommandResponse response=rpcClient.postObject(url,"/client",
                            new KvRpc.ClientCommandRequest(command),KvRpc.ClientCommandResponse.class,timeout);
                    if(response.success()) {
                        if(remaining(started)<=0)return failure("Logical request deadline exceeded after response; outcome unknown");
                        preferredUrl=url;
                        return response;
                    }
                    lastError=response.message()==null?"RPC returned a failed response":response.message();
                    if(terminalMetadataError(lastError))return response;
                    // Membership is fixed; do not follow an arbitrary endpoint supplied in a reply.
                    String hint=response.leaderUrl();
                    if(hint!=null&&nodeUrls.contains(hint)) {
                        preferredUrl=hint;
                        if(!attemptedThisRound.contains(hint)) {
                            candidates.remove(hint);candidates.addFirst(hint);
                        }
                    }
                } catch(IOException transientFailure) {
                    lastError=transientFailure.getMessage()==null?transientFailure.getClass().getSimpleName():transientFailure.getMessage();
                } catch(InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return failure("Interrupted; command outcome unknown");
                }
            }
            long budget=remaining(started);
            if(budget<=0)break;
            long baseMillis=Math.min(200,20L<<Math.min(round++,4));
            long lower=Math.max(20,baseMillis*3/4),upper=Math.min(200,baseMillis*3/2);
            long jitteredMillis=lower+random.nextInt((int)(upper-lower+1));
            long pause=Math.min(budget,TimeUnit.MILLISECONDS.toNanos(jitteredMillis));
            long beforePause=System.nanoTime();
            try {TimeUnit.NANOSECONDS.sleep(pause);}
            catch(InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return failure("Interrupted during retry backoff; command outcome unknown");
            } finally {backoffNanos+=System.nanoTime()-beforePause;}
        }
        return failure("Logical request deadline exceeded; command outcome unknown; last error: "+lastError);
    }

    private long remaining(long started) {return requestBudgetNanos-(System.nanoTime()-started);}
    private KvRpc.ClientCommandResponse failure(String message) {
        return KvRpc.ClientCommandResponse.error(message,null,preferredUrl);
    }
    private static boolean terminalMetadataError(String message) {
        String text=message.toLowerCase(Locale.ROOT);
        return text.contains("stale client sequence")
                ||text.contains("client sequence reused for a different operation")
                ||text.contains("client sequence payload conflict");
    }
    private static long positiveNanos(Duration duration,String name) {
        Objects.requireNonNull(duration,name);
        try {
            long nanos=duration.toNanos();
            if(nanos<=0)throw new IllegalArgumentException(name+" must be positive");
            return nanos;
        } catch(ArithmeticException overflow){throw new IllegalArgumentException(name+" is too large",overflow);}
    }

    /** Counts application /client RPC attempts, excluding diagnostic /status calls. */
    public synchronized Stats stats() {
        return new Stats(logicalRequests,successfulRequests,failedRequests,rpcAttempts,retryRpcAttempts,backoffNanos);
    }
    public record Stats(long logicalRequests,long successfulRequests,long failedRequests,
                        long rpcAttempts,long retryRpcAttempts,long backoffNanos) { }

    public synchronized List<String> statuses() {
        List<String> result=new ArrayList<>();
        for(String url:nodeUrls) {
            try {result.add(rpcClient.postObject(url,"/status","",String.class,Duration.ofNanos(Math.min(rpcCapNanos,TimeUnit.SECONDS.toNanos(2)))));}
            catch(IOException failure){result.add("{\"url\":\""+url+"\",\"error\":\"unavailable\"}");}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;}
        }
        return result;
    }
    @Override public synchronized void close() {if(!closed){closed=true;rpcClient.close();}}
}
