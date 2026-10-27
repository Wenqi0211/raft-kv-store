package raftkv.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import raftkv.kv.KvRpc;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftRpc;
import raftkv.rpc.BinaryCodec;
import raftkv.rpc.GrpcRpcClient;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Loopback gRPC adapter; no TLS, public access, or untrusted payloads are supported. */
public final class RaftGrpcServer implements AutoCloseable {
    private final RaftNode node;
    private final ExecutorService executor;
    private Server server;

    public RaftGrpcServer(RaftNode node) {
        this.node = Objects.requireNonNull(node);
        this.executor = Executors.newFixedThreadPool(16, runnable -> {
            Thread thread = new Thread(runnable, "grpc-" + node.id());
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("gRPC server already started");
        }
        URI uri = URI.create(node.url());
        if (!"grpc".equals(uri.getScheme()) || uri.getHost() == null || uri.getPort() <= 0) {
            throw new IllegalArgumentException("Expected grpc://host:port, got " + node.url());
        }
        ServerServiceDefinition.Builder service = ServerServiceDefinition.builder(GrpcRpcClient.SERVICE);
        add(service, "/rpc/requestVote", RaftRpc.RequestVoteRequest.class, node::handleRequestVote);
        add(service, "/rpc/appendEntries", RaftRpc.AppendEntriesRequest.class, node::handleAppendEntries);
        add(service, "/rpc/installSnapshot", RaftRpc.InstallSnapshotRequest.class, node::handleInstallSnapshot);
        add(service, "/client", KvRpc.ClientCommandRequest.class, request -> node.submit(request.command()));
        add(service, "/status", String.class, ignored -> node.statusJson());
        server = NettyServerBuilder.forAddress(new InetSocketAddress(uri.getHost(), uri.getPort()))
                .executor(executor)
                .maxInboundMessageSize(GrpcRpcClient.MAX_MESSAGE_BYTES)
                .addService(service.build())
                .build()
                .start();
    }

    private static <Q extends Serializable, R extends Serializable> void add(
            ServerServiceDefinition.Builder service, String path, Class<Q> requestType, Function<Q, R> handler) {
        service.addMethod(GrpcRpcClient.methodForPath(path), ServerCalls.asyncUnaryCall(
                (BytesValue envelope, StreamObserver<BytesValue> observer) -> {
                    try {
                        Object decoded = BinaryCodec.decode(envelope.getValue().toByteArray());
                        if (!requestType.isInstance(decoded)) {
                            observer.onError(Status.INVALID_ARGUMENT.withDescription("Wrong request type for " + path)
                                    .asRuntimeException());
                            return;
                        }
                        R result = handler.apply(requestType.cast(decoded));
                        observer.onNext(BytesValue.newBuilder()
                                .setValue(ByteString.copyFrom(BinaryCodec.encode(result))).build());
                        observer.onCompleted();
                    } catch (IOException | ClassCastException malformed) {
                        observer.onError(Status.INVALID_ARGUMENT.withDescription("Malformed request for " + path)
                                .withCause(malformed).asRuntimeException());
                    } catch (RuntimeException failure) {
                        observer.onError(Status.INTERNAL.withDescription("Node rejected " + path)
                                .withCause(failure).asRuntimeException());
                    }
                }));
    }

    public void stop() {
        if (server != null) {
            server.shutdownNow();
            try {
                server.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        executor.shutdownNow();
    }

    @Override
    public void close() {
        stop();
    }
}
