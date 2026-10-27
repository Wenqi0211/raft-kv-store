package raftkv.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Real unary gRPC transport for trusted, Java-only local clusters. */
public final class GrpcRpcClient implements RaftTransport {
    public static final String SERVICE = "raftkv.RaftService";
    public static final int MAX_MESSAGE_BYTES = 16 * 1024 * 1024;
    private static final Map<String, MethodDescriptor<BytesValue, BytesValue>> METHODS = Map.of(
            "/rpc/requestVote", method("RequestVote"),
            "/rpc/appendEntries", method("AppendEntries"),
            "/rpc/installSnapshot", method("InstallSnapshot"),
            "/client", method("Client"),
            "/status", method("Status"));

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private static MethodDescriptor<BytesValue, BytesValue> method(String name) {
        return MethodDescriptor.<BytesValue, BytesValue>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, name))
                .setRequestMarshaller(ProtoUtils.marshaller(BytesValue.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BytesValue.getDefaultInstance()))
                .build();
    }

    public static MethodDescriptor<BytesValue, BytesValue> methodForPath(String path) {
        MethodDescriptor<BytesValue, BytesValue> descriptor = METHODS.get(path);
        if (descriptor == null) {
            throw new IllegalArgumentException("Unsupported gRPC path: " + path);
        }
        return descriptor;
    }

    @Override
    public <T> T postObject(String baseUrl, String path, Serializable request,
                             Class<T> responseType, Duration timeout) throws IOException, InterruptedException {
        if (closed) {
            throw new IOException("gRPC transport is closed");
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interrupted before gRPC call");
        }
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("RPC timeout must be positive");
        }
        ManagedChannel channel = channels.computeIfAbsent(baseUrl, GrpcRpcClient::openChannel);
        BytesValue envelope = BytesValue.newBuilder()
                .setValue(ByteString.copyFrom(BinaryCodec.encode(request)))
                .build();
        try {
            BytesValue response = ClientCalls.blockingUnaryCall(channel, methodForPath(path),
                    CallOptions.DEFAULT.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS), envelope);
            Object decoded = BinaryCodec.decode(response.getValue().toByteArray());
            if (!responseType.isInstance(decoded)) {
                throw new IOException("Unexpected response type for " + path + ": "
                        + (decoded == null ? "null" : decoded.getClass().getName()));
            }
            return responseType.cast(decoded);
        } catch (StatusRuntimeException failure) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted during gRPC call");
            }
            throw new IOException("gRPC " + path + " at " + baseUrl + " failed: "
                    + failure.getStatus(), failure);
        }
    }

    private static ManagedChannel openChannel(String baseUrl) {
        URI uri = URI.create(baseUrl);
        if (!"grpc".equals(uri.getScheme()) || uri.getHost() == null || uri.getPort() <= 0) {
            throw new IllegalArgumentException("Expected grpc://host:port, got " + baseUrl);
        }
        return NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .build();
    }

    @Override
    public void close() {
        closed = true;
        channels.values().forEach(ManagedChannel::shutdownNow);
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        channels.clear();
    }
}
