package raftkv.rpc;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/** Unary RPC boundary shared by the HTTP, gRPC, and fault-injection transports. */
public interface RaftTransport extends AutoCloseable {
    <T> T postObject(String baseUrl, String path, Serializable request,
                     Class<T> responseType, Duration timeout) throws IOException, InterruptedException;

    @Override
    void close();
}
