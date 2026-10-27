package raftkv.rpc;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class HttpRpcClient implements RaftTransport {
    private final HttpClient client;

    public HttpRpcClient() {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(300))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    public <T> T postObject(String baseUrl, String path, Serializable request, Class<T> responseType, Duration timeout)
            throws IOException, InterruptedException {
        String url = trimRight(baseUrl) + path;
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header("Content-Type", "application/x-java-serialized-object")
                .POST(HttpRequest.BodyPublishers.ofByteArray(BinaryCodec.encode(request)))
                .build();
        HttpResponse<byte[]> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) {
            throw new IOException("RPC failed: " + response.statusCode() + " from " + url);
        }
        Object decoded = BinaryCodec.decode(response.body());
        return responseType.cast(decoded);
    }

    public String getText(String baseUrl, String path, Duration timeout) throws IOException, InterruptedException {
        String url = trimRight(baseUrl) + path;
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .GET()
                .build();
        HttpResponse<String> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("GET failed: " + response.statusCode() + " from " + url);
        }
        return response.body();
    }

    private static String trimRight(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    @Override
    public void close() {
    }
}

