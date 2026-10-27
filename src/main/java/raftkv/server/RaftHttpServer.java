package raftkv.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import raftkv.kv.KvRpc;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftRpc;
import raftkv.rpc.BinaryCodec;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public final class RaftHttpServer {
    private final RaftNode node;
    private final String url;
    private HttpServer server;
    private ExecutorService executor;

    public RaftHttpServer(RaftNode node) {
        this.node = node;
        this.url = node.url();
    }

    public void start() throws IOException {
        URI uri = URI.create(url);
        String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
        int port = uri.getPort();
        if (port <= 0) {
            throw new IllegalArgumentException("Node URL must include a port: " + url);
        }
        server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/rpc/requestVote", exchange -> handleObject(exchange,
                RaftRpc.RequestVoteRequest.class, node::handleRequestVote));
        server.createContext("/rpc/appendEntries", exchange -> handleObject(exchange,
                RaftRpc.AppendEntriesRequest.class, node::handleAppendEntries));
        server.createContext("/rpc/installSnapshot", exchange -> handleObject(exchange,
                RaftRpc.InstallSnapshotRequest.class, node::handleInstallSnapshot));
        server.createContext("/client", exchange -> handleObject(exchange,
                KvRpc.ClientCommandRequest.class, request -> node.submit(request.command())));
        server.createContext("/status", this::handleStatus);
        server.createContext("/health", this::handleHealth);
        executor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "http-" + node.id());
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private <Q, R extends Serializable> void handleObject(
            HttpExchange exchange,
            Class<Q> requestType,
            Function<Q, R> handler
    ) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendText(exchange, 405, "method not allowed");
            return;
        }
        try {
            Object decoded = BinaryCodec.decode(exchange.getRequestBody().readAllBytes());
            Q request = requestType.cast(decoded);
            byte[] body = BinaryCodec.encode(handler.apply(request));
            exchange.getResponseHeaders().set("Content-Type", "application/x-java-serialized-object");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        } catch (Exception e) {
            byte[] body = ("server error: " + e.getMessage()).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendText(exchange, 405, "method not allowed");
            return;
        }
        sendText(exchange, 200, node.statusJson());
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        sendText(exchange, 200, "ok");
    }

    private static void sendText(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }
}
