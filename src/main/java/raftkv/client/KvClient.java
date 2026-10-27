package raftkv.client;

import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.rpc.HttpRpcClient;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class KvClient implements AutoCloseable {
    private final List<String> nodeUrls;
    private final HttpRpcClient rpcClient = new HttpRpcClient();
    private volatile String preferredUrl;

    public KvClient(List<String> nodeUrls) {
        if (nodeUrls.isEmpty()) {
            throw new IllegalArgumentException("At least one node URL is required");
        }
        this.nodeUrls = List.copyOf(nodeUrls);
        this.preferredUrl = nodeUrls.get(0);
    }

    public KvRpc.ClientCommandResponse put(String key, String value) {
        return execute(Command.put(key, value));
    }

    public KvRpc.ClientCommandResponse get(String key) {
        return execute(Command.get(key));
    }

    public KvRpc.ClientCommandResponse delete(String key) {
        return execute(Command.delete(key));
    }

    public KvRpc.ClientCommandResponse execute(Command command) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        if (preferredUrl != null) {
            candidates.add(preferredUrl);
        }
        candidates.addAll(nodeUrls);

        String lastError = "no nodes attempted";
        for (int round = 0; round < 2; round++) {
            List<String> snapshot = new ArrayList<>(candidates);
            for (String url : snapshot) {
                try {
                    KvRpc.ClientCommandRequest request = new KvRpc.ClientCommandRequest(command);
                    KvRpc.ClientCommandResponse response = rpcClient.postObject(url, "/client", request,
                            KvRpc.ClientCommandResponse.class, Duration.ofSeconds(6));
                    if (response.success()) {
                        preferredUrl = url;
                        return response;
                    }
                    lastError = response.message();
                    if (response.leaderUrl() != null && !response.leaderUrl().isBlank()) {
                        preferredUrl = response.leaderUrl();
                        candidates.add(response.leaderUrl());
                    }
                } catch (IOException | InterruptedException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    lastError = e.getMessage();
                }
            }
        }
        return KvRpc.ClientCommandResponse.error("request failed: " + lastError, null, preferredUrl);
    }

    public List<String> statuses() {
        List<String> statuses = new ArrayList<>();
        for (String url : nodeUrls) {
            try {
                statuses.add(rpcClient.getText(url, "/status", Duration.ofSeconds(2)));
            } catch (Exception e) {
                statuses.add("{\"url\":\"" + url + "\",\"error\":\"" + e.getMessage() + "\"}");
            }
        }
        return statuses;
    }

    @Override
    public void close() {
        rpcClient.close();
    }
}

