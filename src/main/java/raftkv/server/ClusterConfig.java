package raftkv.server;

import raftkv.raft.Peer;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ClusterConfig {
    private ClusterConfig() {
    }

    public static Map<String, Peer> parse(String cluster) {
        LinkedHashMap<String, Peer> peers = new LinkedHashMap<>();
        for (String part : cluster.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] pieces = trimmed.split("=", 2);
            if (pieces.length != 2) {
                throw new IllegalArgumentException("Cluster entries must look like id=url: " + trimmed);
            }
            peers.put(pieces[0], new Peer(pieces[0], pieces[1]));
        }
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("Cluster must contain at least one peer");
        }
        return peers;
    }

    public static String defaultCluster() {
        return "n1=http://127.0.0.1:8001,n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003";
    }
}

