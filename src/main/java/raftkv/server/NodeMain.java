package raftkv.server;

import raftkv.raft.Peer;
import raftkv.raft.RaftNode;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public final class NodeMain {
    private NodeMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        String id = options.getOrDefault("--id", "n1");
        String clusterText = options.getOrDefault("--cluster", ClusterConfig.defaultCluster());
        Map<String, Peer> peers = ClusterConfig.parse(clusterText);
        String url = options.getOrDefault("--url", peers.containsKey(id) ? peers.get(id).url() : "http://127.0.0.1:8001");
        peers = new LinkedHashMap<>(peers);
        peers.put(id, new Peer(id, url));
        Path dataDir = Path.of(options.getOrDefault("--data-dir", "work/data/" + id));
        int snapshotThreshold = Integer.parseInt(options.getOrDefault("--snapshot-threshold", "50"));

        RaftNode node = new RaftNode(id, url, peers, dataDir, snapshotThreshold);
        RaftHttpServer server = new RaftHttpServer(node);
        server.start();
        node.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            node.stop();
        }));

        System.out.println("RaftKV node " + id + " listening at " + url);
        System.out.println("Data dir: " + dataDir.toAbsolutePath());
        new CountDownLatch(1).await();
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + key);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + key);
            }
            options.put(key, args[++i]);
        }
        return options;
    }
}

