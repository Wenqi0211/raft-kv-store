package raftkv.raft;

import raftkv.client.KvClient;
import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.rpc.HttpRpcClient;
import raftkv.server.RaftHttpServer;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static raftkv.test.TestSupport.*;

/** Real HTTP between nodes in one JVM; this does not simulate process kills or power loss. */
public final class ClusterIntegrationTest {
    public static void main(String[] args) throws Exception {
        run("single-node CRUD and restart preserve values and deletion", ClusterIntegrationTest::singleNodeRestart);
        run("lagging follower installs snapshot after compaction and a new election", ClusterIntegrationTest::snapshotCatchup);
        run("minority cannot acknowledge or apply a new write", ClusterIntegrationTest::quorumLoss);
    }

    private static void singleNodeRestart() throws Exception {
        try (Cluster cluster = new Cluster(1, 4)) {
            cluster.start(0);
            cluster.awaitLeader();
            try (KvClient client = cluster.client()) {
                expect(client.put("kept", "durable"), "durable");
                expect(client.put("deleted", "old"), "old");
                expect(client.get("kept"), "durable");
                expect(client.delete("deleted"), "old");
                expect(client.get("deleted"), null);
            }
            cluster.stop(0);
            cluster.start(0);
            cluster.awaitLeader();
            try (KvClient client = cluster.client()) {
                expect(client.get("kept"), "durable");
                expect(client.get("deleted"), null);
                expect(client.put("after-restart", "works"), "works");
            }
            cluster.awaitState(Map.of("kept", "durable", "after-restart", "works"), 0);
        }
    }

    private static void snapshotCatchup() throws Exception {
        try (Cluster cluster = new Cluster(3, 10)) {
            cluster.startAll();
            int leader = cluster.awaitLeader();
            try (KvClient client = cluster.client()) {
                expect(client.put("kept", "before-failure"), "before-failure");
                expect(client.put("deleted", "must-not-return"), "must-not-return");
                expect(client.delete("deleted"), "must-not-return");
            }
            int lagging = (leader + 1) % 3;
            cluster.stop(lagging);
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("kept", "before-failure");
            try (KvClient client = cluster.client()) {
                for (int i = 0; i < 80; i++) {
                    String key = "key-" + i;
                    String value = "value-" + i;
                    expect(client.put(key, value), value);
                    expected.put(key, value);
                }
            }
            await(() -> cluster.activeSnapshotFloor() >= 40, Duration.ofSeconds(10),
                    "both active nodes must compact while the follower is offline");

            // Re-election resets nextIndex to the log tip. The old implementation
            // then clamped retries above the snapshot and could never catch up.
            cluster.stop(leader);
            cluster.start(leader);
            int newLeader = cluster.awaitLeader();
            check(cluster.nodes[newLeader].snapshotIndex() > cluster.nodes[lagging].lastAppliedIndex(),
                    "the recovering follower must be behind the new leader's compacted prefix");
            long target = cluster.nodes[newLeader].lastAppliedIndex();
            cluster.start(lagging);
            cluster.awaitState(expected, target);
            check(cluster.nodes[lagging].snapshotIndex() > 0, "catch-up must install a snapshot, not only append logs");
            try (KvClient client = cluster.client()) {
                expect(client.get("key-79"), "value-79");
                expect(client.get("deleted"), null);
            }
        }
    }

    private static void quorumLoss() throws Exception {
        try (Cluster cluster = new Cluster(3, 50)) {
            cluster.startAll();
            int leader = cluster.awaitLeader();
            try (KvClient client = cluster.client()) {
                expect(client.put("confirmed", "before-quorum-loss"), "before-quorum-loss");
            }
            for (int i = 0; i < 3; i++) {
                if (i != leader) {
                    cluster.stop(i);
                }
            }
            try (HttpRpcClient rpc = new HttpRpcClient()) {
                KvRpc.ClientCommandResponse response = rpc.postObject(cluster.nodes[leader].url(), "/client",
                        new KvRpc.ClientCommandRequest(Command.put("uncertain", "no-majority")),
                        KvRpc.ClientCommandResponse.class, Duration.ofSeconds(7));
                check(!response.success(), "one of three nodes must not acknowledge a new write");
                check(!cluster.nodes[leader].localState().containsKey("uncertain"),
                        "the minority must not apply its uncommitted entry");
            }
            for (int i = 0; i < 3; i++) {
                if (i != leader) {
                    cluster.start(i);
                }
            }
            cluster.awaitLeader();
            try (KvClient client = cluster.client()) {
                expect(client.put("after-recovery", "available"), "available");
                expect(client.get("confirmed"), "before-quorum-loss");
            }
            // A timed-out command is allowed to commit later. Do not assert its
            // absence after recovery; exactly-once execution is not implemented.
            int recoveredLeader = cluster.awaitLeader();
            cluster.awaitState(cluster.nodes[recoveredLeader].localState(),
                    cluster.nodes[recoveredLeader].lastAppliedIndex());
        }
    }

    private static void expect(KvRpc.ClientCommandResponse response, String value) {
        check(response.success(), "operation failed: " + response.message());
        equal(value, response.value(), "operation must return the correct value");
    }

    private static final class Cluster implements AutoCloseable {
        private final Path directory = Files.createTempDirectory("raftkv-cluster-test-");
        private final Map<String, Peer> peers = new LinkedHashMap<>();
        private final List<Peer> peerList;
        private final int threshold;
        private final RaftNode[] nodes;
        private final RaftHttpServer[] servers;
        private final boolean[] active;

        Cluster(int size, int threshold) throws Exception {
            this.threshold = threshold;
            nodes = new RaftNode[size];
            servers = new RaftHttpServer[size];
            active = new boolean[size];
            List<ServerSocket> reservations = new ArrayList<>();
            try {
                for (int i = 0; i < size; i++) {
                    ServerSocket socket = new ServerSocket(0);
                    reservations.add(socket);
                    String id = "n" + (i + 1);
                    peers.put(id, new Peer(id, "http://127.0.0.1:" + socket.getLocalPort()));
                }
            } finally {
                for (ServerSocket socket : reservations) {
                    socket.close();
                }
            }
            peerList = List.copyOf(peers.values());
        }

        void startAll() throws Exception {
            for (int i = 0; i < nodes.length; i++) {
                start(i);
            }
        }

        void start(int index) throws Exception {
            Peer peer = peerList.get(index);
            nodes[index] = new RaftNode(peer.id(), peer.url(), peers, directory.resolve(peer.id()), threshold);
            servers[index] = new RaftHttpServer(nodes[index]);
            servers[index].start();
            nodes[index].start();
            active[index] = true;
        }

        void stop(int index) {
            if (active[index]) {
                servers[index].stop();
                nodes[index].stop();
                active[index] = false;
            }
        }

        KvClient client() {
            List<String> urls = new ArrayList<>();
            for (int i = 0; i < nodes.length; i++) {
                if (active[i]) {
                    urls.add(nodes[i].url());
                }
            }
            return new KvClient(urls);
        }

        int awaitLeader() throws Exception {
            await(() -> leaderCount() == 1, Duration.ofSeconds(12), "one active leader must be elected");
            for (int i = 0; i < nodes.length; i++) {
                if (active[i] && nodes[i].role() == Role.LEADER) {
                    return i;
                }
            }
            throw new AssertionError("leader changed while checking");
        }

        int leaderCount() {
            int leaders = 0;
            for (int i = 0; i < nodes.length; i++) {
                if (active[i] && nodes[i].role() == Role.LEADER) {
                    leaders++;
                }
            }
            return leaders;
        }

        long activeSnapshotFloor() {
            long minimum = Long.MAX_VALUE;
            for (int i = 0; i < nodes.length; i++) {
                if (active[i]) {
                    minimum = Math.min(minimum, nodes[i].snapshotIndex());
                }
            }
            return minimum;
        }

        void awaitState(Map<String, String> expected, long minimumApplied) throws Exception {
            try {
                await(() -> {
                    for (int i = 0; i < nodes.length; i++) {
                        if (active[i] && (nodes[i].lastAppliedIndex() < minimumApplied
                                || !Objects.equals(expected, nodes[i].localState()))) {
                            return false;
                        }
                    }
                    return true;
                }, Duration.ofSeconds(15), "all active nodes must apply the expected data");
            } catch (AssertionError failure) {
                String statuses = "";
                for (int i = 0; i < nodes.length; i++) {
                    statuses += "\n" + nodes[i].statusJson();
                }
                throw new AssertionError(failure.getMessage() + statuses, failure);
            }
        }

        @Override
        public void close() throws Exception {
            for (int i = 0; i < nodes.length; i++) {
                if (servers[i] != null) {
                    servers[i].stop();
                }
            }
            for (RaftNode node : nodes) {
                if (node != null) {
                    node.stop();
                }
            }
            deleteTree(directory);
        }
    }
}
