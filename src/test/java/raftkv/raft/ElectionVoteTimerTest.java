package raftkv.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raftkv.kv.Command;
import raftkv.rpc.RaftTransport;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ElectionVoteTimerTest {
    @TempDir Path directory;

    @Test void repeatedStaleHigherTermCandidatesCannotPostponeAnEligibleElection() throws Exception {
        RaftNode node = node();
        try {
            set(node, "running", true);
            set(node, "electionDeadlineMillis", 0L); // Expired without sleeping or starting timers.
            for (long term = 5; term <= 9; term++) {
                assertFalse(node.handleRequestVote(new RaftRpc.RequestVoteRequest(term, "stale", 0, 0)).voteGranted());
            }
            var election = RaftNode.class.getDeclaredMethod("startElection");
            election.setAccessible(true);
            election.invoke(node);
            assertEquals(Role.CANDIDATE, node.role(), "rejected stale votes must not suppress an expired election");
            assertEquals(10, node.currentTerm());
            assertEquals("n1", new PersistentStore(directory).load().votedFor);
        } finally { node.stop(); }
    }

    @Test void rejectingAHigherTermVoteStillStepsDownClearsVoteAndPersistsTerm() throws Exception {
        RaftNode node = node();
        try {
            set(node, "role", Role.LEADER);
            set(node, "electionDeadlineMillis", 42L);
            var response = node.handleRequestVote(new RaftRpc.RequestVoteRequest(7, "stale", 0, 0));
            assertFalse(response.voteGranted());
            assertEquals(Role.FOLLOWER, node.role());
            var saved = new PersistentStore(directory).load();
            assertEquals(7, saved.currentTerm);
            assertNull(saved.votedFor);
            assertEquals(42L, field(node, "electionDeadlineMillis"));
        } finally { node.stop(); }
    }

    @Test void grantedVotesAndCurrentLeaderHeartbeatsStillResetTheDeadline() throws Exception {
        RaftNode node = node();
        try {
            set(node, "electionDeadlineMillis", 0L);
            assertTrue(node.handleRequestVote(new RaftRpc.RequestVoteRequest(5, "fresh", 1, 4)).voteGranted());
            assertTrue((long) field(node, "electionDeadlineMillis") > 0);
            assertEquals("fresh", new PersistentStore(directory).load().votedFor);
            set(node, "electionDeadlineMillis", 0L);
            assertTrue(node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(5, "fresh", 1, 4, List.of(), 1)).success());
            assertTrue((long) field(node, "electionDeadlineMillis") > 0);
        } finally { node.stop(); }
    }

    private RaftNode node() throws Exception {
        new PersistentStore(directory).save(new PersistentState(4, "previous", 0, 0, 1, 1,
                List.of(new LogEntry(1, 4, Command.put("stable", "value"))), Snapshot.empty()));
        Map<String,Peer> peers = Map.of("n1", new Peer("n1", "sim://n1"),
                "n2", new Peer("n2", "sim://n2"), "n3", new Peer("n3", "sim://n3"));
        RaftTransport unavailable = new RaftTransport() {
            public <T> T postObject(String url, String path, Serializable request, Class<T> type,
                                    Duration timeout) throws IOException { throw new IOException("No remote votes in this deterministic test"); }
            public void close() { }
        };
        return new RaftNode("n1", "sim://n1", peers, directory, 10, unavailable, 42);
    }
    private static Object field(RaftNode node, String name) throws Exception {
        Field field = RaftNode.class.getDeclaredField(name); field.setAccessible(true); return field.get(node);
    }
    private static void set(RaftNode node, String name, Object value) throws Exception {
        Field field = RaftNode.class.getDeclaredField(name); field.setAccessible(true); field.set(node, value);
    }
}
