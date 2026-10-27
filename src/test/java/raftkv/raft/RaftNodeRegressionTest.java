package raftkv.raft;

import raftkv.kv.Command;
import raftkv.kv.KvRpc;
import raftkv.test.TestSupport;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static raftkv.test.TestSupport.check;
import static raftkv.test.TestSupport.equal;

/** Deterministic protocol regressions; no election timers or external servers are needed. */
public final class RaftNodeRegressionTest {
    private RaftNodeRegressionTest() {
    }

    public static void main(String[] args) throws Exception {
        TestSupport.run("a rejected vote still persists the higher term", RaftNodeRegressionTest::rejectedVotePersistsTerm);
        TestSupport.run("an empty heartbeat persists the higher term", RaftNodeRegressionTest::heartbeatPersistsTerm);
        TestSupport.run("a rejected append persists the higher term", RaftNodeRegressionTest::rejectedAppendPersistsTerm);
        TestSupport.run("same-term step-down does not permit a second vote", RaftNodeRegressionTest::sameTermStepDownRetainsVote);
        TestSupport.run("an append commits only the prefix matched by that request", RaftNodeRegressionTest::appendDoesNotCommitUnmatchedSuffix);
        TestSupport.run("a previous-log mismatch leaves the existing log intact", RaftNodeRegressionTest::mismatchedPreviousEntryDoesNotDeleteLog);
        TestSupport.run("a matching snapshot boundary preserves the later log", RaftNodeRegressionTest::matchingSnapshotRetainsSuffix);
        TestSupport.run("a conflicting snapshot boundary discards the later log", RaftNodeRegressionTest::conflictingSnapshotDiscardsSuffix);
        TestSupport.run("a snapshot cannot conflict with a committed boundary awaiting application", RaftNodeRegressionTest::snapshotCannotReplaceCommittedBoundary);
        TestSupport.run("a stale snapshot cannot roll back applied state", RaftNodeRegressionTest::staleSnapshotDoesNotRollBackState);
        TestSupport.run("an overwritten request cannot receive its replacement's result", RaftNodeRegressionTest::overwrittenRequestCannotSucceed);
        TestSupport.run("a failed durable update prevents future acknowledgments", RaftNodeRegressionTest::failedPersistenceStopsProtocolResponses);
    }

    private static void rejectedVotePersistsTerm() throws Exception {
        PersistentState initial = state(4, "previous-vote", 0, 0, List.of(entry(1, 4, "key", "value")));
        try (Fixture fixture = new Fixture(initial)) {
            RaftRpc.RequestVoteResponse response = fixture.node.handleRequestVote(
                    new RaftRpc.RequestVoteRequest(5, "candidate", 0, 0));
            check(!response.voteGranted(), "a candidate with an older log must not receive a vote");
            equal(5L, response.term(), "the response must report the new term");
            assertDurableTerm(fixture, 5);
        }
    }

    private static void heartbeatPersistsTerm() throws Exception {
        try (Fixture fixture = new Fixture(state(4, "previous-vote", 0, 0, List.of()))) {
            RaftRpc.AppendEntriesResponse response = fixture.node.handleAppendEntries(
                    new RaftRpc.AppendEntriesRequest(5, "leader", 0, 0, List.of(), 0));
            check(response.success(), "the heartbeat must match the empty log");
            assertDurableTerm(fixture, 5);
        }
    }

    private static void rejectedAppendPersistsTerm() throws Exception {
        try (Fixture fixture = new Fixture(state(4, "previous-vote", 0, 0, List.of()))) {
            RaftRpc.AppendEntriesResponse response = fixture.node.handleAppendEntries(
                    new RaftRpc.AppendEntriesRequest(5, "leader", 8, 4, List.of(), 0));
            check(!response.success(), "an absent previous entry must reject the append");
            assertDurableTerm(fixture, 5);
        }
    }

    private static void sameTermStepDownRetainsVote() throws Exception {
        try (Fixture fixture = new Fixture(state(4, "n1", 0, 0, List.of()))) {
            Field role = RaftNode.class.getDeclaredField("role");
            role.setAccessible(true);
            role.set(fixture.node, Role.CANDIDATE);
            fixture.node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(4, "n2", 0, 0, List.of(), 0));
            equal(Role.FOLLOWER, fixture.node.role(), "the candidate must step down on a current-term leader's heartbeat");

            RaftRpc.RequestVoteResponse secondCandidate = fixture.node.handleRequestVote(
                    new RaftRpc.RequestVoteRequest(4, "n3", 100, 100));
            check(!secondCandidate.voteGranted(), "stepping down must not erase the vote already cast in this term");
            equal("n1", fixture.saved().votedFor, "the durable vote must still identify the original candidate");
        }
    }

    private static void appendDoesNotCommitUnmatchedSuffix() throws Exception {
        List<LogEntry> entries = List.of(
                entry(1, 1, "prefix", "first"),
                entry(2, 1, "prefix", "second"),
                entry(3, 2, "divergent", "must-not-apply"));
        try (Fixture fixture = new Fixture(state(3, null, 1, 1, entries))) {
            // This heartbeat proves only entries through index 2. Index 3 belongs to an old leader.
            RaftRpc.AppendEntriesResponse response = fixture.node.handleAppendEntries(
                    new RaftRpc.AppendEntriesRequest(3, "leader", 2, 1, List.of(), 3));
            check(response.success(), "the matching prefix must accept the heartbeat");
            equal(2L, fixture.saved().commitIndex, "the unmatched suffix must remain uncommitted");
            applyOnce(fixture.node);
            equal(Map.of("prefix", "second"), fixture.node.localState(), "only the confirmed prefix may reach the state machine");
        }
    }

    private static void mismatchedPreviousEntryDoesNotDeleteLog() throws Exception {
        List<LogEntry> entries = List.of(entry(1, 1, "a", "one"), entry(2, 2, "b", "two"), entry(3, 2, "c", "three"));
        try (Fixture fixture = new Fixture(state(3, null, 0, 0, entries))) {
            RaftRpc.AppendEntriesResponse response = fixture.node.handleAppendEntries(
                    new RaftRpc.AppendEntriesRequest(3, "leader", 2, 1, List.of(), 0));
            check(!response.success(), "the term mismatch must reject the append");
            equal(entries, fixture.saved().log, "a rejected probe must not erase entries; deletion needs a conflicting incoming entry");
            // A subsequent matching probe also confirms that the in-memory log was not deleted.
            check(fixture.node.handleAppendEntries(
                    new RaftRpc.AppendEntriesRequest(3, "leader", 3, 2, List.of(), 0)).success(),
                    "the rejected probe must leave index 3 available");
        }
    }

    private static void matchingSnapshotRetainsSuffix() throws Exception {
        LogEntry suffix = entry(3, 3, "later", "retained");
        try (Fixture fixture = new Fixture(state(4, null, 0, 0,
                List.of(entry(1, 1, "old", "one"), entry(2, 2, "old", "two"), suffix)))) {
            check(fixture.node.handleInstallSnapshot(new RaftRpc.InstallSnapshotRequest(
                    5, "leader", 2, 2, Map.of("at-boundary", "snapshot"))).success(), "snapshot installation must succeed");
            equal(List.of(suffix), fixture.saved().log, "a matching index and term must preserve the suffix");
            equal(Map.of("at-boundary", "snapshot"), fixture.node.localState(), "the installed snapshot supplies the applied state");
            equal(2L, fixture.node.lastAppliedIndex(), "installing a snapshot applies through its boundary");

            fixture.node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(5, "leader", 3, 3, List.of(), 3));
            applyOnce(fixture.node);
            equal(Map.of("at-boundary", "snapshot", "later", "retained"), fixture.node.localState(),
                    "the preserved suffix must still be applicable after the snapshot");
        }
    }

    private static void conflictingSnapshotDiscardsSuffix() throws Exception {
        try (Fixture fixture = new Fixture(state(4, null, 0, 0,
                List.of(entry(1, 1, "old", "one"), entry(2, 2, "old", "two"), entry(3, 3, "wrong-branch", "discard"))))) {
            fixture.node.handleInstallSnapshot(new RaftRpc.InstallSnapshotRequest(5, "leader", 2, 1, Map.of("key", "leader-state")));
            equal(List.of(), fixture.saved().log, "an index match without a term match must discard the entire old suffix");
            equal(Map.of("key", "leader-state"), fixture.node.localState(), "conflicting local state must be replaced by the snapshot");

            fixture.node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(
                    5, "leader", 2, 1, List.of(entry(3, 5, "correct-branch", "accepted")), 3));
            applyOnce(fixture.node);
            equal(Map.of("key", "leader-state", "correct-branch", "accepted"), fixture.node.localState(),
                    "future entries must attach to the installed boundary without resurrecting the conflicting branch");
        }
    }

    private static void staleSnapshotDoesNotRollBackState() throws Exception {
        try (Fixture fixture = new Fixture(state(3, null, 2, 2,
                List.of(entry(1, 1, "key", "older"), entry(2, 2, "key", "current"))))) {
            equal(Map.of("key", "current"), fixture.node.localState(), "the fixture must replay the applied entries");
            fixture.node.handleInstallSnapshot(new RaftRpc.InstallSnapshotRequest(4, "leader", 1, 1, Map.of("key", "older")));
            equal(Map.of("key", "current"), fixture.node.localState(), "an older snapshot must not replace newer applied data");
            equal(2L, fixture.node.lastAppliedIndex(), "lastApplied must not move backwards");
            check(fixture.saved().commitIndex >= 2, "the committed prefix must not move backwards");
            try (RaftNodeHandle restarted = fixture.restart()) {
                equal(Map.of("key", "current"), restarted.node.localState(), "restarting must preserve the newer applied state too");
            }
        }
    }

    private static void snapshotCannotReplaceCommittedBoundary() throws Exception {
        List<LogEntry> committed = List.of(entry(1, 1, "key", "older"), entry(2, 2, "key", "committed"));
        try (Fixture fixture = new Fixture(state(3, null, 2, 1, committed))) {
            equal(Map.of("key", "older"), fixture.node.localState(), "the second committed entry must still await application");
            RaftRpc.InstallSnapshotResponse response = fixture.node.handleInstallSnapshot(
                    new RaftRpc.InstallSnapshotRequest(4, "leader", 2, 1, Map.of("key", "conflicting")));
            check(!response.success(), "a snapshot term conflict at the committed index must be rejected");
            equal(committed, fixture.saved().log, "rejecting the snapshot must preserve the entire committed log");
            equal(2L, fixture.saved().commitIndex, "the committed boundary must remain intact");
            equal(Map.of("key", "older"), fixture.node.localState(), "the rejected snapshot must not replace applied state");
            applyOnce(fixture.node);
            equal(Map.of("key", "committed"), fixture.node.localState(), "the original committed command must still apply");
            equal(2L, fixture.node.lastAppliedIndex(), "the original committed prefix must remain applicable");
        }
    }

    private static void overwrittenRequestCannotSucceed() throws Exception {
        try (Fixture fixture = new Fixture(state(2, null, 0, 0, List.of()))) {
            invokeLocked(fixture.node, "becomeLeaderLocked");
            setRunning(fixture.node, true);
            ExecutorService client = Executors.newSingleThreadExecutor();
            try {
                Future<KvRpc.ClientCommandResponse> original = client.submit(() -> fixture.node.submit(Command.put("key", "original")));
                TestSupport.await(() -> containsValue(fixture, "original"), Duration.ofSeconds(2), "the original request must enter the uncommitted log");

                // The new leader replaces the same log index with an entirely different request.
                fixture.node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(3, "new-leader", 0, 0,
                        List.of(new LogEntry(1, 3, Command.noop()), entry(2, 3, "key", "replacement")), 2));
                applyOnce(fixture.node);
                KvRpc.ClientCommandResponse response = original.get(2, TimeUnit.SECONDS);
                check(!response.success(), "a lost-leadership request must fail instead of accepting the replacement's result");
                equal(Map.of("key", "replacement"), fixture.node.localState(), "the new leader's committed request must still apply");
            } finally {
                fixture.node.stop();
                client.shutdownNow();
                check(client.awaitTermination(2, TimeUnit.SECONDS), "the test client must terminate");
            }
        }
    }

    private static void failedPersistenceStopsProtocolResponses() throws Exception {
        try (Fixture fixture = new Fixture(state(3, null, 0, 0, List.of()))) {
            // Make the destination non-replaceable without relying on OS-specific permission bits.
            Path stateFile = fixture.directory.resolve("raft-state.bin");
            Files.delete(stateFile);
            Files.createDirectory(stateFile);
            Files.writeString(stateFile.resolve("prevent-replacement"), "injected storage failure");
            boolean saveFailed = false;
            try {
                fixture.node.handleAppendEntries(new RaftRpc.AppendEntriesRequest(
                        3, "leader", 0, 0, List.of(entry(1, 3, "key", "not-durable")), 0));
            } catch (IllegalStateException expected) {
                saveFailed = true;
            }
            check(saveFailed, "the fault must prevent saving the new log entry");

            // Index 1 exists only in memory. A later probe must not turn it into a successful replica acknowledgment.
            boolean acknowledged;
            try {
                acknowledged = fixture.node.handleAppendEntries(
                        new RaftRpc.AppendEntriesRequest(3, "leader", 1, 3, List.of(), 0)).success();
            } catch (IllegalStateException stopped) {
                acknowledged = false;
            }
            check(!acknowledged, "after a persistence failure, an empty probe must not acknowledge volatile log entries");

            boolean snapshotAcknowledged;
            try {
                snapshotAcknowledged = fixture.node.handleInstallSnapshot(
                        new RaftRpc.InstallSnapshotRequest(3, "leader", 0, 0, Map.of())).success();
            } catch (IllegalStateException stopped) {
                snapshotAcknowledged = false;
            }
            check(!snapshotAcknowledged, "the failed node must not acknowledge snapshot RPCs either");
        }
    }

    private static void assertDurableTerm(Fixture fixture, long expectedTerm) throws Exception {
        equal(expectedTerm, fixture.saved().currentTerm, "observing a higher term must persist even when no log or vote changes");
        equal(null, fixture.saved().votedFor, "the previous term's vote must be cleared durably");
        try (RaftNodeHandle restarted = fixture.restart()) {
            RaftRpc.RequestVoteResponse oldRequest = restarted.node.handleRequestVote(
                    new RaftRpc.RequestVoteRequest(expectedTerm - 1, "old-candidate", 100, 100));
            check(!oldRequest.voteGranted(), "a restart must not forget the higher term and accept an old-term request");
            equal(expectedTerm, oldRequest.term(), "the restarted node must retain its term");
        }
    }

    private static boolean containsValue(Fixture fixture, String value) {
        try {
            return fixture.saved().log.stream().anyMatch(entry -> value.equals(entry.command().value()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PersistentState state(long term, String votedFor, long commit, long applied, List<LogEntry> log) {
        return new PersistentState(term, votedFor, 0, 0, commit, applied, log, Snapshot.empty());
    }

    private static LogEntry entry(long index, long term, String key, String value) {
        return new LogEntry(index, term, Command.put(key, value));
    }

    private static void applyOnce(RaftNode node) throws Exception {
        // Drive only the state-machine tick: real scheduling would add random elections to protocol unit tests.
        setRunning(node, true);
        try {
            invokeLocked(node, "applyTick");
        } finally {
            setRunning(node, false);
        }
    }

    private static void setRunning(RaftNode node, boolean value) throws ReflectiveOperationException {
        Field running = RaftNode.class.getDeclaredField("running");
        running.setAccessible(true);
        running.setBoolean(node, value);
    }

    private static void invokeLocked(RaftNode node, String name) throws Exception {
        Field lock = RaftNode.class.getDeclaredField("lock");
        lock.setAccessible(true);
        Method method = RaftNode.class.getDeclaredMethod(name);
        method.setAccessible(true);
        synchronized (lock.get(node)) {
            method.invoke(node);
        }
    }

    private static RaftNode newNode(Path directory) throws IOException {
        Map<String, Peer> peers = Map.of(
                "n1", new Peer("n1", "http://127.0.0.1:1"),
                "n2", new Peer("n2", "http://127.0.0.1:2"),
                "n3", new Peer("n3", "http://127.0.0.1:3"));
        return new RaftNode("n1", "http://127.0.0.1:1", peers, directory, 1000);
    }

    private static final class Fixture implements AutoCloseable {
        private final Path directory = Files.createTempDirectory("raftkv-protocol-test-");
        private final PersistentStore store = new PersistentStore(directory);
        private final RaftNode node;

        private Fixture(PersistentState initial) throws IOException {
            store.save(initial);
            node = newNode(directory);
        }

        private PersistentState saved() throws IOException {
            return store.load();
        }

        private RaftNodeHandle restart() throws IOException {
            node.stop();
            return new RaftNodeHandle(newNode(directory));
        }

        @Override
        public void close() throws Exception {
            node.stop();
            TestSupport.deleteTree(directory);
        }
    }

    private static final class RaftNodeHandle implements AutoCloseable {
        private final RaftNode node;

        private RaftNodeHandle(RaftNode node) {
            this.node = node;
        }

        @Override
        public void close() {
            node.stop();
        }
    }
}
