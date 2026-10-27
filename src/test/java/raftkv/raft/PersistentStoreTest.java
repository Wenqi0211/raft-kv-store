package raftkv.raft;

import raftkv.kv.Command;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static raftkv.test.TestSupport.check;
import static raftkv.test.TestSupport.deleteTree;
import static raftkv.test.TestSupport.equal;
import static raftkv.test.TestSupport.run;

public final class PersistentStoreTest {
    public static void main(String[] args) throws Exception {
        run("missing state starts empty", PersistentStoreTest::missingStateStartsEmpty);
        run("state and snapshot survive replacement", PersistentStoreTest::stateAndSnapshotRoundTrip);
        run("existing serialization format remains readable", PersistentStoreTest::legacyFormatRemainsReadable);
        run("stale temporary files cannot replace committed state", PersistentStoreTest::staleTemporaryFileIsIgnored);
        run("corrupt state fails instead of starting empty", PersistentStoreTest::corruptStateFails);
        run("unexpected serialized type fails with IOException", PersistentStoreTest::unexpectedTypeFails);
        run("failed serialization preserves previous state", PersistentStoreTest::failedSavePreservesPreviousState);
        run("failed replacement preserves target and cleans temporary file", PersistentStoreTest::failedReplacementPreservesTarget);
    }

    private static void missingStateStartsEmpty() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            PersistentState state = new PersistentStore(directory).load();
            equal(0L, state.currentTerm, "initial term");
            equal(null, state.votedFor, "initial vote");
            check(state.log.isEmpty(), "initial log must be empty");
            check(state.snapshot.data().isEmpty(), "initial snapshot must be empty");
        } finally {
            deleteTree(directory);
        }
    }

    private static void stateAndSnapshotRoundTrip() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            PersistentStore store = new PersistentStore(directory.resolve("node"));
            PersistentState original = sampleState(3);
            store.save(original);
            assertState(original, new PersistentStore(directory.resolve("node")).load());

            PersistentState replacement = sampleState(4);
            replacement.votedFor = "n3";
            replacement.log.add(new LogEntry(7, 4, Command.put("next", "value")));
            store.save(replacement);
            assertState(replacement, new PersistentStore(directory.resolve("node")).load());
        } finally {
            deleteTree(directory);
        }
    }

    private static void legacyFormatRemainsReadable() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            PersistentState state = sampleState(8);
            try (ObjectOutputStream out = new ObjectOutputStream(
                    Files.newOutputStream(directory.resolve("raft-state.bin")))) {
                out.writeObject(state);
            }
            assertState(state, new PersistentStore(directory).load());
        } finally {
            deleteTree(directory);
        }
    }

    private static void staleTemporaryFileIsIgnored() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            PersistentStore store = new PersistentStore(directory);
            PersistentState original = sampleState(3);
            store.save(original);
            // A leftover object at the old fixed temporary path must neither be
            // loaded as state nor prevent a later successful save.
            Path staleTemporary = directory.resolve("raft-state.bin.tmp");
            Files.createDirectory(staleTemporary);
            Files.writeString(staleTemporary.resolve("leftover"), "incomplete old save");
            assertState(original, store.load());
            PersistentState replacement = sampleState(5);
            store.save(replacement);
            assertState(replacement, store.load());
            equal("incomplete old save", Files.readString(staleTemporary.resolve("leftover")),
                    "an unrelated leftover must not be reused");
        } finally {
            deleteTree(directory);
        }
    }

    private static void corruptStateFails() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            Files.write(directory.resolve("raft-state.bin"), new byte[]{0x01, 0x02, 0x03});
            expectIOException(() -> new PersistentStore(directory).load());
        } finally {
            deleteTree(directory);
        }
    }

    private static void unexpectedTypeFails() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            try (ObjectOutputStream out = new ObjectOutputStream(
                    Files.newOutputStream(directory.resolve("raft-state.bin")))) {
                out.writeObject("not a PersistentState");
            }
            expectIOException(() -> new PersistentStore(directory).load());
        } finally {
            deleteTree(directory);
        }
    }

    private static void failedSavePreservesPreviousState() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            PersistentStore store = new PersistentStore(directory);
            PersistentState original = sampleState(3);
            store.save(original);
            PersistentState invalid = sampleState(9);
            invalid.log = new FailingLog();
            expectIOException(() -> store.save(invalid));
            assertState(original, store.load());
            assertOnlyStateFile(directory);
        } finally {
            deleteTree(directory);
        }
    }

    private static void failedReplacementPreservesTarget() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-store-test-");
        try {
            Path target = directory.resolve("raft-state.bin");
            Files.createDirectory(target);
            Files.writeString(target.resolve("existing"), "keep me");
            expectIOException(() -> new PersistentStore(directory).save(sampleState(3)));
            equal("keep me", Files.readString(target.resolve("existing")), "failed move must preserve target");
            assertOnlyStateFile(directory);
        } finally {
            deleteTree(directory);
        }
    }

    private static PersistentState sampleState(long term) {
        Snapshot snapshot = new Snapshot(4, 2, Map.of("saved", "before snapshot", "unicode", "你好"));
        return new PersistentState(term, "n2", 4, 2, 6, 5,
                List.of(new LogEntry(5, term, Command.put("key", "value")),
                        new LogEntry(6, term, Command.delete("saved"))), snapshot);
    }

    private static void assertState(PersistentState expected, PersistentState actual) {
        equal(expected.currentTerm, actual.currentTerm, "current term");
        equal(expected.votedFor, actual.votedFor, "vote");
        equal(expected.snapshotIndex, actual.snapshotIndex, "snapshot index");
        equal(expected.snapshotTerm, actual.snapshotTerm, "snapshot term");
        equal(expected.commitIndex, actual.commitIndex, "commit index");
        equal(expected.lastApplied, actual.lastApplied, "applied index");
        equal(expected.log, actual.log, "log including commands and request ids");
        equal(expected.snapshot, actual.snapshot, "snapshot metadata and all keys");
    }

    private static void assertOnlyStateFile(Path directory) throws IOException {
        try (var files = Files.list(directory)) {
            equal(List.of("raft-state.bin"), files.map(path -> path.getFileName().toString()).sorted().toList(),
                    "temporary save files must be removed after failure");
        }
    }

    private static void expectIOException(IoAction action) throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            return;
        }
        throw new AssertionError("Expected IOException");
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws Exception;
    }

    private static final class FailingLog extends ArrayList<LogEntry> {
        private static final long serialVersionUID = 1L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new IOException("injected serialization failure");
        }
    }
}
