package raftkv.raft;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Stores one Java-serialized state using a forced temporary file and an atomic
 * replacement. Saving requires a filesystem that supports atomic replacement
 * and directory synchronization; unsupported operations fail instead of silently
 * weakening that contract. These calls cannot guarantee behavior of storage
 * hardware, filesystem implementations, or a newly created directory's ancestors
 * during power loss.
 */
public final class PersistentStore {
    private final Path stateFile;

    public PersistentStore(Path dataDir) {
        this.stateFile = dataDir.toAbsolutePath().resolve("raft-state.bin");
    }

    public PersistentState load() throws IOException {
        if (Files.notExists(stateFile)) {
            return PersistentState.empty();
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(stateFile))) {
            Object decoded = in.readObject();
            if (!(decoded instanceof PersistentState state)) {
                throw new IOException("Invalid persisted Raft state: expected PersistentState");
            }
            return state;
        } catch (ClassNotFoundException e) {
            throw new IOException("Unable to read persisted Raft state", e);
        }
    }

    public void save(PersistentState state) throws IOException {
        Objects.requireNonNull(state, "state");
        Path directory = stateFile.getParent();
        Files.createDirectories(directory);
        // Check directory-sync support before replacing a previously valid state.
        // Some platforms (including common Windows providers) cannot open a
        // directory channel; report that limitation rather than claiming success.
        try (FileChannel directoryChannel = openSyncedDirectory(directory)) {
            Path tmp = Files.createTempFile(directory, "raft-state-", ".tmp");
            try {
                try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE);
                     ObjectOutputStream out = new ObjectOutputStream(Channels.newOutputStream(channel))) {
                    out.writeObject(state);
                    out.flush();
                    channel.force(true);
                }
                try {
                    Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException | UnsupportedOperationException e) {
                    throw new IOException("Atomic Raft state replacement is required by this store", e);
                }
                try {
                    directoryChannel.force(true);
                } catch (IOException | UnsupportedOperationException e) {
                    // The replacement has happened: callers must treat this as
                    // an uncertain save, not assume that the old state survived.
                    throw new IOException("Raft state was replaced, but directory synchronization failed; "
                            + "durability is not confirmed", e);
                }
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }
    }

    private static FileChannel openSyncedDirectory(Path directory) throws IOException {
        FileChannel channel;
        try {
            channel = FileChannel.open(directory, StandardOpenOption.READ);
        } catch (IOException | UnsupportedOperationException e) {
            throw new IOException("Cannot open Raft state directory for synchronization: " + directory, e);
        }
        try {
            channel.force(true);
            return channel;
        } catch (IOException | UnsupportedOperationException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw new IOException("Cannot synchronize Raft state directory: " + directory, failure);
        }
    }
}

