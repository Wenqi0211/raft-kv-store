package raftkv.raft;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class PersistentState implements Serializable {
    public long currentTerm;
    public String votedFor;
    public long snapshotIndex;
    public long snapshotTerm;
    public long commitIndex;
    public long lastApplied;
    public List<LogEntry> log;
    public Snapshot snapshot;

    public PersistentState(
            long currentTerm,
            String votedFor,
            long snapshotIndex,
            long snapshotTerm,
            long commitIndex,
            long lastApplied,
            List<LogEntry> log,
            Snapshot snapshot
    ) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.log = new ArrayList<>(log);
        this.snapshot = snapshot;
    }

    public static PersistentState empty() {
        return new PersistentState(0, null, 0, 0, 0, 0, List.of(), Snapshot.empty());
    }
}

