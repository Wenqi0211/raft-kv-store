package raftkv.raft;

import java.io.Serializable;
import raftkv.kv.ClientSession;
import java.util.List;
import java.util.Map;

public final class RaftRpc {
    private RaftRpc() {
    }

    public record RequestVoteRequest(
            long term,
            String candidateId,
            long lastLogIndex,
            long lastLogTerm
    ) implements Serializable {
    }

    public record RequestVoteResponse(
            long term,
            boolean voteGranted
    ) implements Serializable {
    }

    public record AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit
    ) implements Serializable {
    }

    public record AppendEntriesResponse(
            long term,
            boolean success,
            long matchIndex
    ) implements Serializable {
    }

    public record InstallSnapshotRequest(
            long term,
            String leaderId,
            long lastIncludedIndex,
            long lastIncludedTerm,
            Map<String, String> data,
            Map<String, ClientSession> sessions
    ) implements Serializable {
        public InstallSnapshotRequest(long term,String leaderId,long index,long snapshotTerm,Map<String,String> data) {
            this(term,leaderId,index,snapshotTerm,data,Map.of());
        }
    }

    public record InstallSnapshotResponse(
            long term,
            boolean success,
            long lastIncludedIndex
    ) implements Serializable {
    }
}

