package raftkv.raft;

import raftkv.kv.Command;

import java.io.Serializable;

public record LogEntry(long index, long term, Command command) implements Serializable {
}

