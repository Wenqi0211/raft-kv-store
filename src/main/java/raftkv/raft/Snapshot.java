package raftkv.raft;

import raftkv.kv.ClientSession;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public record Snapshot(long lastIncludedIndex,long lastIncludedTerm,Map<String,String> data,
                       Map<String,ClientSession> sessions) implements Serializable {
    public Snapshot {
        data = new LinkedHashMap<>(data);
        sessions = sessions == null ? new LinkedHashMap<>() : new LinkedHashMap<>(sessions);
    }
    public Snapshot(long index,long term,Map<String,String> data) { this(index,term,data,Map.of()); }
    public static Snapshot empty() { return new Snapshot(0,0,Map.of(),Map.of()); }
}
