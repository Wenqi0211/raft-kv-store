package raftkv.raft;

import java.io.Serializable;

public record Peer(String id, String url) implements Serializable {
}

