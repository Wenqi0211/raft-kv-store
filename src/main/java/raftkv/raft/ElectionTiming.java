package raftkv.raft;

/** Operational availability parameters; they do not relax commit or persistence rules. */
public record ElectionTiming(int minimumMillis,int maximumExclusiveMillis,int heartbeatMillis) {
    public ElectionTiming {
        if(heartbeatMillis<1||minimumMillis<1||maximumExclusiveMillis<=minimumMillis
                ||(long)minimumMillis<=3L*heartbeatMillis)
            throw new IllegalArgumentException("Require positive heartbeat, election minimum >3 heartbeats, and maximum >minimum");
    }
    public static ElectionTiming fastSimulation(){return new ElectionTiming(260,500,70);}
    public static ElectionTiming durableGrpc(){return new ElectionTiming(1000,2000,70);}
}
