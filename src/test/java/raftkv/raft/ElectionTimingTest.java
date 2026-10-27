package raftkv.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ElectionTimingTest {
    @Test void durableGrpcUsesExplicitlyLongerElectionButSameHeartbeat() {
        var simulation=ElectionTiming.fastSimulation();var durable=ElectionTiming.durableGrpc();
        assertEquals(260,simulation.minimumMillis());assertEquals(500,simulation.maximumExclusiveMillis());
        assertEquals(1000,durable.minimumMillis());assertEquals(2000,durable.maximumExclusiveMillis());
        assertEquals(simulation.heartbeatMillis(),durable.heartbeatMillis());
    }
    @Test void invalidAndHeartbeatConflictingConfigurationsAreRejected() {
        assertThrows(IllegalArgumentException.class,()->new ElectionTiming(0,1000,70));
        assertThrows(IllegalArgumentException.class,()->new ElectionTiming(1000,1000,70));
        assertThrows(IllegalArgumentException.class,()->new ElectionTiming(200,1000,70));
        assertThrows(IllegalArgumentException.class,()->new ElectionTiming(1000,2000,0));
    }
}
