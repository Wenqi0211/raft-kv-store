package raftkv.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GrpcNodeConfigTest {
    @Test void defaultsHaveFiveExplicitGrpcPeersAndIndependentDataDirectory() {
        var config=GrpcNodeMain.parse(new String[]{"--id","n3"});
        assertEquals(1000,config.timing().minimumMillis());assertEquals(2000,config.timing().maximumExclusiveMillis());
        assertEquals(5,config.peers().size());assertEquals("grpc://127.0.0.1:8103",config.url());
        assertTrue(config.directory().endsWith("grpc-nodes/n3"));
    }
    @Test void explicitMembershipAndStatePathArePreserved() {
        var config=GrpcNodeMain.parse(new String[]{"--id","other","--cluster","other=grpc://127.0.0.1:9001", "--data-dir","work/custom", "--snapshot-threshold","12"});
        assertEquals(1,config.peers().size());assertEquals(12,config.snapshotThreshold());assertTrue(config.directory().endsWith("work/custom"));
    }
    @Test void explicitElectionConfigurationIsValidated() {
        var config=GrpcNodeMain.parse(new String[]{"--id","n1","--election-min-ms","1500","--election-max-ms","3000","--heartbeat-ms","100"});
        assertEquals(1500,config.timing().minimumMillis());assertEquals(3000,config.timing().maximumExclusiveMillis());assertEquals(100,config.timing().heartbeatMillis());
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--election-min-ms","10"}));
    }
    @Test void inconsistentMembershipUnknownOptionsAndInvalidThresholdAreRejected() {
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","absent"}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--url","grpc://127.0.0.1:9999"}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--snapshot-threshold","0"}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--made-up","1"}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--cluster","n1=http://127.0.0.1:8001"}));
        assertThrows(IllegalArgumentException.class,()->GrpcNodeMain.parse(new String[]{"--id","n1","--cluster","n1=grpc://127.0.0.1:8001,n1=grpc://127.0.0.1:8002"}));
    }
}
