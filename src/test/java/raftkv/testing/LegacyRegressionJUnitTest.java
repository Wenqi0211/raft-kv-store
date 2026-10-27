package raftkv.testing;

import org.junit.jupiter.api.Test;
import raftkv.raft.PersistentStoreTest;
import raftkv.raft.RaftNodeRegressionTest;
import raftkv.raft.ClusterIntegrationTest;

class LegacyRegressionJUnitTest {
    @Test void persistenceEightChecks() throws Exception { PersistentStoreTest.main(new String[0]); }
    @Test void protocolTwelveChecks() throws Exception { RaftNodeRegressionTest.main(new String[0]); }
    @Test void realHttpThreeScenarios() throws Exception { ClusterIntegrationTest.main(new String[0]); }
}
