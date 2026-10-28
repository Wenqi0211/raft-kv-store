package raftkv.tool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class GrpcProcessClusterLifecycleTest {
    @TempDir Path directory;

    @Test void closesAnOwnedJvmAndRejectsRestartAfterCleanup() throws Exception {
        GrpcProcessCluster cluster = new GrpcProcessCluster(directory);
        var child = cluster.children.get(0);
        try {
            cluster.start(child);
            Process process = child.process;
            cluster.close();
            assertFalse(process.isAlive(), "cleanup must terminate every registered child");
            assertThrows(IOException.class, () -> cluster.start(child));
            assertSame(process, child.process, "rejected restart must not spawn another JVM");
        } finally { cluster.close(); }
    }

    @Test void closingBeforeStartupPreventsAnyChildFromBeingCreated() throws Exception {
        GrpcProcessCluster cluster = new GrpcProcessCluster(directory);
        try {
            cluster.close();
            assertThrows(IOException.class, () -> cluster.start(cluster.children.get(0)));
            assertTrue(cluster.processes.isEmpty());
        } finally { cluster.close(); }
    }
}
