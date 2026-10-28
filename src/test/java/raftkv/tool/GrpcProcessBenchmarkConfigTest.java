package raftkv.tool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raftkv.kv.CommandResult;
import raftkv.kv.KvRpc;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class GrpcProcessBenchmarkConfigTest {
    @TempDir Path directory;

    @Test void rejectsAnExistingEvidenceDirectoryBeforeStartingAnyProcess() throws Exception {
        Path evidence = directory.resolve("prior-run");
        Files.createDirectory(evidence);
        Path marker = evidence.resolve("summary.json");
        Files.writeString(marker, "preserve this evidence");
        assertThrows(FileAlreadyExistsException.class,
                () -> GrpcProcessBenchmarkMain.main(new String[]{"--report-dir", evidence.toString()}));
        assertEquals("preserve this evidence", Files.readString(marker));
        try (var files = Files.list(evidence)) { assertEquals(1, files.count()); }
    }

    @Test void requiresTrafficTimeOnBothSidesOfTheKill() {
        assertThrows(IllegalArgumentException.class, () -> parse("--duration", "5", "--kill-after", "4"));
        assertThrows(IllegalArgumentException.class, () -> parse("--kill-after", "0"));
        assertThrows(IllegalArgumentException.class, () -> parse("--duration", "500"));
    }

    @Test void rejectsUnknownDuplicateAndIncompleteOptions() {
        assertThrows(IllegalArgumentException.class, () -> parse("--unknown", "1"));
        assertThrows(IllegalArgumentException.class, () -> parse("--keys", "64", "--keys", "128"));
        assertThrows(IllegalArgumentException.class, () -> parse("--keys"));
        assertThrows(IllegalArgumentException.class, () -> parse("--clients", "5", "--keys", "4"));
    }

    @Test void recordsExplicitWorkloadAndAbsoluteEvidencePath() {
        var config = parse("--duration", "12", "--kill-after", "3", "--seed", "42",
                "--report-dir", "reports/config-test");
        assertEquals(12, config.duration());
        assertEquals(3, config.killAfter());
        assertEquals(42, config.seed());
        assertTrue(config.report().isAbsolute());
    }

    @Test void recordsPositiveTermOnlyForMatchingFreshLeaderObservation() {
        var response = KvRpc.ClientCommandResponse.ok(CommandResult.ok("before-crash"), "n1", "grpc://127.0.0.1:9001");
        assertEquals(7, GrpcProcessBenchmarkMain.validateLeaderObservation(response, "n1",
                "grpc://127.0.0.1:9001", leaderStatus()));
    }

    @Test void rejectsSuccessfulRepliesFromAFormerLeaderOrStaleStatus() {
        var former = KvRpc.ClientCommandResponse.ok(CommandResult.ok("before-crash"), "n2", "grpc://127.0.0.1:9002");
        assertThrows(IllegalStateException.class, () -> GrpcProcessBenchmarkMain.validateLeaderObservation(former,
                "n1", "grpc://127.0.0.1:9001", leaderStatus()));
        var current = KvRpc.ClientCommandResponse.ok(CommandResult.ok("before-crash"), "n1", "grpc://127.0.0.1:9001");
        assertThrows(IllegalStateException.class, () -> GrpcProcessBenchmarkMain.validateLeaderObservation(current,
                "n1", "grpc://127.0.0.1:9001", leaderStatus().replace("LEADER", "FOLLOWER")));
        assertThrows(IllegalStateException.class, () -> GrpcProcessBenchmarkMain.validateLeaderObservation(current,
                "n1", "grpc://127.0.0.1:9001", leaderStatus().replace("\"term\":7", "\"term\":0")));
    }

    private static String leaderStatus() {
        return "{\"id\":\"n1\",\"url\":\"grpc://127.0.0.1:9001\",\"role\":\"LEADER\","
                + "\"running\":true,\"term\":7,\"leaderId\":\"n1\"}";
    }

    private static GrpcProcessBenchmarkMain.Config parse(String... args) {
        return GrpcProcessBenchmarkMain.Config.parse(args);
    }
}
