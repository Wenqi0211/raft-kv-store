package raftkv.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class TestSupport {
    private TestSupport() {
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    public static void run(String name, CheckedRunnable test) throws Exception {
        try {
            test.run();
            System.out.println("PASS " + name);
        } catch (Exception | AssertionError failure) {
            System.err.println("FAIL " + name);
            throw failure;
        }
    }

    public static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void equal(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }

    public static void await(BooleanSupplier condition, Duration timeout, String message) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(10);
        }
    }

    public static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
