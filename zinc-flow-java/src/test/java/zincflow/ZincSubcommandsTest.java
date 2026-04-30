package zincflow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the validate + bench subcommands introduced for CLI parity
/// with zinc-flow-csharp. Exit codes and stdout shape are asserted
/// directly — these are part of the user-facing contract.
final class ZincSubcommandsTest {

    @Test
    void validateReturnsZeroOnCleanConfig(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("config.yaml");
        Files.writeString(cfg, """
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                      config:
                        prefix: "[hi] "
                """);

        try (Captured out = Captured.stdoutStderr()) {
            int code = Zinc.runValidate(cfg.toString());
            assertEquals(0, code, "clean config should exit 0; got stdout:\n" + out.stdout());
            assertTrue(out.stdout().contains("no issues"),
                    "expected 'no issues' in stdout, got:\n" + out.stdout());
        }
    }

    @Test
    void validateReturnsTwoWhenFileMissing() {
        try (Captured out = Captured.stdoutStderr()) {
            int code = Zinc.runValidate("/does/not/exist.yaml");
            assertEquals(2, code);
            assertTrue(out.stderr().contains("config file not found"),
                    "expected 'config file not found' in stderr, got:\n" + out.stderr());
        }
    }

    @Test
    void validateReturnsOneOnConfigError(@TempDir Path dir) throws Exception {
        // Malformed connection target — references a processor that
        // doesn't exist. ConfigLoader rejects this with a clear message;
        // runValidate catches the throw and surfaces it on stderr.
        Path cfg = dir.resolve("config.yaml");
        Files.writeString(cfg, """
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: LogAttribute
                  connections:
                    a:
                      success: [nonexistent]
                """);

        try (Captured out = Captured.stdoutStderr()) {
            int code = Zinc.runValidate(cfg.toString());
            assertEquals(1, code, "expected non-zero for invalid config; stdout:\n"
                    + out.stdout() + "\nstderr:\n" + out.stderr());
        }
    }

    @Test
    void benchRunsAndPrintsThroughput() {
        // Smoke — bench builds its own graph and runs a warmup + 4 passes.
        // Counts are big (up to 500K), but UpdateAttribute is cheap so
        // this still completes well inside the test timeout. We just
        // assert that the command exits normally and prints the expected
        // section headers.
        try (Captured out = Captured.stdoutStderr()) {
            Zinc.runBench();
            String o = out.stdout();
            assertTrue(o.contains("zinc-flow-java benchmark"), "expected banner in:\n" + o);
            assertTrue(o.contains("Pipeline throughput"), "expected results section in:\n" + o);
            assertTrue(o.contains("ff/s") || o.contains("<1ms"),
                    "expected rate or sub-ms marker in:\n" + o);
        }
    }

    /// Tiny stdout/stderr capture that restores the originals on close.
    /// Scoped to a single test; no cross-test sharing.
    private static final class Captured implements AutoCloseable {
        private final PrintStream savedOut;
        private final PrintStream savedErr;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final ByteArrayOutputStream err = new ByteArrayOutputStream();

        private Captured() {
            this.savedOut = System.out;
            this.savedErr = System.err;
            System.setOut(new PrintStream(out, true));
            System.setErr(new PrintStream(err, true));
        }

        static Captured stdoutStderr() { return new Captured(); }

        String stdout() { return out.toString(); }
        String stderr() { return err.toString(); }

        @Override public void close() {
            System.setOut(savedOut);
            System.setErr(savedErr);
        }
    }
}
