package zincflow.providers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/// Uses a real {@code git init} in a temp directory — available
/// wherever the test runner has git on PATH (same assumption as
/// the C# design doc). CI without git just skips this class via
/// {@link org.junit.jupiter.api.Assumptions}.
final class VersionControlProviderTest {

    private static boolean gitAvailable() {
        try {
            Process p = new ProcessBuilder("git", "--version")
                    .redirectErrorStream(true).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void statusWhenDisabledReportsDisabled() {
        var vc = new VersionControlProvider(Path.of("."), null, null, null);
        var s = vc.status();
        assertFalse(s.enabled);
        assertEquals("provider disabled", s.error);
    }

    @Test
    void statusOnFreshRepoIsClean(@TempDir Path dir) throws Exception {
        assumeGit();
        initRepo(dir);
        var vc = enabledProvider(dir);
        var s = vc.status();
        assertTrue(s.enabled);
        assertTrue(s.clean, "freshly-initialised repo with committed README should be clean");
        assertEquals(0, s.ahead);
        assertEquals(0, s.behind);
    }

    @Test
    void commitStagesAndCommits(@TempDir Path dir) throws Exception {
        assumeGit();
        initRepo(dir);
        Path newFile = dir.resolve("note.txt");
        Files.writeString(newFile, "hello");
        var vc = enabledProvider(dir);
        var r = vc.commit("note.txt", "add note");
        assertTrue(r.ok, r.stderr);
        assertTrue(vc.status().clean, "after commit the working tree should be clean");
    }

    @Test
    void commitBlankMessageRejected() {
        var vc = new VersionControlProvider(Path.of("."), null, null, null);
        vc.enable();
        var r = vc.commit("file", "");
        assertFalse(r.ok);
    }

    @Test
    void commandResultSurfacesExitCodeFromMissingBinary(@TempDir Path dir) throws Exception {
        // Point at a bogus "git" binary; commit() should fail cleanly
        // rather than throw.
        var vc = new VersionControlProvider(dir, "/no/such/git", "origin", "main");
        vc.enable();
        var r = vc.commit("x", "msg");
        assertFalse(r.ok);
        assertFalse(r.stderr.isEmpty(), "expected stderr-populated failure for missing binary");
    }

    // --- helpers ---

    private static void assumeGit() {
        org.junit.jupiter.api.Assumptions.assumeTrue(gitAvailable(),
                "git not on PATH — skipping");
    }

    private static VersionControlProvider enabledProvider(Path repo) {
        var vc = new VersionControlProvider(repo, null, null, null);
        vc.enable();
        return vc;
    }

    private static void initRepo(Path dir) throws Exception {
        run(dir, "git", "init", "-b", "main");
        run(dir, "git", "config", "user.email", "test@example.com");
        run(dir, "git", "config", "user.name",  "Zinc Test");
        Files.writeString(dir.resolve("README.md"), "hi\n");
        run(dir, "git", "add", "README.md");
        run(dir, "git", "commit", "-m", "init");
    }

    private static void run(Path dir, String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true).start();
        int exit = p.waitFor();
        if (exit != 0) {
            String out = new String(p.getInputStream().readAllBytes());
            throw new AssertionError("command " + List.of(cmd) + " failed: " + out);
        }
    }
}
