package jobs.testsupport;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** A node JVM started from a test, with its stdout captured line by line. */
public final class ChildNode implements AutoCloseable {

    private final String name;
    private final Process process;
    private final List<String> lines = new CopyOnWriteArrayList<>();

    private ChildNode(String name, Process process) {
        this.name = name;
        this.process = process;
        var reader = new Thread(() -> {
            try (var in = new BufferedReader(new InputStreamReader(process.getInputStream(),
                    StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
            } catch (IOException ignored) {
                // process gone
            }
        }, "child-" + name);
        reader.setDaemon(true);
        reader.start();
    }

    /** Starts {@code mainClass} with the test's own classpath and the given environment. */
    public static ChildNode start(String name, Class<?> mainClass, Map<String, String> env)
            throws IOException {
        var java = new File(System.getProperty("java.home"), "bin/java").getPath();
        var classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        var pb = new ProcessBuilder(java, "-cp", classpath, mainClass.getName());
        pb.redirectErrorStream(true);
        pb.environment().putAll(env);
        return new ChildNode(name, pb.start());
    }

    public List<String> lines() {
        return new ArrayList<>(lines);
    }

    /** First checkpoint that starts with {@code prefix}, or throws after {@code timeout}. */
    public String awaitCheckpoint(String prefix, Duration timeout) {
        var wanted = "CHECKPOINT " + prefix;
        var deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (var line : lines) {
                if (line.startsWith(wanted)) {
                    return line;
                }
            }
            if (!process.isAlive() && lines.stream().noneMatch(l -> l.startsWith(wanted))) {
                break;
            }
            pause();
        }
        throw new AssertionError(name + ": no '" + wanted + "' within " + timeout
                + " — output so far:\n" + String.join("\n", lines));
    }

    /** True if no checkpoint starting with {@code prefix} shows up within {@code window}. */
    public boolean staysQuiet(String prefix, int alreadySeen, Duration window) {
        var wanted = "CHECKPOINT " + prefix;
        var deadline = Instant.now().plus(window);
        while (Instant.now().isBefore(deadline)) {
            if (lines.stream().filter(l -> l.startsWith(wanted)).count() > alreadySeen) {
                return false;
            }
            pause();
        }
        return true;
    }

    public boolean isAlive() {
        return process.isAlive();
    }

    public long count(String prefix) {
        return lines.stream().filter(l -> l.startsWith("CHECKPOINT " + prefix)).count();
    }

    /** SIGKILL: no stop(), no unregister, no shutdown hook. */
    public void kill() {
        process.destroyForcibly();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        kill();
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
