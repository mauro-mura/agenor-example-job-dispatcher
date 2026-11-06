package jobs.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Valkey and PostgreSQL, started once per test JVM and shared by every IT class. Each test
 * isolates itself with its own consumer group prefix and agent ids, so shared containers do not
 * leak streams between tests.
 */
public final class Infra {

    public static final String JDBC_USER = "agenor";
    public static final String JDBC_PASSWORD = "agenor_test";

    private static GenericContainer<?> postgres;
    private static GenericContainer<?> valkey;
    private static GenericContainer<?> jaeger;

    private Infra() {}

    private static RuntimeException startFailure;

    /**
     * Starts the three containers once. A failure stops whatever had started and is remembered:
     * every later call fails at once with the same cause, instead of starting another set of
     * containers per test and leaving the earlier ones running.
     */
    public static synchronized void ensureStarted() {
        if (postgres != null) {
            return;
        }
        if (startFailure != null) {
            throw startFailure;
        }
        var pg = new GenericContainer<>("postgres:16-alpine")
                .withExposedPorts(5432)
                .withEnv("POSTGRES_DB", "agenor_test")
                .withEnv("POSTGRES_USER", JDBC_USER)
                .withEnv("POSTGRES_PASSWORD", JDBC_PASSWORD)
                // the entrypoint runs a throwaway server for initdb: wait for the real one
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                .withStartupTimeout(Duration.ofSeconds(90));
        var vk = new GenericContainer<>("valkey/valkey:8")
                .withExposedPorts(6379)
                .withStartupTimeout(Duration.ofSeconds(60));
        // Jaeger starts with the others, not later: the Docker client reuses a connection to the
        // socket that podman closes after a minute without calls, and a container started after
        // the long tests failed with Broken pipe.
        var jg = new GenericContainer<>("jaegertracing/all-in-one:1.62.0")
                .withExposedPorts(4318, 16686)
                .withEnv("COLLECTOR_OTLP_ENABLED", "true")
                .withStartupTimeout(Duration.ofSeconds(90));
        var started = new java.util.ArrayList<GenericContainer<?>>();
        try {
            for (var container : List.of(pg, vk, jg)) {
                container.start();
                started.add(container);
            }
        } catch (RuntimeException e) {
            started.forEach(GenericContainer::stop);
            startFailure = new IllegalStateException(
                    "test containers did not start; every integration test will fail the same way: "
                            + e.getMessage(), e);
            throw startFailure;
        }
        postgres = pg;
        valkey = vk;
        jaeger = jg;
    }

    public static String redisUri() {
        ensureStarted();
        return "redis://" + valkey.getHost() + ":" + valkey.getMappedPort(6379);
    }

    public static String jdbcUrl() {
        ensureStarted();
        return "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432)
                + "/agenor_test";
    }

    /** Environment shared by every child node of a test; the caller adds the rest. */
    public static java.util.Map<String, String> env(String role, String groupPrefix, String nodeId,
                                                    String agentId) {
        var env = new java.util.HashMap<String, String>();
        env.put("NODE_ROLE", role);
        env.put("HEARTBEAT_MS", "500");
        env.put("GROUP_PREFIX", groupPrefix);
        env.put("NODE_ID", nodeId);
        env.put("AGENT_ID", agentId);
        env.put("REDIS_URI", redisUri());
        env.put("JDBC_URL", jdbcUrl());
        env.put("JDBC_USER", JDBC_USER);
        env.put("JDBC_PASSWORD", JDBC_PASSWORD);
        env.put("PENDING_MS", "2000");
        return env;
    }

    /**
     * Entries delivered to {@code nodeId} and not acknowledged yet: {@code XPENDING} on its
     * stream. Stream and group names are the ones {@code RedisMessagingConfig} derives.
     */
    public static long pendingEntries(String groupPrefix, String nodeId) {
        var client = io.lettuce.core.RedisClient.create(redisUri());
        try (var conn = client.connect()) {
            var pending = conn.sync().xpending(groupPrefix + ":node:" + nodeId,
                    groupPrefix + ":cg:node");
            return pending.getCount();
        } finally {
            client.shutdown();
        }
    }

    /** Freezes the container's processes: connections hang, nothing is refused or reset. */
    public static void pauseValkey() { pause(valkey); }

    public static void unpauseValkey() { unpause(valkey); }

    public static void pausePostgres() { pause(postgres); }

    public static void unpausePostgres() { unpause(postgres); }

    private static void pause(GenericContainer<?> container) {
        runContainerCommand("pause", container.getContainerId());
    }

    private static void unpause(GenericContainer<?> container) {
        runContainerCommand("unpause", container.getContainerId());
    }

    /**
     * Through the CLI, not the Java client: after a minute without Docker calls the socket's
     * keep-alive connection is closed on podman's side and the client's next call fails with
     * {@code Broken pipe}, which is what made the pause tests error before they began.
     */
    private static void runContainerCommand(String verb, String containerId) {
        for (var tool : List.of("podman", "docker")) {
            try {
                var process = new ProcessBuilder(tool, verb, containerId)
                        .redirectErrorStream(true).start();
                var output = new String(process.getInputStream().readAllBytes());
                if (process.waitFor() == 0) {
                    return;
                }
                throw new IllegalStateException(tool + " " + verb + " failed: " + output);
            } catch (java.io.IOException notInstalled) {
                // try the next tool
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("neither podman nor docker could " + verb + " " + containerId);
    }

    /** OTLP over HTTP, as {@code OtelTelemetryFactory} wants it. */
    public static String jaegerOtlpEndpoint() {
        ensureStarted();
        return "http://" + jaeger.getHost() + ":" + jaeger.getMappedPort(4318);
    }

    /** Base URL of Jaeger's query API and UI. */
    public static String jaegerQueryUrl() {
        ensureStarted();
        return "http://" + jaeger.getHost() + ":" + jaeger.getMappedPort(16686);
    }

    /** Short unique suffix: consumer group prefix and agent ids of one test. */
    public static String unique() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
