package jobs.service;

import jobs.manager.ManagerConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Everything a node needs, read from environment variables. A missing or malformed one fails at
 * start with its name, not later with a null.
 *
 * <pre>
 * both:     NODE_ROLE (manager|worker), NODE_ID, AGENT_ID, REDIS_URI, JDBC_URL, JDBC_USER,
 *           JDBC_PASSWORD; GROUP_PREFIX, PENDING_MS, HEARTBEAT_MS, OTEL_EXPORTER_OTLP_ENDPOINT
 * worker:   WORKER_CAPABILITIES, STORAGE_ROOT, CAPACITY, INITIAL_GUESS_MS, SIMULATED_WORK_MS
 * manager:  MAX_ATTEMPTS, CFP_TIMEOUT_MS, DEADLINE_MARGIN_MS, STALE_AFTER_MS, BATCH_SIZE,
 *           SUMMARY_PORT, CONSOLE_PORT (0 turns either off)
 * submit:   STORAGE_ROOT (the PDFs under {@code in/}), SUBMIT_KIND; runs once and exits
 * </pre>
 *
 * <p>{@code HEARTBEAT_MS} must stay well under {@code STALE_AFTER_MS}: a worker whose row is not
 * refreshed within it stops being a candidate, without any error anywhere.
 */
public record ServiceConfig(
        Role role, String nodeId, String agentId, String groupPrefix, String redisUri,
        String jdbcUrl, String jdbcUser, String jdbcPassword, long pendingMs, Duration heartbeat,
        String otlpEndpoint,
        List<String> capabilities, Path storageRoot, int capacity, Duration initialGuess,
        int maxAttempts, ManagerConfig manager, int summaryPort, int consolePort,
        Duration simulatedWork, String submitKind) {

    public enum Role { MANAGER, WORKER, SUBMIT }

    public static ServiceConfig from(Map<String, String> env) {
        var role = switch (required(env, "NODE_ROLE").toLowerCase()) {
            case "manager" -> Role.MANAGER;
            case "worker" -> Role.WORKER;
            case "submit" -> Role.SUBMIT;
            default -> throw new IllegalArgumentException(
                    "NODE_ROLE must be manager, worker or submit: " + env.get("NODE_ROLE"));
        };
        var heartbeat = Duration.ofMillis(number(env, "HEARTBEAT_MS", 10_000));
        var staleAfter = Duration.ofMillis(number(env, "STALE_AFTER_MS", 90_000));
        if (role == Role.MANAGER && heartbeat.multipliedBy(2).compareTo(staleAfter) > 0) {
            throw new IllegalArgumentException("HEARTBEAT_MS (" + heartbeat.toMillis()
                    + ") must be well under STALE_AFTER_MS (" + staleAfter.toMillis()
                    + "): workers would be dropped as candidates while alive");
        }
        return new ServiceConfig(
                role,
                required(env, "NODE_ID"),
                required(env, "AGENT_ID"),
                env.getOrDefault("GROUP_PREFIX", "agenor-example-job-dispatcher"),
                required(env, "REDIS_URI"),
                required(env, "JDBC_URL"),
                required(env, "JDBC_USER"),
                required(env, "JDBC_PASSWORD"),
                number(env, "PENDING_MS", 30_000),
                heartbeat,
                blankToNull(env.get("OTEL_EXPORTER_OTLP_ENDPOINT")),
                Arrays.stream(env.getOrDefault("WORKER_CAPABILITIES", "").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList(),
                Path.of(env.getOrDefault("STORAGE_ROOT", "storage")),
                (int) number(env, "CAPACITY", 5),
                Duration.ofMillis(number(env, "INITIAL_GUESS_MS", 2_000)),
                (int) number(env, "MAX_ATTEMPTS", 3),
                new ManagerConfig(
                        Duration.ofMillis(number(env, "CFP_TIMEOUT_MS", 3_000)),
                        Duration.ofMillis(number(env, "DEADLINE_MARGIN_MS", 30_000)),
                        staleAfter,
                        (int) number(env, "BATCH_SIZE", 10)),
                (int) number(env, "SUMMARY_PORT", 8081),
                (int) number(env, "CONSOLE_PORT", 8080),
                Duration.ofMillis(number(env, "SIMULATED_WORK_MS", 0)),
                env.getOrDefault("SUBMIT_KIND", "pdf-to-text"));
    }

    private static String required(Map<String, String> env, String name) {
        var value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing environment variable " + name);
        }
        return value;
    }

    private static long number(Map<String, String> env, String name, long fallback) {
        var value = env.get(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number: " + value);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
