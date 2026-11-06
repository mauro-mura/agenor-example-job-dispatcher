package jobs.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.domain.JobState;
import jobs.testsupport.ChildNode;
import jobs.testsupport.Infra;
import jobs.service.ServiceNode;
import jobs.store.JobStore;
import jobs.store.TestDatabase;
import jobs.worker.WorkerMain;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A service made of real processes: a manager and workers as child JVMs, a job table of its own,
 * and a producer in the test JVM. Whatever it starts is killed (by the PID it holds, never by
 * pattern) when it is closed.
 *
 * <p>The children are the service's own {@code ServiceMain} with a processor swapped in, so what
 * is under test is the production wiring.
 */
final class Cluster implements AutoCloseable {

    static final Duration SETTLE = Duration.ofSeconds(120);

    final String u = Infra.unique();
    final String prefix = "cluster-" + u;
    final String kind = "doc-" + u;
    final String managerId = "m-" + u;
    final HikariDataSource db = TestDatabase.freshDatabase();
    final JobStore store = new JobStore(db, 3);
    final int summaryPort = freePort();
    final int consolePort = freePort();

    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();
    /** Every child ever started, in order, killed or not: executions are counted across all. */
    private final Map<String, ChildNode> children = new LinkedHashMap<>();
    private final List<ChildNode> everStarted = new ArrayList<>();
    private ServiceNode producerNode;
    private TestProducer producer;

    String workerId(String name) {
        return "w-" + name + "-" + u;
    }

    private Map<String, String> env(String role, String nodeName, String agentId) {
        var env = Infra.env(role, prefix, "node-" + nodeName, agentId);
        env.put("JDBC_URL", db.getJdbcUrl());
        env.put("KIND", kind);
        return env;
    }

    /** The manager: short deadlines so a test does not wait minutes, stale after 3 s. */
    ChildNode startManager() throws Exception {
        var env = env("manager", "m", managerId);
        env.put("MAX_ATTEMPTS", "3");
        env.put("CFP_TIMEOUT_MS", "3000");
        env.put("DEADLINE_MARGIN_MS", "5000");
        env.put("STALE_AFTER_MS", "3000");
        env.put("BATCH_SIZE", "10");
        env.put("SUMMARY_PORT", String.valueOf(summaryPort));
        env.put("CONSOLE_PORT", String.valueOf(consolePort));
        return start("manager", ManagerMain.class, env);
    }

    ChildNode startWorker(String name, String mode, long holdMs, long initialGuessMs)
            throws Exception {
        var env = env("worker", name, workerId(name));
        env.put("MODE", mode);
        env.put("HOLD_MS", String.valueOf(holdMs));
        env.put("INITIAL_GUESS_MS", String.valueOf(initialGuessMs));
        env.put("CAPACITY", "5");
        env.put("WORKER_CAPABILITIES", kind);
        return start("worker-" + name, WorkerMain.class, env);
    }

    private ChildNode start(String name, Class<?> main, Map<String, String> env) throws Exception {
        var child = ChildNode.start(name, main, env);
        // a restarted node reuses its name: the previous process must be dead by now
        children.put(name, child);
        everStarted.add(child);
        child.awaitCheckpoint("started", Duration.ofSeconds(90));
        return child;
    }

    ChildNode child(String name) {
        return children.get(name);
    }

    void kill(String name) {
        children.get(name).kill();
    }

    void startProducer() {
        producer = new TestProducer("p-" + u);
        producerNode = ServiceNode.start("node-p", prefix, Infra.redisUri(), db.getJdbcUrl(),
                Infra.JDBC_USER, Infra.JDBC_PASSWORD, 2_000,
                new ServiceNode.Options(Duration.ofMillis(500), null, false), producer);
    }

    JobSpec job(String id) {
        return new JobSpec(id + "-" + u, 0, kind, "in/" + id, 100);
    }

    void submit(JobSpec spec) {
        producer.submit(spec);
    }

    List<JobSpec> submitMany(int count, String idPrefix) {
        var jobs = new ArrayList<JobSpec>();
        for (int i = 1; i <= count; i++) {
            var spec = job(idPrefix + i);
            jobs.add(spec);
            submit(spec);
        }
        return jobs;
    }

    /** The rows of {@code ids}, in order; a job not on the table is {@code null}. */
    List<Job> rows(List<JobSpec> ids) {
        return ids.stream().map(s -> store.find(s.jobId()).orElse(null)).toList();
    }

    /** Waits until every job is terminal ({@code DONE} or {@code FAILED}), or the time is up. */
    List<Job> awaitSettled(List<JobSpec> ids, Duration timeout) {
        var deadline = Instant.now().plus(timeout);
        List<Job> rows = rows(ids);
        while (Instant.now().isBefore(deadline)) {
            rows = rows(ids);
            if (rows.stream().allMatch(j -> j != null
                    && (j.state() == JobState.DONE || j.state() == JobState.FAILED))) {
                return rows;
            }
            ManagerEnv.pause(250);
        }
        return rows;
    }

    /** How many job executions every child has started, summed. */
    long executions() {
        return everStarted.stream().mapToLong(c -> c.count("running")).sum();
    }

    long executions(String childName) {
        return children.get(childName).count("running");
    }

    long finishedExecutions() {
        return everStarted.stream().mapToLong(c -> c.count("done")).sum();
    }

    /** What the manager's HTTP summary says, or null if it does not answer. */
    JsonNode summary() {
        try {
            var response = http.send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + summaryPort + "/jobs/summary")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? json.readTree(response.body()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** The dead letters the manager's console lists, or empty if it does not answer. */
    List<JsonNode> deadLetters() {
        try {
            var response = http.send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + consolePort + "/api/deadletters?limit=200"))
                    .GET().build(), HttpResponse.BodyHandlers.ofString());
            var entries = new ArrayList<JsonNode>();
            json.readTree(response.body()).path("data").forEach(entries::add);
            return entries;
        } catch (Exception e) {
            return List.of();
        }
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** The output of every child, for reading after a failure; {@code -Dcluster.dump=true}. */
    private void dump() {
        for (var entry : children.entrySet()) {
            var lines = entry.getValue().lines();
            System.out.println("==== " + entry.getKey() + " (" + lines.size() + " lines, "
                    + (entry.getValue().isAlive() ? "alive" : "dead") + ")");
            lines.stream().skip(Math.max(0, lines.size() - 60)).forEach(System.out::println);
        }
    }

    @Override
    public void close() {
        if (Boolean.getBoolean("cluster.dump")) {
            dump();
        }
        // by the PID of the process we started, one by one
        everStarted.forEach(ChildNode::kill);
        if (producerNode != null) {
            producerNode.close();
        }
        db.close();
    }
}
