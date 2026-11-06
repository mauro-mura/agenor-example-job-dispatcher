package jobs.manager;

import com.zaxxer.hikari.HikariDataSource;
import dev.agenor.core.Agent;
import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.testsupport.Infra;
import jobs.service.ServiceNode;
import jobs.store.JobSchema;
import jobs.store.JobStore;
import jobs.store.TestDatabase;
import jobs.worker.FakeProcessor;
import jobs.worker.JobProcessor;
import jobs.worker.JobWorker;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;

/**
 * One test's world: a job table of its own, and the nodes that gather around it. Everything it
 * starts is added to {@code toClose}, which the test closes in reverse order.
 */
final class ManagerEnv {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final Duration HEARTBEAT = Duration.ofMillis(500);

    final String u = Infra.unique();
    final String prefix = "mgr-" + u;
    final String kind = "pdf-" + u;
    final HikariDataSource db = TestDatabase.freshDatabase();
    final JobStore store;
    final ManagerConfig config;
    ServiceNode managerNode;
    TestProducer producer;

    /** Given to every node started after it is set. */
    dev.agenor.core.telemetry.AgenorTelemetry telemetry;
    /** Whether nodes started after it is set read the Redis dead letter stream. */
    boolean deadLetters;
    java.time.Duration heartbeat = HEARTBEAT;

    private final List<AutoCloseable> toClose;

    ManagerEnv(List<AutoCloseable> toClose, ManagerConfig config) {
        this.toClose = toClose;
        this.config = config;
        JobSchema.migrate(db);
        store = new JobStore(db, 3);
        toClose.add(db);
    }

    String workerId(String name) {
        return "w-" + name + "-" + u;
    }

    JobWorker worker(String name, JobProcessor processor, Duration initialGuess) {
        return new JobWorker(workerId(name), List.of(processor), 5, initialGuess,
                Duration.ofSeconds(2));
    }

    ServiceNode start(String nodeName, Agent... agents) {
        var node = ServiceNode.start("node-" + nodeName, prefix, Infra.redisUri(), Infra.jdbcUrl(),
                Infra.JDBC_USER, Infra.JDBC_PASSWORD, 2_000,
                new ServiceNode.Options(heartbeat, telemetry, deadLetters), agents);
        toClose.add(node);
        return node;
    }

    void startWorker(String name, JobProcessor processor, Duration initialGuess) {
        start(name, worker(name, processor, initialGuess));
    }

    JobWorker startWorkerAgent(String name, JobProcessor processor, Duration initialGuess) {
        var worker = worker(name, processor, initialGuess);
        start(name, worker);
        return worker;
    }

    JobManager startManager() {
        return startManager(new JobManager("m-" + u, store, config, Clock.systemUTC()));
    }

    JobManager startManager(JobManager manager) {
        managerNode = start("m", manager);
        return manager;
    }

    void startProducer() {
        producer = new TestProducer("p-" + u);
        start("p", producer);
    }

    JobSpec job(String suffix) {
        return new JobSpec("job-" + suffix + "-" + u, 0, kind, "in/" + suffix, 100);
    }

    Job awaitJob(String jobId, Predicate<Job> condition, String what) {
        return awaitJob(jobId, condition, what, WAIT);
    }

    Job awaitJob(String jobId, Predicate<Job> condition, String what, Duration timeout) {
        var deadline = Instant.now().plus(timeout);
        Job last = null;
        while (Instant.now().isBefore(deadline)) {
            last = store.find(jobId).orElse(null);
            if (last != null && condition.test(last)) {
                return last;
            }
            pause(100);
        }
        throw new AssertionError("job " + jobId + " never " + what + "; last seen: " + last);
    }

    static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
