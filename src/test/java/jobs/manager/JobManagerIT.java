package jobs.manager;

import com.zaxxer.hikari.HikariDataSource;
import dev.agenor.core.AgentDescriptor;
import dev.agenor.core.AgentEndpoint;
import dev.agenor.core.AgentStatus;
import dev.agenor.core.dialogue.DialogueMessage;
import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.domain.JobState;
import jobs.testsupport.Infra;
import jobs.store.JobSchema;
import jobs.store.JobStore;
import jobs.store.TestDatabase;
import jobs.worker.FakeProcessor;
import jobs.worker.JobProcessor;
import jobs.worker.JobWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Manager, workers and producer on separate nodes over Valkey, sharing PostgreSQL for
 * the directory and each test with a job table of its own.
 */
@DisplayName("JobManager over Valkey + PostgreSQL")
class JobManagerIT {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final Duration HEARTBEAT = Duration.ofMillis(500);
    private static final ManagerConfig CONFIG = new ManagerConfig(
            Duration.ofSeconds(4), Duration.ofSeconds(30), Duration.ofSeconds(3), 10);

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private CountDownLatch gate() {
        var gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    // ---------------------------------------------------------------------------------- happy path

    @Test
    @DisplayName("a job published on the topic ends DONE, with the worker and the output on its row")
    void publishedJobReachesDone() {
        var env = new ManagerEnv(toClose, CONFIG);
        env.startWorker("a", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();
        var job = env.job("one");

        env.producer.submit(job);

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.attempt()).isEqualTo(1);
        assertThat(done.assignee()).isEqualTo(env.workerId("a"));
        assertThat(done.outputRef()).isEqualTo("out/" + job.jobId());
        assertThat(done.lastError()).isNull();
        assertThat(done.deadlineAt()).isNull();
    }

    @Test
    @DisplayName("the lowest estimate wins")
    void theLowestEstimateWins() {
        var env = new ManagerEnv(toClose, CONFIG);
        env.startWorker("fast", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startWorker("slow", FakeProcessor.ok(env.kind), Duration.ofSeconds(5));
        env.startManager();
        env.startProducer();
        var job = env.job("two");

        env.producer.submit(job);

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("fast"));
    }

    @Test
    @DisplayName("a worker busy with one job loses the next to the other worker")
    void aBusyWorkerLosesTheNextJob() throws Exception {
        var env = new ManagerEnv(toClose, CONFIG);
        var gate = gate();
        env.startWorker("a", FakeProcessor.blockedOn(env.kind, gate), Duration.ofSeconds(1));
        env.startWorker("b", FakeProcessor.ok(env.kind), Duration.ofMillis(1_500));
        env.startManager();
        env.startProducer();
        var first = env.job("first");
        var second = env.job("second");

        env.producer.submit(first);
        var firstRow = env.awaitJob(first.jobId(), j -> j.assignee() != null, "was assigned");
        assertThat(firstRow.assignee()).isEqualTo(env.workerId("a"));

        // a's queue now holds one job: its estimate is 2 s against b's 1.5 s
        env.producer.submit(second);
        var done = env.awaitJob(second.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("b"));
    }

    // ---------------------------------------------------------------------------------- no worker

    @Test
    @DisplayName("with no worker for the kind the job waits, quietly, and is taken when one arrives")
    void aJobWaitsForAWorker() {
        var env = new ManagerEnv(toClose, CONFIG);
        env.startManager();
        env.startProducer();
        var job = env.job("lonely");

        env.producer.submit(job);
        env.awaitJob(job.jobId(), j -> true, "was recorded");
        pause(4_000);
        var waiting = env.store.find(job.jobId()).orElseThrow();
        assertThat(waiting.state()).isEqualTo(JobState.SUBMITTED);
        assertThat(waiting.attempt()).isZero();
        assertThat(waiting.lastError()).isNull();

        env.startWorker("late", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("late"));
    }

    @Test
    @DisplayName("when every capable worker has already failed the job it is asked of them again")
    void everyoneExcludedIsStillAssigned() {
        var env = new ManagerEnv(toClose, CONFIG);
        var job = env.job("again");
        env.store.submit(job);
        assertThat(env.store.assign(job.jobId(), 0, env.workerId("a"), Instant.now().plusSeconds(60)))
                .isTrue();
        assertThat(env.store.fail(job.jobId(), 1, env.workerId("a"), "seed", true))
                .contains(JobState.SUBMITTED);
        assertThat(env.store.find(job.jobId()).orElseThrow().excluded())
                .containsExactly(env.workerId("a"));

        env.startWorker("a", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.assignee()).isEqualTo(env.workerId("a"));
    }

    // ---------------------------------------------------------------------------------- liveness

    @Test
    @DisplayName("a row that stopped reporting costs one full CFP timeout, then drops out of auctions")
    void aGhostCandidateIsDroppedOnceItGoesStale() {
        var env = new ManagerEnv(toClose, CONFIG);
        env.startWorker("real", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();

        // a worker that is in the directory and nowhere else, as after a kill -9
        var ghostId = env.workerId("ghost");
        env.managerNode.directory().registry().register(AgentDescriptor.builder(ghostId)
                .agentName("ghost").agentType("worker").status(AgentStatus.RUNNING)
                .capabilities(Set.of(env.kind))
                .endpoint(new AgentEndpoint("node-ghost", "redis", Map.of()))
                .build()).join();

        var early = env.job("early");
        var t0 = Instant.now();
        env.producer.submit(early);
        env.awaitJob(early.jobId(), j -> j.state() != JobState.SUBMITTED, "was assigned");
        var withGhost = Duration.between(t0, Instant.now());

        // let the ghost's row age past staleAfter while the real worker keeps beating
        pause(CONFIG.staleAfter().toMillis() + 1_500);

        var late = env.job("late");
        var t1 = Instant.now();
        env.producer.submit(late);
        env.awaitJob(late.jobId(), j -> j.state() != JobState.SUBMITTED, "was assigned");
        var withoutGhost = Duration.between(t1, Instant.now());

        assertThat(withGhost).as("the ghost was a candidate: the auction waited for it")
                .isGreaterThanOrEqualTo(CONFIG.cfpTimeout().minusMillis(500));
        assertThat(withoutGhost).as("the stale row is not asked")
                .isLessThan(CONFIG.cfpTimeout().minusSeconds(1));
    }

    // ---------------------------------------------------------------------------------- outcomes

    @Test
    @DisplayName("a non-retryable FAILURE ends the job in FAILED, without a second attempt")
    void aNonRetryableFailureEndsTheJob() throws Exception {
        var env = new ManagerEnv(toClose, CONFIG);
        var processor = FakeProcessor.failing(env.kind, FakeProcessor.Outcome.NOT_RETRYABLE);
        env.startWorker("a", processor, Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();
        var job = env.job("bad");

        env.producer.submit(job);

        var failed = env.awaitJob(job.jobId(), j -> j.state() == JobState.FAILED, "reached FAILED");
        assertThat(failed.lastError()).contains("wrong for everyone");
        assertThat(failed.attempt()).isEqualTo(1);
        pause(3_000);
        assertThat(processor.executions()).isEqualTo(1);
    }

    @Test
    @DisplayName("a retryable FAILURE puts the job back, and another worker finishes it")
    void aRetryableFailureIsReassigned() {
        var env = new ManagerEnv(toClose, CONFIG);
        env.startWorker("flaky", FakeProcessor.failing(env.kind, FakeProcessor.Outcome.RUNTIME_ERROR),
                Duration.ofSeconds(1));
        env.startWorker("steady", FakeProcessor.ok(env.kind), Duration.ofSeconds(5));
        env.startManager();
        env.startProducer();
        var job = env.job("retry");

        env.producer.submit(job);

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("steady"));
        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.excluded()).containsExactly(env.workerId("flaky"));
    }

    @Test
    @DisplayName("the same job published twice is one row and one execution")
    void aDuplicateSubmissionRunsOnce() {
        var env = new ManagerEnv(toClose, CONFIG);
        var processor = FakeProcessor.ok(env.kind);
        env.startWorker("a", processor, Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();
        var job = env.job("dup");

        env.producer.submit(job);
        env.producer.submit(job);

        env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        pause(3_000);
        assertThat(processor.executions()).isEqualTo(1);
        assertThat(env.store.find(job.jobId()).orElseThrow().attempt()).isEqualTo(1);
    }

    // ---------------------------------------------------------------------------------- D1

    /** A manager whose AGREE never gets out: what a crash between assign and AGREE looks like. */
    private static final class LosesTheAgree extends JobManager {
        LosesTheAgree(String id, JobStore store) {
            super(id, store, CONFIG, Clock.systemUTC());
        }

        @Override
        CompletableFuture<Void> sendAgree(DialogueMessage proposal, JobSpec spec) {
            return CompletableFuture.failedFuture(new IllegalStateException("connection lost"));
        }
    }

    @Test
    @DisplayName("if the AGREE is lost the row is ASSIGNED, with its deadline, and the worker idle")
    void aLostAgreeLeavesTheRowAssigned() {
        var env = new ManagerEnv(toClose, CONFIG);
        var processor = FakeProcessor.ok(env.kind);
        env.startWorker("a", processor, Duration.ofSeconds(1));
        env.startManager(new LosesTheAgree("m-" + env.u, env.store));
        env.startProducer();
        var job = env.job("lost");

        env.producer.submit(job);

        var assigned = env.awaitJob(job.jobId(), j -> j.state() == JobState.ASSIGNED, "was assigned");
        assertThat(assigned.assignee()).isEqualTo(env.workerId("a"));
        assertThat(assigned.attempt()).isEqualTo(1);
        assertThat(assigned.deadlineAt()).isAfter(Instant.now());
        pause(3_000);
        assertThat(processor.executions()).as("the worker was never told").isZero();
        assertThat(env.store.find(job.jobId()).orElseThrow().state()).isEqualTo(JobState.ASSIGNED);
    }
}
