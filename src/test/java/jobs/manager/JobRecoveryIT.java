package jobs.manager;

import dev.agenor.core.AgentDescriptor;
import dev.agenor.core.AgentEndpoint;
import dev.agenor.core.AgentStatus;
import jobs.domain.JobState;
import jobs.testsupport.ChildNode;
import jobs.testsupport.Infra;
import jobs.worker.FakeProcessor;
import jobs.worker.WorkerMain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static jobs.manager.ManagerEnv.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What happens to a job whose worker does not answer. With the estimate at 1 s and the
 * margin at 2 s, an assignment's deadline is about 4 s after it is made.
 *
 * <p>A worker that has to die is a child JVM killed with SIGKILL: a handler cannot
 * be killed from inside the JVM that runs the test.
 */
@DisplayName("Recovery of jobs whose worker does not answer")
class JobRecoveryIT {

    private static final ManagerConfig CONFIG = new ManagerConfig(
            Duration.ofSeconds(3), Duration.ofSeconds(2), Duration.ofSeconds(3), 10);

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    // ---------------------------------------------------------------------------------- 1

    @Test
    @DisplayName("1. a worker killed mid-job: another one finishes it, once, after the deadline")
    void aKilledWorkersJobIsFinishedByAnother() throws Exception {
        var env = new ManagerEnv(toClose, CONFIG);
        var childEnv = Infra.env("worker", env.prefix, "node-a", env.workerId("a"));
        childEnv.put("KIND", env.kind);
        childEnv.put("HOLD_MS", "60000");
        childEnv.put("INITIAL_GUESS_MS", "1000");
        var a = ChildNode.start("worker-a", WorkerMain.class, childEnv);
        toClose.add(a);
        a.awaitCheckpoint("started", Duration.ofSeconds(60));

        var processorB = FakeProcessor.ok(env.kind);
        env.startWorker("b", processorB, Duration.ofSeconds(5));
        env.startManager();
        env.startProducer();
        var job = env.job("killed");

        env.producer.submit(job);
        a.awaitCheckpoint("running " + job.jobId(), Duration.ofSeconds(30));
        var killedAt = Instant.now();
        a.kill();

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        var recovery = Duration.between(killedAt, Instant.now());

        pause(2_000);
        assertThat(done.assignee()).isEqualTo(env.workerId("b"));
        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.excluded()).containsExactly(env.workerId("a"));
        assertThat(processorB.executions()).as("done once").isEqualTo(1);
        assertThat(recovery).isLessThan(Duration.ofSeconds(9));
    }

    // ---------------------------------------------------------------------------------- 2

    @Test
    @DisplayName("2. a worker that is slow, not dead, loses the job and its late result changes nothing")
    void aSlowWorkersLateResultIsDiscarded() {
        var env = new ManagerEnv(toClose, CONFIG);
        var slow = FakeProcessor.slow(env.kind, Duration.ofSeconds(8));
        var fast = FakeProcessor.ok(env.kind);
        env.startWorker("slow", slow, Duration.ofSeconds(1));
        env.startWorker("fast", fast, Duration.ofSeconds(5));
        env.startManager();
        env.startProducer();
        var job = env.job("slow");

        env.producer.submit(job);

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("fast"));
        assertThat(done.attempt()).isEqualTo(2);

        // the slow worker is still running attempt 1 and will report it
        pause(10_000);
        var after = env.store.find(job.jobId()).orElseThrow();
        assertThat(slow.executions()).isEqualTo(1);
        assertThat(after.state()).isEqualTo(JobState.DONE);
        assertThat(after.assignee()).isEqualTo(env.workerId("fast"));
        assertThat(after.attempt()).isEqualTo(2);
        assertThat(after.updatedAt()).as("the late INFORM did not touch the row")
                .isEqualTo(done.updatedAt());
    }

    // ---------------------------------------------------------------------------------- 3

    @Test
    @DisplayName("3. a worker that never finishes: three deadlines, then FAILED with the reason")
    void aJobThatNeverFinishesEndsFailed() {
        var env = new ManagerEnv(toClose, CONFIG);
        var gate = new CountDownLatch(1);
        gates.add(gate);
        env.startWorker("stuck", FakeProcessor.blockedOn(env.kind, gate), Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();
        var job = env.job("stuck");

        env.producer.submit(job);

        var failed = env.awaitJob(job.jobId(), j -> j.state() == JobState.FAILED, "reached FAILED",
                Duration.ofSeconds(60));
        assertThat(failed.attempt()).isEqualTo(3);
        assertThat(failed.lastError()).isEqualTo(JobManager.DEADLINE_EXCEEDED);
        assertThat(failed.excluded()).containsExactly(env.workerId("stuck"));
    }

    // ---------------------------------------------------------------------------------- 4

    @Test
    @DisplayName("4. a manager that starts with an expired assignment on the table recovers it")
    void aRestartedManagerRecoversWhatExpired() {
        var env = new ManagerEnv(toClose, CONFIG);
        var job = env.job("left");
        env.store.submit(job);
        assertThat(env.store.assign(job.jobId(), 0, env.workerId("gone"),
                Instant.now().minusSeconds(30))).isTrue();

        env.startWorker("b", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();

        var done = env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        assertThat(done.assignee()).isEqualTo(env.workerId("b"));
        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.excluded()).containsExactly(env.workerId("gone"));
    }

    // ---------------------------------------------------------------------------------- 5

    @Test
    @DisplayName("5. an assignment that is not due is left alone, tick after tick")
    void aJobNotDueIsNotTouched() {
        var env = new ManagerEnv(toClose, CONFIG);
        var job = env.job("patient");
        env.store.submit(job);
        assertThat(env.store.assign(job.jobId(), 0, env.workerId("quiet"),
                Instant.now().plusSeconds(120))).isTrue();
        var before = env.store.find(job.jobId()).orElseThrow();

        env.startManager();
        pause(5_000);

        var after = env.store.find(job.jobId()).orElseThrow();
        assertThat(after.state()).isEqualTo(JobState.ASSIGNED);
        assertThat(after.attempt()).isEqualTo(1);
        assertThat(after.updatedAt()).isEqualTo(before.updatedAt());
    }

    // ---------------------------------------------------------------------------------- 6

    @Test
    @DisplayName("6. recovery does not wait behind an auction that is waiting for a silent candidate")
    void theSweepDoesNotWaitForAuctions() {
        // staleAfter long enough that the ghost stays a candidate for the whole test
        var config = new ManagerConfig(Duration.ofSeconds(3), Duration.ofSeconds(2),
                Duration.ofSeconds(60), 10);
        var env = new ManagerEnv(toClose, config);
        env.startManager();
        // the sweep's first tick waits five seconds after a start (so results left in the stream
        // are applied before anything is called overdue); this test is about the sweep not waiting
        // for auctions, not about starting up
        pause(6_000);

        // three jobs for a kind whose only worker is a row nobody serves: each auction waits the
        // whole CFP timeout, so the round is busy for about nine seconds
        env.managerNode.directory().registry().register(AgentDescriptor.builder(env.workerId("ghost"))
                .agentName("ghost").agentType("worker").status(AgentStatus.RUNNING)
                .capabilities(Set.of(env.kind))
                .endpoint(new AgentEndpoint("node-ghost", "redis", Map.of()))
                .build()).join();
        for (var n : List.of("x1", "x2", "x3")) {
            env.store.submit(env.job(n));
        }

        // an assignment of another kind that expires a little later, while the round is busy
        var expiring = new jobs.domain.JobSpec("job-exp-" + env.u, 0, "other-" + env.u, "in/e", 1);
        env.store.submit(expiring);
        var deadline = Instant.now().plusSeconds(2);
        assertThat(env.store.assign(expiring.jobId(), 0, env.workerId("gone"), deadline)).isTrue();

        var recovered = env.awaitJob(expiring.jobId(),
                j -> JobManager.DEADLINE_EXCEEDED.equals(j.lastError()), "was given back",
                Duration.ofSeconds(8));
        var lateBy = Duration.between(deadline, Instant.now());

        var stillWaiting = env.store.awaitingAssignment(10).stream()
                .filter(j -> j.kind().equals(env.kind)).toList();
        assertThat(recovered.state()).isEqualTo(JobState.SUBMITTED);
        assertThat(lateBy).isLessThan(Duration.ofSeconds(3));
        assertThat(stillWaiting).as("the auction round was still busy").isNotEmpty();
    }
}
