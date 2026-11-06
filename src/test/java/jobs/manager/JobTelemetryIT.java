package jobs.manager;

import jobs.domain.JobState;
import jobs.worker.FakeProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static jobs.manager.ManagerEnv.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The spans of the domain, read back from a recording telemetry given to every node. The
 * test JVM holds the manager and the workers, so one recorder sees them all.
 */
@DisplayName("Spans of the job domain")
class JobTelemetryIT {

    private static final ManagerConfig CONFIG = new ManagerConfig(
            Duration.ofSeconds(3), Duration.ofSeconds(2), Duration.ofSeconds(3), 10);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    private ManagerEnv env(RecordingTelemetry telemetry) {
        var env = new ManagerEnv(toClose, CONFIG);
        env.telemetry = telemetry;
        return env;
    }

    private static void await(String what, BooleanSupplier condition) {
        var deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            pause(100);
        }
        throw new AssertionError("timed out waiting for: " + what);
    }

    @Test
    @DisplayName("one job: an auction that assigned it and an execution that did it, joined by conversation.id")
    void aJobLeavesAnAuctionAndAnExecution() {
        var recorder = new RecordingTelemetry();
        var env = env(recorder);
        env.startWorker("a", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();
        env.startProducer();
        var job = env.job("traced");

        env.producer.submit(job);
        env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        await("the execution span ended", () ->
                recorder.named("job.execute", job.jobId()).stream().anyMatch(s -> s.ended()));

        var assigned = recorder.named("job.auction", job.jobId()).stream()
                .filter(s -> "assigned".equals(s.attr("job.outcome"))).toList();
        assertThat(assigned).hasSize(1);
        var auction = assigned.get(0);
        assertThat(auction.attr("job.kind")).isEqualTo(env.kind);
        assertThat(auction.attr("job.attempt")).isEqualTo("1");
        assertThat(auction.attr("job.winner")).isEqualTo(env.workerId("a"));
        assertThat(auction.attr("job.candidates")).isEqualTo("1");
        assertThat(auction.attr("job.proposals")).isEqualTo("1");
        assertThat(auction.attr("job.winner.eta_ms")).isEqualTo("1000");
        assertThat(auction.ended()).isTrue();

        var execution = recorder.named("job.execute", job.jobId()).get(0);
        assertThat(execution.attr("job.outcome")).isEqualTo("done");
        assertThat(execution.attr("job.attempt")).isEqualTo("1");
        assertThat(execution.attr("job.kind")).isEqualTo(env.kind);
        assertThat(execution.attr("conversation.id")).isNotBlank()
                .isEqualTo(auction.attr("conversation.id"));
    }

    @Test
    @DisplayName("with no worker the auction says so, and nothing is executed")
    void noCandidates() {
        var recorder = new RecordingTelemetry();
        var env = env(recorder);
        env.startManager();
        env.startProducer();
        var job = env.job("lonely");

        env.producer.submit(job);
        await("an auction without candidates", () ->
                recorder.named("job.auction", job.jobId()).stream()
                        .anyMatch(s -> s.ended() && "no_candidates".equals(s.attr("job.outcome"))));

        var auctions = recorder.named("job.auction", job.jobId());
        assertThat(auctions).allSatisfy(s -> {
            assertThat(s.attr("job.outcome")).isIn("no_candidates", null);
            assertThat(s.attr("job.candidates")).isEqualTo("0");
        });
        assertThat(recorder.named("job.execute", job.jobId())).isEmpty();
    }

    @Test
    @DisplayName("a retryable failure and its second auction are told apart by attempt")
    void aRetryShowsAsTwoAttempts() {
        var recorder = new RecordingTelemetry();
        var env = env(recorder);
        env.startWorker("flaky", FakeProcessor.failing(env.kind, FakeProcessor.Outcome.RUNTIME_ERROR),
                Duration.ofSeconds(1));
        env.startWorker("steady", FakeProcessor.ok(env.kind), Duration.ofSeconds(5));
        env.startManager();
        env.startProducer();
        var job = env.job("retry");

        env.producer.submit(job);
        env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");
        await("both executions ended", () -> recorder.named("job.execute", job.jobId()).stream()
                .filter(s -> s.ended()).count() == 2);

        var outcomes = recorder.named("job.execute", job.jobId()).stream()
                .collect(Collectors.toMap(s -> s.attr("job.attempt"), s -> s.attr("job.outcome")));
        assertThat(outcomes).containsEntry("1", "failed_retryable").containsEntry("2", "done");

        var assignedAttempts = recorder.named("job.auction", job.jobId()).stream()
                .filter(s -> "assigned".equals(s.attr("job.outcome")))
                .map(s -> s.attr("job.attempt")).collect(Collectors.toSet());
        assertThat(assignedAttempts).isEqualTo(Set.of("1", "2"));
    }

    @Test
    @DisplayName("an expired assignment leaves a job.expire with the worker, the attempt and the new state")
    void anExpiryIsASpan() {
        var recorder = new RecordingTelemetry();
        var env = env(recorder);
        var job = env.job("left");
        env.store.submit(job);
        assertThat(env.store.assign(job.jobId(), 0, env.workerId("gone"),
                Instant.now().minusSeconds(30))).isTrue();
        env.startWorker("b", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();

        env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");

        var expired = recorder.named("job.expire", job.jobId());
        assertThat(expired).hasSize(1);
        assertThat(expired.get(0).attr("job.assignee")).isEqualTo(env.workerId("gone"));
        assertThat(expired.get(0).attr("job.attempt")).isEqualTo("1");
        assertThat(expired.get(0).attr("job.state")).isEqualTo(JobState.SUBMITTED.name());
        assertThat(expired.get(0).ended()).isTrue();
    }

    @Test
    @DisplayName("idle: how many spans a manager and a worker emit with nothing to do")
    void whatIdleCosts() {
        var recorder = new RecordingTelemetry();
        var env = env(recorder);
        env.heartbeat = Duration.ofSeconds(10);
        env.startWorker("idle", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.startManager();
        pause(3_000);

        int before = recorder.spans().size();
        var window = Duration.ofSeconds(30);
        pause(window.toMillis());
        var spans = recorder.spans().subList(before, recorder.spans().size());

        var byName = new TreeMap<String, Long>();
        spans.forEach(s -> byName.merge(s.name(), 1L, Long::sum));
        var perDay = spans.size() * (86_400L / window.toSeconds());
        assertThat(recorder.named("job.auction", null).stream().filter(s -> s.attr("job.id") != null))
                .as("nothing to auction while idle").isEmpty();
    }
}
