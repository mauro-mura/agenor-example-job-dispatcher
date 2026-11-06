package jobs.worker;

import dev.agenor.core.dialogue.DialogueMessage;
import dev.agenor.core.dialogue.Performative;
import jobs.domain.Bid;
import jobs.domain.JobFailure;
import jobs.domain.JobResult;
import jobs.domain.JobSpec;
import jobs.testsupport.Infra;
import jobs.service.ServiceNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The worker over the real transport. Worker and manager are on different nodes of one
 * JVM, sharing Valkey and PostgreSQL, so every message is serialised: a test on one runtime would
 * take the local path and prove nothing about the wire.
 */
@DisplayName("JobWorker over Valkey + PostgreSQL")
class JobWorkerIT {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir Path storage;

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    private record Rig(String prefix, String workerId, JobWorker worker, TestManager manager,
                       ServiceNode workerNode, ServiceNode managerNode) {

        DialogueMessage propose(JobSpec spec) {
            var responses = manager.cfp(workerId, spec).join();
            assertThat(responses).hasSize(1);
            return responses.get(0);
        }
    }

    private CountDownLatch gate() {
        var gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    private Rig rig(int capacity, JobProcessor... processors) {
        var u = Infra.unique();
        var prefix = "wrk-" + u;
        var workerId = "w-" + u;
        var worker = new JobWorker(workerId, List.of(processors), capacity,
                Duration.ofSeconds(2), Duration.ofSeconds(2));
        var manager = new TestManager("m-" + u);

        var workerNode = ServiceNode.start("node-w", prefix, Infra.redisUri(), Infra.jdbcUrl(),
                Infra.JDBC_USER, Infra.JDBC_PASSWORD, 2_000,
                new ServiceNode.Options(null, null, false), worker);
        toClose.add(workerNode);
        var managerNode = ServiceNode.start("node-m", prefix, Infra.redisUri(), Infra.jdbcUrl(),
                Infra.JDBC_USER, Infra.JDBC_PASSWORD, 2_000,
                new ServiceNode.Options(null, null, false), manager);
        toClose.add(managerNode);

        await("worker resolvable from the manager's node", () ->
                managerNode.runtime().getAgentDirectory().resolveEndpoint(workerId).join()
                        .map(e -> !e.nodeId().isBlank()).orElse(false));
        return new Rig(prefix, workerId, worker, manager, workerNode, managerNode);
    }

    private static JobSpec spec(String kind, String suffix) {
        return new JobSpec("job-" + suffix + "-" + Infra.unique(), 1, kind, "in/" + suffix, 100);
    }

    private static void await(String what, BooleanSupplier condition) {
        var deadline = Instant.now().plus(WAIT);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("timed out waiting for: " + what);
    }

    private static DialogueMessage take(java.util.concurrent.BlockingQueue<DialogueMessage> queue)
            throws InterruptedException {
        var msg = queue.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertThat(msg).as("message from the worker").isNotNull();
        return msg;
    }

    // ---------------------------------------------------------------------------------- CFP

    @Test
    @DisplayName("a CFP for a supported kind gets a readable PROPOSE")
    void proposesForASupportedKind() {
        var kind = "ocr-" + Infra.unique();
        var rig = rig(5, FakeProcessor.ok(kind));

        var proposal = rig.propose(spec(kind, "a"));

        assertThat(proposal.performative()).isEqualTo(Performative.PROPOSE);
        var bid = proposal.contentAs(Bid.class);
        assertThat(bid.queueLength()).isZero();
        assertThat(bid.eta()).isEqualTo(Duration.ofSeconds(2));
        assertThat(bid.attempt()).isEqualTo(1);
    }

    @Test
    @DisplayName("a CFP for a kind it does not run is refused")
    void refusesAnUnsupportedKind() {
        var rig = rig(5, FakeProcessor.ok("ocr-" + Infra.unique()));

        var response = rig.propose(spec("thumbnail", "a"));

        assertThat(response.performative()).isEqualTo(Performative.REFUSE);
        assertThat(String.valueOf(response.content())).contains("unsupported kind");
    }

    @Test
    @DisplayName("an unreadable CFP is refused and acknowledged: nothing to redeliver")
    void refusesAnUnreadableCfpWithoutRedelivery() {
        var rig = rig(5, FakeProcessor.ok("ocr-" + Infra.unique()));

        var responses = rig.manager.cfp(rig.workerId, Map.of("attempt", "not-a-number")).join();

        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).performative()).isEqualTo(Performative.REFUSE);
        assertThat(String.valueOf(responses.get(0).content())).contains("unreadable");
        await("worker stream fully acknowledged", () ->
                Infra.pendingEntries(rig.prefix, "node-w") == 0);
    }

    @Test
    @DisplayName("the bid grows with the queue: queueLength and eta of later offers are larger")
    void theBidReflectsTheQueue() {
        var kind = "ocr-" + Infra.unique();
        var gate = gate();
        var rig = rig(5, FakeProcessor.blockedOn(kind, gate));

        var first = rig.propose(spec(kind, "p0")).contentAs(Bid.class);
        var a = spec(kind, "a");
        rig.manager.agree(rig.propose(a), a).join();
        await("one job in flight", () -> rig.worker.inFlight() == 1);

        var second = rig.propose(spec(kind, "p1")).contentAs(Bid.class);
        assertThat(second.queueLength()).isEqualTo(1);
        assertThat(second.eta()).isGreaterThan(first.eta());

        var b = spec(kind, "b");
        rig.manager.agree(rig.propose(b), b).join();
        await("two jobs in flight", () -> rig.worker.inFlight() == 2);

        var third = rig.propose(spec(kind, "p2")).contentAs(Bid.class);
        assertThat(third.queueLength()).isEqualTo(2);
        assertThat(third.eta()).isGreaterThan(second.eta());
    }

    // ---------------------------------------------------------------------------------- AGREE

    @Test
    @DisplayName("AGREE for a real PDF: INFORM with the output reference, and the file is there")
    void convertsARealPdf() throws Exception {
        jobs.worker.PdfToTextProcessorTest.writePdf(storage.resolve("in/real.pdf"),
                "Agenor over Valkey");
        var rig = rig(5, new PdfToTextProcessor(storage));
        var job = new JobSpec("job-real-" + Infra.unique(), 1, PdfToTextProcessor.KIND,
                "in/real.pdf", 1);

        var proposal = rig.propose(job);
        assertThat(proposal.performative()).isEqualTo(Performative.PROPOSE);
        rig.manager.agree(proposal, job).join();

        var result = take(rig.manager.informs).contentAs(JobResult.class);
        assertThat(result.jobId()).isEqualTo(job.jobId());
        assertThat(result.attempt()).isEqualTo(1);
        assertThat(result.outputRef()).isEqualTo("out/" + job.jobId() + ".txt");
        assertThat(Files.readString(storage.resolve(result.outputRef())))
                .contains("Agenor over Valkey");
        await("worker idle again", () -> rig.worker.inFlight() == 0);
    }

    @Test
    @DisplayName("an AGREE delivered twice runs the job once")
    void aDuplicateAgreeRunsOnce() throws Exception {
        var kind = "ocr-" + Infra.unique();
        var processor = FakeProcessor.ok(kind);
        var rig = rig(5, processor);
        var job = spec(kind, "dup");

        var proposal = rig.propose(job);
        rig.manager.agree(proposal, job).join();
        try {
            rig.manager.agree(proposal, job).join();
        } catch (RuntimeException secondReplyRefusedLocally) {
            // the sender's protocol state does not allow a second AGREE in that conversation:
            // reach the same (jobId, attempt) through another conversation instead
            var again = rig.propose(job);
            rig.manager.agree(again, job).join();
        }

        take(rig.manager.informs);
        assertThat(rig.manager.informs.poll(3, TimeUnit.SECONDS))
                .as("a second INFORM would mean the job ran twice").isNull();
        assertThat(processor.executions()).isEqualTo(1);
    }

    @Test
    @DisplayName("a processor that says 'do not retry' produces a typed, non-retryable FAILURE")
    void reportsANonRetryableFailure() throws Exception {
        var kind = "ocr-" + Infra.unique();
        var rig = rig(5, FakeProcessor.failing(kind, FakeProcessor.Outcome.NOT_RETRYABLE));
        var job = spec(kind, "bad");

        rig.manager.agree(rig.propose(job), job).join();

        var failure = take(rig.manager.failures).contentAs(JobFailure.class);
        assertThat(failure.jobId()).isEqualTo(job.jobId());
        assertThat(failure.attempt()).isEqualTo(1);
        assertThat(failure.retryable()).isFalse();
        assertThat(failure.reason()).contains("wrong for everyone");
        await("worker idle again", () -> rig.worker.inFlight() == 0);
    }

    @Test
    @DisplayName("an unexpected exception in the processor is reported as retryable")
    void reportsAnUnexpectedErrorAsRetryable() throws Exception {
        var kind = "ocr-" + Infra.unique();
        var rig = rig(5, FakeProcessor.failing(kind, FakeProcessor.Outcome.RUNTIME_ERROR));
        var job = spec(kind, "bug");

        rig.manager.agree(rig.propose(job), job).join();

        var failure = take(rig.manager.failures).contentAs(JobFailure.class);
        assertThat(failure.retryable()).isTrue();
        assertThat(failure.reason()).contains("IllegalStateException").contains("processor bug");
        await("worker idle again", () -> rig.worker.inFlight() == 0);
    }

    @Test
    @DisplayName("a full queue at AGREE time is a retryable FAILURE, not a handler error")
    void aFullQueueAnswersWithFailure() throws Exception {
        var kind = "ocr-" + Infra.unique();
        var gate = gate();
        var processor = FakeProcessor.blockedOn(kind, gate);
        var rig = rig(2, processor);

        // three auctions seen with an empty queue: this is the over-commitment of Contract Net
        var jobs = List.of(spec(kind, "1"), spec(kind, "2"), spec(kind, "3"));
        var proposals = jobs.stream().map(rig::propose).toList();
        proposals.forEach(p -> assertThat(p.performative()).isEqualTo(Performative.PROPOSE));

        rig.manager.agree(proposals.get(0), jobs.get(0)).join();
        rig.manager.agree(proposals.get(1), jobs.get(1)).join();
        await("capacity reached", () -> rig.worker.inFlight() == 2);
        rig.manager.agree(proposals.get(2), jobs.get(2)).join();

        var failure = take(rig.manager.failures).contentAs(JobFailure.class);
        assertThat(failure.jobId()).isEqualTo(jobs.get(2).jobId());
        assertThat(failure.retryable()).isTrue();
        assertThat(failure.reason()).isEqualTo("queue full");

        gate.countDown();
        take(rig.manager.informs);
        take(rig.manager.informs);
        assertThat(processor.executions()).as("the third job never ran").isEqualTo(2);
        assertThat(Infra.pendingEntries(rig.prefix, "node-w"))
                .as("nothing left for redelivery: the handler did not throw").isZero();
    }

    @Test
    @DisplayName("an AGREE the handler cannot read is redelivered and dead-lettered with its exception")
    void aHandlerThatThrowsEndsInTheDeadLetterQueue() throws Exception {
        var kind = "ocr-" + Infra.unique();
        var rig = rig(5, FakeProcessor.ok(kind));

        var proposal = rig.propose(spec(kind, "poison"));
        rig.manager.agree(proposal, Map.of("attempt", "not-a-number")).join();

        var dlq = rig.workerNode.factory().deadLetterQueue();
        var deadline = Instant.now().plusSeconds(60);
        dev.agenor.core.deadletter.DeadLetter found = null;
        while (found == null && Instant.now().isBefore(deadline)) {
            found = dlq.recent(100).stream()
                    .filter(d -> rig.workerId.equals(d.recipientId()))
                    .findFirst().orElse(null);
            Thread.sleep(250);
        }

        assertThat(found).as("a dead letter for the worker").isNotNull();
        assertThat(found.attempts()).isGreaterThanOrEqualTo(3);
        assertThat(found.reason()).isNotBlank();
        assertThat(rig.worker.inFlight()).as("the job never started").isZero();
    }

    // ---------------------------------------------------------------------------------- directory

    @Test
    @DisplayName("the worker is found by the kinds of its processors, and only by those")
    void advertisesTheKindsItRuns() {
        var u = Infra.unique();
        var ocr = "ocr-" + u;
        var pdf = "pdf-" + u;
        var rig = rig(5, FakeProcessor.ok(ocr), FakeProcessor.ok(pdf));
        var discovery = rig.managerNode.directory().discovery();

        await("found by ocr", () -> discovery.findByCapability(ocr).join().stream()
                .anyMatch(d -> d.agentId().equals(rig.workerId)));
        assertThat(discovery.findByCapability(pdf).join())
                .extracting(d -> d.agentId()).contains(rig.workerId);
        assertThat(discovery.findByCapability("thumbnail-" + u).join()).isEmpty();

        var endpoint = rig.managerNode.runtime().getAgentDirectory()
                .resolveEndpoint(rig.workerId).join();
        assertThat(endpoint).isPresent();
        assertThat(endpoint.get().nodeId()).isEqualTo("node-w");
    }
}
