package jobs.worker;

import jobs.domain.JobSpec;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** A processor whose duration, outcome and blocking the test controls, and which counts runs. */
public final class FakeProcessor implements JobProcessor {

    public enum Outcome { OK, NOT_RETRYABLE, RUNTIME_ERROR }

    private final String kind;
    private final Outcome outcome;
    private final CountDownLatch gate;
    private final Duration work;
    private final Consumer<JobSpec> onStart;
    private final AtomicInteger executions = new AtomicInteger();

    FakeProcessor(String kind, Outcome outcome, CountDownLatch gate, Duration work) {
        this(kind, outcome, gate, work, job -> { });
    }

    /** {@code onStart} runs when a job begins, before any waiting: a test's "it is running now". */
    public FakeProcessor(String kind, Outcome outcome, CountDownLatch gate, Duration work,
                         Consumer<JobSpec> onStart) {
        this.kind = kind;
        this.outcome = outcome;
        this.gate = gate;
        this.work = work;
        this.onStart = onStart;
    }

    /** Succeeds, after {@code work}. */
    public static FakeProcessor slow(String kind, Duration work) {
        return new FakeProcessor(kind, Outcome.OK, null, work);
    }

    public static FakeProcessor ok(String kind) {
        return new FakeProcessor(kind, Outcome.OK, null, Duration.ZERO);
    }

    public static FakeProcessor blockedOn(String kind, CountDownLatch gate) {
        return new FakeProcessor(kind, Outcome.OK, gate, Duration.ZERO);
    }

    public static FakeProcessor failing(String kind, Outcome outcome) {
        return new FakeProcessor(kind, outcome, null, Duration.ZERO);
    }

    public int executions() {
        return executions.get();
    }

    @Override
    public String kind() {
        return kind;
    }

    @Override
    public String process(JobSpec job) throws JobProcessingException {
        executions.incrementAndGet();
        onStart.accept(job);
        try {
            if (gate != null) {
                gate.await();
            }
            if (!work.isZero()) {
                Thread.sleep(work.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JobProcessingException("interrupted", true, e);
        }
        return switch (outcome) {
            case OK -> "out/" + job.jobId();
            case NOT_RETRYABLE -> throw new JobProcessingException("input is wrong for everyone", false);
            case RUNTIME_ERROR -> throw new IllegalStateException("processor bug");
        };
    }
}
