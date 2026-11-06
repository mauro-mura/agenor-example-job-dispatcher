package jobs.worker;

import dev.agenor.core.AgentDescriptor;
import dev.agenor.core.annotations.Agent;
import dev.agenor.core.dialogue.DialogueHandler;
import dev.agenor.core.dialogue.DialogueMessage;
import dev.agenor.core.dialogue.Performative;
import dev.agenor.runtime.agent.BaseAgent;
import dev.agenor.runtime.dialogue.DialogueCapability;
import dev.agenor.core.telemetry.SpanStatus;
import jobs.domain.Bid;
import jobs.domain.JobFailure;
import jobs.domain.JobResult;
import jobs.domain.JobSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Bids on jobs it can do, runs the ones it wins, and reports each outcome as a typed message.
 *
 * <p>The AGREE handler queues the job and returns: a handler that does the work is reclaimed
 * and run again by the transport after {@code pendingEntriesTimeoutMs}, and meanwhile holds one
 * of the mailbox's bounded slots. The work runs on a single execution thread, so a bid's
 * estimate, {@code (in flight + 1) x average duration}, is exact rather than hopeful.
 *
 * <p>What this worker advertises is what it can do: the capabilities of its directory row are the
 * kinds of the processors it was given.
 */
@Agent(type = "worker")
public class JobWorker extends BaseAgent {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private static final int SEEN_CAPACITY = 1000;

    private final String id;
    private final Map<String, JobProcessor> processors;
    private final int capacity;
    private final Duration shutdownGrace;
    private final DurationEstimator estimator;
    private final SeenJobs seen = new SeenJobs(SEEN_CAPACITY);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final DialogueCapability dialogue = new DialogueCapability(this);
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        var t = new Thread(r, "job-worker-execution");
        t.setDaemon(true);
        return t;
    });

    /**
     * @param id            agent id on the directory
     * @param processors    one per kind; their kinds become the advertised capabilities
     * @param capacity      jobs in flight (queued or running) before the worker starts refusing
     * @param initialGuess  duration assumed for a kind that has not been observed yet
     * @param shutdownGrace how long {@code onStop} waits for the running job
     */
    public JobWorker(String id, Collection<JobProcessor> processors, int capacity,
                     Duration initialGuess, Duration shutdownGrace) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1: " + capacity);
        }
        this.id = id;
        this.processors = processors.stream()
                .collect(Collectors.toUnmodifiableMap(JobProcessor::kind, Function.identity()));
        this.capacity = capacity;
        this.shutdownGrace = shutdownGrace;
        this.estimator = new DurationEstimator(initialGuess);
    }

    @Override public String getAgentId() { return id; }
    @Override public String getAgentName() { return "Worker " + id; }

    /**
     * Keeps the descriptor the runtime built, endpoint included, and replaces only its
     * capabilities with the kinds of this worker's processors. A descriptor assembled by hand
     * would drop the endpoint, and the row would be found but not reachable.
     */
    @Override
    public void setAgentDescriptor(AgentDescriptor d) {
        Set<String> kinds = processors.keySet();
        super.setAgentDescriptor(new AgentDescriptor(d.agentId(), d.agentName(), d.agentType(),
                d.status(), kinds, d.metadata(), d.endpoint(), d.registeredAt(), d.lastSeen()));
    }

    /** Jobs queued or running right now. */
    public int inFlight() {
        return inFlight.get();
    }

    @DialogueHandler(performatives = Performative.CFP)
    public void onCfp(DialogueMessage msg) {
        JobSpec spec;
        try {
            spec = msg.contentAs(JobSpec.class);
        } catch (IllegalArgumentException e) {
            spec = null;
        }
        // A CFP we cannot read is not a failure of this worker, and redelivering it would not
        // change the payload: decline it.
        if (spec == null || spec.jobId() == null || spec.kind() == null) {
            dialogue.refuse(msg, "unreadable job");
            return;
        }
        if (!processors.containsKey(spec.kind())) {
            dialogue.refuse(msg, "unsupported kind: " + spec.kind());
            return;
        }
        int ahead = inFlight.get();
        if (ahead >= capacity) {
            dialogue.refuse(msg, "queue full");
            return;
        }
        var eta = estimator.average(spec.kind()).multipliedBy(ahead + 1L);
        dialogue.propose(msg, new Bid(spec.jobId(), spec.attempt(), eta, ahead));
    }

    @DialogueHandler(performatives = Performative.AGREE)
    public void onAgree(DialogueMessage msg) {
        // Unreadable here is a defect, not a refusal: let it throw, so the transport redelivers
        // and then dead-letters it with this exception as the reason.
        var spec = msg.contentAs(JobSpec.class);

        if (!seen.firstTime(spec.jobId() + "#" + spec.attempt())) {
            log.debug("duplicate AGREE for {} attempt {} ignored", spec.jobId(), spec.attempt());
            return;
        }
        var processor = processors.get(spec.kind());
        if (processor == null) {
            report(msg, spec, "unsupported kind: " + spec.kind(), true);
            return;
        }
        if (!admit()) {
            // Concurrent auctions all saw the same queue; the one that arrives last finds it full.
            report(msg, spec, "queue full", true);
            return;
        }
        executor.execute(() -> run(msg, spec, processor));
    }

    /** The only place {@code inFlight} grows, so checking and counting under one lock suffice. */
    private synchronized boolean admit() {
        if (inFlight.get() >= capacity) {
            return false;
        }
        inFlight.incrementAndGet();
        return true;
    }

    private void run(DialogueMessage agree, JobSpec spec, JobProcessor processor) {
        var span = getTelemetry().spanBuilder("job.execute")
                .setAttribute("job.id", spec.jobId())
                .setAttribute("job.kind", spec.kind())
                .setAttribute("job.attempt", (long) spec.attempt())
                .setAttribute("conversation.id", agree.conversationId())
                .startSpan();
        var started = System.nanoTime();
        try {
            var outputRef = processor.process(spec);
            estimator.record(spec.kind(), Duration.ofNanos(System.nanoTime() - started));
            span.setAttribute("job.outcome", "done").setStatus(SpanStatus.OK);
            send(dialogue.inform(agree, new JobResult(spec.jobId(), spec.attempt(), outputRef)),
                    spec, "INFORM");
        } catch (JobProcessingException e) {
            span.setAttribute("job.outcome", e.isRetryable() ? "failed_retryable" : "failed")
                    .recordException(e).setStatus(SpanStatus.ERROR);
            report(agree, spec, e.getMessage(), e.isRetryable());
        } catch (RuntimeException e) {
            // Not the processor's own verdict: another worker may well succeed, and the
            // manager's attempt limit bounds the cost of being wrong.
            span.setAttribute("job.outcome", "failed_retryable").recordException(e)
                    .setStatus(SpanStatus.ERROR);
            report(agree, spec, e.getClass().getSimpleName() + ": " + e.getMessage(), true);
        } finally {
            inFlight.decrementAndGet();
            span.end();
        }
    }

    private void report(DialogueMessage agree, JobSpec spec, String reason, boolean retryable) {
        var failure = new JobFailure(spec.jobId(), spec.attempt(), reason, retryable);
        send(dialogue.reply(agree, Performative.FAILURE, failure), spec, "FAILURE");
    }

    private void send(java.util.concurrent.CompletableFuture<Void> sent, JobSpec spec, String what) {
        sent.whenComplete((v, e) -> {
            if (e != null) {
                log.warn("could not send {} for {} attempt {}: {}", what, spec.jobId(),
                        spec.attempt(), e.getMessage());
            }
        });
    }

    @Override
    protected void onStop() {
        super.onStop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
