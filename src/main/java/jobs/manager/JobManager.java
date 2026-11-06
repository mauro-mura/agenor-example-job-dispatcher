package jobs.manager;

import dev.agenor.core.Message;
import dev.agenor.core.annotations.Agent;
import dev.agenor.core.annotations.AgenorMessageHandler;
import dev.agenor.core.annotations.Behavior;
import dev.agenor.core.dialogue.DialogueHandler;
import dev.agenor.core.dialogue.DialogueMessage;
import dev.agenor.core.dialogue.Performative;
import dev.agenor.core.telemetry.Span;
import dev.agenor.core.telemetry.SpanStatus;
import dev.agenor.runtime.agent.BaseAgent;
import dev.agenor.runtime.dialogue.DialogueCapability;
import jobs.domain.Bid;
import jobs.domain.Job;
import jobs.domain.JobFailure;
import jobs.domain.JobResult;
import jobs.domain.JobSpec;
import jobs.manager.AuctionRules.Offer;
import jobs.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static dev.agenor.core.BehaviorType.CYCLIC;

/**
 * Takes jobs from {@code jobs.submitted}, auctions them to the workers, and records the outcome.
 * The table is the source of truth: nothing here survives a restart, and nothing needs to.
 *
 * <p>Two rules matter more than the rest:
 * <ul>
 *   <li><b>assign before AGREE.</b> If the manager dies between the two, the row is
 *       {@code ASSIGNED} with a deadline and the worker never heard: that costs the wait for the
 *       deadline. The other order leaves a worker doing a job whose row is still
 *       {@code SUBMITTED}, and its result is then discarded.
 *   <li><b>one auction at a time.</b> The next CFP is sent after the previous AGREE, and a
 *       worker's node reads one stream in order, so the queue a CFP sees already counts the
 *       previous award. No two auctions can take the same job either.
 * </ul>
 */
@Agent(type = "manager")
public class JobManager extends BaseAgent {

    public static final String SUBMITTED_TOPIC = "jobs.submitted";

    /** The reason recorded on a job whose assignment ran past its deadline. */
    public static final String DEADLINE_EXCEEDED = "deadline exceeded";

    private static final Logger log = LoggerFactory.getLogger(JobManager.class);

    private final String id;
    private final JobStore store;
    private final ManagerConfig config;
    private final Clock clock;
    private final DialogueCapability dialogue = new DialogueCapability(this);
    private final AtomicBoolean roundRunning = new AtomicBoolean();

    public JobManager(String id, JobStore store, ManagerConfig config, Clock clock) {
        this.id = id;
        this.store = store;
        this.config = config;
        this.clock = clock;
    }

    @Override public String getAgentId() { return id; }
    @Override public String getAgentName() { return "Job manager " + id; }

    // ---------------------------------------------------------------------------------- intake

    /**
     * Returns the failure instead of throwing it, so that the dead letter's reason, shown by the
     * console under {@code /api/deadletters}, carries the actual cause of a refused submission.
     */
    @AgenorMessageHandler(SUBMITTED_TOPIC)
    public CompletableFuture<Void> onSubmitted(Message message) {
        try {
            var spec = message.getContent(JobSpec.class);
            if (store.submit(spec)) {
                log.info("job {} ({}) submitted", spec.jobId(), spec.kind());
            } else {
                log.debug("job {} was already known", spec.jobId());
            }
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // ---------------------------------------------------------------------------------- auctions

    /**
     * One round: the oldest waiting jobs, one auction at a time. Returns at once, because a tick
     * that waits holds a thread of the scheduler's pool; the next tick finds the flag set.
     */
    @Behavior(type = CYCLIC, interval = "1s", autoStart = true)
    public void auctionRound() {
        if (!roundRunning.compareAndSet(false, true)) {
            return;
        }
        List<Job> waiting;
        try {
            waiting = store.awaitingAssignment(config.batchSize());
        } catch (RuntimeException e) {
            roundRunning.set(false);
            throw e;
        }
        if (waiting.isEmpty()) {
            roundRunning.set(false);
            return;
        }
        inSequence(waiting.iterator()).whenComplete((v, e) -> roundRunning.set(false));
    }

    private CompletableFuture<Void> inSequence(Iterator<Job> jobs) {
        if (!jobs.hasNext()) {
            return CompletableFuture.completedFuture(null);
        }
        var job = jobs.next();
        return auction(job)
                .exceptionally(e -> {
                    log.warn("auction for {} failed: {}", job.jobId(), e.getMessage());
                    return null;
                })
                .thenCompose(v -> inSequence(jobs));
    }

    /**
     * One auction, as one span. {@code job.attempt} is the attempt this auction would assign, so a
     * second auction of the same job reads 2. Searching a trace backend for {@code job.id} finds
     * this span, the worker's {@code job.execute} and {@code job.expire}; {@code conversation.id}
     * leads from there to every message of the negotiation.
     */
    private CompletableFuture<Void> auction(Job job) {
        var span = getTelemetry().spanBuilder("job.auction")
                .setAttribute("job.id", job.jobId())
                .setAttribute("job.kind", job.kind())
                .setAttribute("job.attempt", job.attempt() + 1L)
                .startSpan();
        return agentDirectory.findByCapability(job.kind()).thenCompose(capable -> {
            var candidates = AuctionRules.candidates(capable, job.excluded(), clock.instant(),
                    config.staleAfter());
            span.setAttribute("job.candidates", (long) candidates.size());
            if (candidates.isEmpty()) {
                log.debug("no live worker for {} ({})", job.jobId(), job.kind());
                span.setAttribute("job.outcome", "no_candidates");
                return CompletableFuture.<Void>completedFuture(null);
            }
            return dialogue.callForProposals(candidates, job.toSpec(), config.cfpTimeout())
                    .thenCompose(responses -> award(job, responses, span));
        }).whenComplete((v, e) -> {
            if (e != null) {
                span.setAttribute("job.outcome", "error").recordException(e)
                        .setStatus(SpanStatus.ERROR);
            } else {
                span.setStatus(SpanStatus.OK);
            }
            span.end();
        });
    }

    private CompletableFuture<Void> award(Job job, List<DialogueMessage> responses, Span span) {
        var offers = new ArrayList<Offer>();
        for (var response : responses) {
            if (response.performative() != Performative.PROPOSE) {
                continue;
            }
            try {
                offers.add(new Offer(response.senderId(), response.contentAs(Bid.class), response));
            } catch (IllegalArgumentException e) {
                log.warn("unreadable proposal from {} for {}", response.senderId(), job.jobId());
            }
        }
        span.setAttribute("job.proposals", (long) offers.size());
        var winner = AuctionRules.winner(offers);
        if (winner.isEmpty()) {
            log.debug("no proposals for {}", job.jobId());
            span.setAttribute("job.outcome", "no_proposals");
            return CompletableFuture.completedFuture(null);
        }

        var best = winner.get();
        var deadline = AuctionRules.deadline(clock.instant(), best.bid().eta(), config.deadlineMargin());
        var assigned = store.assign(job.jobId(), job.attempt(), best.workerId(), deadline);
        span.setAttribute("job.outcome", assigned ? "assigned" : "taken")
                .setAttribute("job.winner", best.workerId())
                .setAttribute("job.winner.eta_ms", best.bid().eta().toMillis())
                .setAttribute("conversation.id", best.message().conversationId());

        var sent = new ArrayList<CompletableFuture<Void>>();
        for (var offer : offers) {
            if (offer == best && assigned) {
                var spec = new JobSpec(job.jobId(), job.attempt() + 1, job.kind(), job.inputRef(),
                        job.sizeBytes());
                log.info("job {} -> {} (attempt {}, eta {})", job.jobId(), best.workerId(),
                        spec.attempt(), best.bid().eta());
                sent.add(sendAgree(offer.message(), spec).exceptionally(e -> {
                    // the row is ASSIGNED already: the deadline will see to it
                    log.warn("AGREE for {} to {} not sent: {}", job.jobId(), best.workerId(),
                            e.getMessage());
                    return null;
                }));
            } else {
                sent.add(dialogue.reply(offer.message(), Performative.REFUSE,
                        assigned ? "another proposal was selected" : "job already assigned"));
            }
        }
        if (!assigned) {
            log.info("job {} was taken while the auction ran", job.jobId());
        }
        return CompletableFuture.allOf(sent.toArray(new CompletableFuture[0]));
    }

    /** The only place an AGREE leaves from; a test replaces it to lose one. */
    CompletableFuture<Void> sendAgree(DialogueMessage proposal, JobSpec spec) {
        return dialogue.reply(proposal, Performative.AGREE, spec);
    }

    // ---------------------------------------------------------------------------------- deadlines

    /**
     * Gives back the jobs whose assignment ran past its deadline: each one fails as retryable, so
     * the worker goes into {@code excluded} and the job is auctioned again, or ends in
     * {@code FAILED} when its attempts are used up.
     *
     * <p>A behavior of its own and not a step of {@link #auctionRound()}: an auction can wait the
     * whole CFP timeout for a candidate that does not answer, and recovery must not wait behind
     * that. After a restart the first tick finds whatever expired meanwhile, so there is no
     * separate recovery code.
     *
     * <p>The first tick waits five seconds. A manager that was down finds, on start, results that
     * arrived while it was away, and an assignment whose deadline passed in the meantime is
     * overdue only if its result is not among them. Without the wait the sweep ran at start, ahead
     * of the consumer reading the stream, and rerun jobs that had finished.
     */
    @Behavior(type = CYCLIC, interval = "1s", initialDelay = "5s", autoStart = true)
    public void expireDueAssignments() {
        for (var job : store.dueForReassignment(clock.instant(), config.batchSize())) {
            var span = getTelemetry().spanBuilder("job.expire")
                    .setAttribute("job.id", job.jobId())
                    .setAttribute("job.attempt", (long) job.attempt())
                    .setAttribute("job.assignee", job.assignee())
                    .startSpan();
            try {
                var now = store.fail(job.jobId(), job.attempt(), job.assignee(),
                        DEADLINE_EXCEEDED, true);
                span.setAttribute("job.state", now.map(Enum::name).orElse("stale"))
                        .setStatus(SpanStatus.OK);
                now.ifPresent(state -> log.warn(
                        "job {} on {} ran past its deadline (attempt {}) -> {}",
                        job.jobId(), job.assignee(), job.attempt(), state));
            } catch (RuntimeException e) {
                span.recordException(e).setStatus(SpanStatus.ERROR);
                throw e;
            } finally {
                span.end();
            }
        }
    }

    // ---------------------------------------------------------------------------------- outcomes

    @DialogueHandler(performatives = Performative.INFORM)
    public void onInform(DialogueMessage msg) {
        var result = msg.contentAs(JobResult.class);
        if (store.complete(result.jobId(), result.attempt(), msg.senderId(), result.outputRef())) {
            log.info("job {} done by {} (attempt {})", result.jobId(), msg.senderId(), result.attempt());
        } else {
            log.warn("result of {} from {} (attempt {}) discarded: stale or duplicate",
                    result.jobId(), msg.senderId(), result.attempt());
        }
    }

    @DialogueHandler(performatives = Performative.FAILURE)
    public void onFailure(DialogueMessage msg) {
        var failure = msg.contentAs(JobFailure.class);
        var now = store.fail(failure.jobId(), failure.attempt(), msg.senderId(), failure.reason(),
                failure.retryable());
        if (now.isPresent()) {
            log.info("job {} failed on {} (attempt {}, retryable {}): {} -> {}", failure.jobId(),
                    msg.senderId(), failure.attempt(), failure.retryable(), failure.reason(),
                    now.get());
        } else {
            log.warn("failure of {} from {} (attempt {}) discarded: stale or duplicate",
                    failure.jobId(), msg.senderId(), failure.attempt());
        }
    }
}
