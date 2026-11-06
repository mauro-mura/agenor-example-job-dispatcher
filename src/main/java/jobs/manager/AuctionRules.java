package jobs.manager;

import dev.agenor.core.AgentDescriptor;
import dev.agenor.core.dialogue.DialogueMessage;
import jobs.domain.Bid;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/** The decisions of an auction, with no messaging in them, so they can be tested without any. */
final class AuctionRules {

    static final String WORKER_TYPE = "worker";

    /** A proposal, with the message it came in: the AGREE is a reply to that message. */
    record Offer(String workerId, Bid bid, DialogueMessage message) {}

    private AuctionRules() {}

    /**
     * Whom to ask. Workers of the right type whose row is fresh, minus those that already failed
     * this job. The exclusion is a preference: if it would leave nobody, the workers that failed
     * are asked again, since {@code maxAttempts} bounds how many times that can happen.
     *
     * <p>Freshness tells a live worker from a row left behind by a killed one: the directory
     * returns both, and an auction waits for every participant.
     *
     * @return worker ids, sorted; empty only when no live worker has the capability at all
     */
    static List<String> candidates(Collection<AgentDescriptor> capable, Collection<String> excluded,
                                   Instant now, Duration staleAfter) {
        var cutoff = now.minus(staleAfter);
        var live = capable.stream()
                .filter(d -> WORKER_TYPE.equals(d.agentType()))
                .filter(d -> !d.lastSeen().isBefore(cutoff))
                .map(AgentDescriptor::agentId)
                .distinct()
                .sorted()
                .toList();
        var avoided = new HashSet<>(excluded);
        var preferred = live.stream().filter(id -> !avoided.contains(id)).toList();
        return preferred.isEmpty() ? live : preferred;
    }

    /** Lowest estimate wins; then the shorter queue; then the id, so the choice is repeatable. */
    static Optional<Offer> winner(Collection<Offer> offers) {
        return offers.stream().min(
                Comparator.comparing((Offer o) -> o.bid().eta())
                        .thenComparingInt(o -> o.bid().queueLength())
                        .thenComparing(Offer::workerId));
    }

    /**
     * {@code now + 2 x eta + margin}: twice the estimate, because an estimate is a guess.
     */
    static Instant deadline(Instant now, Duration eta, Duration margin) {
        return now.plus(eta.multipliedBy(2)).plus(margin);
    }
}
