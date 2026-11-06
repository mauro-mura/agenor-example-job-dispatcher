package jobs.manager;

import dev.agenor.core.AgentDescriptor;
import dev.agenor.core.AgentStatus;
import jobs.domain.Bid;
import jobs.manager.AuctionRules.Offer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("AuctionRules")
class AuctionRulesTest {

    private static final Instant NOW = Instant.parse("2026-10-15T12:00:00Z");
    private static final Duration STALE = Duration.ofSeconds(90);

    private static AgentDescriptor worker(String id, Instant lastSeen) {
        return AgentDescriptor.builder(id).agentType("worker").status(AgentStatus.RUNNING)
                .lastSeen(lastSeen).build();
    }

    private static Offer offer(String id, long etaMillis, int queue) {
        return new Offer(id, new Bid("job", 1, Duration.ofMillis(etaMillis), queue), null);
    }

    @Test
    @DisplayName("workers that are fresh and not excluded, sorted")
    void freshAndNotExcluded() {
        var capable = List.of(worker("w-b", NOW), worker("w-a", NOW.minusSeconds(10)),
                worker("w-c", NOW));
        assertThat(AuctionRules.candidates(capable, List.of("w-c"), NOW, STALE))
                .containsExactly("w-a", "w-b");
    }

    @Test
    @DisplayName("a row not refreshed within staleAfter is not a candidate")
    void staleRowsAreDropped() {
        var capable = List.of(worker("live", NOW.minusSeconds(30)),
                worker("ghost", NOW.minusSeconds(91)));
        assertThat(AuctionRules.candidates(capable, List.of(), NOW, STALE)).containsExactly("live");
    }

    @Test
    @DisplayName("a row exactly at the limit is still live")
    void theLimitIsInclusive() {
        var capable = List.of(worker("edge", NOW.minus(STALE)));
        assertThat(AuctionRules.candidates(capable, List.of(), NOW, STALE)).containsExactly("edge");
    }

    @Test
    @DisplayName("when everyone is excluded they are asked again")
    void exclusionIsAPreference() {
        var capable = List.of(worker("w-a", NOW), worker("w-b", NOW));
        assertThat(AuctionRules.candidates(capable, List.of("w-a", "w-b"), NOW, STALE))
                .containsExactly("w-a", "w-b");
    }

    @Test
    @DisplayName("but an excluded worker that is not live is not resurrected by the fallback")
    void fallbackOnlyUsesLiveWorkers() {
        var capable = List.of(worker("w-a", NOW), worker("ghost", NOW.minusSeconds(500)));
        assertThat(AuctionRules.candidates(capable, List.of("w-a"), NOW, STALE))
                .containsExactly("w-a");
    }

    @Test
    @DisplayName("agents of another type are never candidates")
    void onlyWorkers() {
        var other = AgentDescriptor.builder("m-1").agentType("manager").status(AgentStatus.RUNNING)
                .lastSeen(NOW).build();
        assertThat(AuctionRules.candidates(List.of(other), List.of(), NOW, STALE)).isEmpty();
    }

    @Test
    @DisplayName("the lowest estimate wins")
    void lowestEta() {
        var winner = AuctionRules.winner(List.of(offer("slow", 5_000, 0), offer("fast", 1_000, 3)));
        assertThat(winner).get().extracting(Offer::workerId).isEqualTo("fast");
    }

    @Test
    @DisplayName("on equal estimates the shorter queue wins, then the id")
    void tieBreaks() {
        assertThat(AuctionRules.winner(List.of(offer("w-a", 1_000, 2), offer("w-b", 1_000, 1))))
                .get().extracting(Offer::workerId).isEqualTo("w-b");
        assertThat(AuctionRules.winner(List.of(offer("w-b", 1_000, 1), offer("w-a", 1_000, 1))))
                .get().extracting(Offer::workerId).isEqualTo("w-a");
    }

    @Test
    @DisplayName("no offers, no winner")
    void noOffers() {
        assertThat(AuctionRules.winner(List.of())).isEmpty();
    }

    @Test
    @DisplayName("deadline is twice the estimate plus the margin")
    void deadline() {
        assertThat(AuctionRules.deadline(NOW, Duration.ofSeconds(4), Duration.ofSeconds(30)))
                .isEqualTo(NOW.plusSeconds(38));
        assertThat(AuctionRules.deadline(NOW, Duration.ZERO, Duration.ofSeconds(30)))
                .isEqualTo(NOW.plusSeconds(30));
    }
}
