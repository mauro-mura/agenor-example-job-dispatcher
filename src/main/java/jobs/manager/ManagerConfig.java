package jobs.manager;

import java.time.Duration;

/**
 * @param cfpTimeout     how long an auction waits for proposals. It is paid in full whenever a
 *                       candidate does not answer, because {@code callForProposals} waits for
 *                       every participant
 * @param deadlineMargin added to twice the winning bid's estimate to get the assignment's
 *                       deadline; it absorbs the first job of a worker that has not been measured
 * @param staleAfter     a worker whose directory row has not been refreshed for this long is not
 *                       a candidate. Workers must send heartbeats more often than this
 * @param batchSize      jobs examined per round
 */
public record ManagerConfig(Duration cfpTimeout, Duration deadlineMargin, Duration staleAfter,
                            int batchSize) {

    public ManagerConfig {
        for (var d : new Duration[] {cfpTimeout, deadlineMargin, staleAfter}) {
            if (d == null || d.isNegative() || d.isZero()) {
                throw new IllegalArgumentException("durations must be positive: " + d);
            }
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1: " + batchSize);
        }
    }

    public static ManagerConfig defaults() {
        return new ManagerConfig(Duration.ofSeconds(3), Duration.ofSeconds(30),
                Duration.ofSeconds(90), 10);
    }
}
