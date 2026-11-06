package jobs.worker;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exponential moving average of how long each kind of job takes on this worker. It is what makes
 * a bid a number that exists rather than a random one.
 *
 * <p>Until a kind has been observed the estimate is a configured guess; the first observation
 * replaces it outright, so that an arbitrary starting value does not weigh on later bids.
 */
public final class DurationEstimator {

    private static final double ALPHA = 0.3;

    private record Stat(double averageMillis, int observations) {}

    private final Duration initialGuess;
    private final Map<String, Stat> stats = new ConcurrentHashMap<>();

    public DurationEstimator(Duration initialGuess) {
        if (initialGuess.isNegative() || initialGuess.isZero()) {
            throw new IllegalArgumentException("initial guess must be positive: " + initialGuess);
        }
        this.initialGuess = initialGuess;
    }

    /** The estimate for one job of {@code kind}. */
    public Duration average(String kind) {
        var stat = stats.get(kind);
        return stat == null ? initialGuess : Duration.ofMillis(Math.round(stat.averageMillis()));
    }

    public void record(String kind, Duration observed) {
        var millis = (double) observed.toMillis();
        stats.merge(kind, new Stat(millis, 1), (old, fresh) ->
                new Stat(ALPHA * millis + (1 - ALPHA) * old.averageMillis(), old.observations() + 1));
    }

    public int observations(String kind) {
        var stat = stats.get(kind);
        return stat == null ? 0 : stat.observations();
    }
}
