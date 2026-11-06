package jobs.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("DurationEstimator")
class DurationEstimatorTest {

    @Test
    void usesTheInitialGuessUntilAKindIsObserved() {
        var estimator = new DurationEstimator(Duration.ofSeconds(5));
        assertThat(estimator.average("ocr")).isEqualTo(Duration.ofSeconds(5));
        assertThat(estimator.observations("ocr")).isZero();
    }

    @Test
    void theFirstObservationReplacesTheGuessOutright() {
        var estimator = new DurationEstimator(Duration.ofSeconds(5));
        estimator.record("ocr", Duration.ofMillis(800));
        assertThat(estimator.average("ocr")).isEqualTo(Duration.ofMillis(800));
    }

    @Test
    void laterObservationsMoveTheAverageTowardsThem() {
        var estimator = new DurationEstimator(Duration.ofSeconds(5));
        estimator.record("ocr", Duration.ofMillis(1000));
        estimator.record("ocr", Duration.ofMillis(2000));
        // 0.3 * 2000 + 0.7 * 1000
        assertThat(estimator.average("ocr")).isEqualTo(Duration.ofMillis(1300));
        for (int i = 0; i < 40; i++) {
            estimator.record("ocr", Duration.ofMillis(2000));
        }
        assertThat(estimator.average("ocr").toMillis()).isBetween(1990L, 2000L);
    }

    @Test
    void kindsAreIndependent() {
        var estimator = new DurationEstimator(Duration.ofSeconds(5));
        estimator.record("ocr", Duration.ofMillis(100));
        assertThat(estimator.average("thumbnail")).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsANonPositiveGuess() {
        assertThatThrownBy(() -> new DurationEstimator(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
