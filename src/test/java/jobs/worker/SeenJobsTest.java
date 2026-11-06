package jobs.worker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SeenJobsTest {

    @Test
    void reportsARepeatOnlyAfterTheFirstTime() {
        var seen = new SeenJobs(10);
        assertThat(seen.firstTime("job-1#1")).isTrue();
        assertThat(seen.firstTime("job-1#1")).isFalse();
        assertThat(seen.firstTime("job-1#2")).isTrue();
    }

    @Test
    void forgetsTheOldestKeyBeyondItsCapacity() {
        var seen = new SeenJobs(2);
        seen.firstTime("a");
        seen.firstTime("b");
        seen.firstTime("c");

        assertThat(seen.firstTime("a")).as("a was evicted").isTrue();
        assertThat(seen.firstTime("c")).as("c is recent").isFalse();
    }
}
