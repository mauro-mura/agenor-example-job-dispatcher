package jobs.manager;

import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.domain.JobState;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** What must hold after any failure, whatever else happened. */
final class Invariants {

    private Invariants() {}

    /** Counts per state, for the record. */
    static Map<JobState, Long> byState(List<Job> rows) {
        var counts = new EnumMap<JobState, Long>(JobState.class);
        for (var state : JobState.values()) {
            counts.put(state, 0L);
        }
        rows.stream().filter(j -> j != null).forEach(j -> counts.merge(j.state(), 1L, Long::sum));
        return counts;
    }

    /**
     * No job is lost, none is half-written, and a finished job says who finished it.
     *
     * @param submitted the jobs that were put on the topic
     * @param rows      their rows, in the same order
     */
    static void hold(List<JobSpec> submitted, List<Job> rows) {
        assertThat(rows).as("one row per submitted job").hasSameSizeAs(submitted);
        for (int i = 0; i < rows.size(); i++) {
            var spec = submitted.get(i);
            var row = rows.get(i);
            assertThat(row).as("job " + spec.jobId() + " is on the table").isNotNull();
            switch (row.state()) {
                case DONE -> {
                    assertThat(row.outputRef()).as(spec.jobId() + " has an output").isNotBlank();
                    assertThat(row.assignee()).as(spec.jobId() + " says who did it").isNotBlank();
                    assertThat(row.attempt()).isGreaterThanOrEqualTo(1);
                }
                case FAILED -> assertThat(row.lastError()).as(spec.jobId() + " says why")
                        .isNotBlank();
                case ASSIGNED -> {
                    assertThat(row.assignee()).isNotBlank();
                    assertThat(row.deadlineAt()).isNotNull();
                }
                case SUBMITTED -> assertThat(row.assignee()).isNull();
            }
        }
    }
}
