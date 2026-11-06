package jobs.domain;

import java.time.Instant;
import java.util.List;

/** A row of the {@code jobs} table. Read-only: it changes only through {@code JobStore}. */
public record Job(
        String jobId,
        String kind,
        String inputRef,
        long sizeBytes,
        JobState state,
        int attempt,
        String assignee,
        Instant deadlineAt,
        List<String> excluded,
        String outputRef,
        String lastError,
        Instant createdAt,
        Instant updatedAt) {

    public Job {
        excluded = List.copyOf(excluded);
    }

    /** The message to put on the wire for the attempt this row is at. */
    public JobSpec toSpec() {
        return new JobSpec(jobId, attempt, kind, inputRef, sizeBytes);
    }
}
