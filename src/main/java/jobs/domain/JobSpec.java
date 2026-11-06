package jobs.domain;

/**
 * A job as it travels in a CFP and in the AGREE. {@code attempt} is per assignment, not per job:
 * it is the fencing token the result has to come back with.
 */
public record JobSpec(String jobId, int attempt, String kind, String inputRef, long sizeBytes) {}
