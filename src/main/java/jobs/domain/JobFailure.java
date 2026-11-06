package jobs.domain;

/** FAILURE payload; {@code retryable = false} sends the job to {@code FAILED} without a retry. */
public record JobFailure(String jobId, int attempt, String reason, boolean retryable) {}
