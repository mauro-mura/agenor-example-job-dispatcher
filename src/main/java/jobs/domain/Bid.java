package jobs.domain;

import java.time.Duration;

/** A worker's offer: its estimated time to finish the job, and how many jobs are ahead of it. */
public record Bid(String jobId, int attempt, Duration eta, int queueLength) {}
