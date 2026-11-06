package jobs.domain;

/** INFORM payload: the output lives in shared storage, the message carries only its reference. */
public record JobResult(String jobId, int attempt, String outputRef) {}
