package jobs.worker;

/**
 * How a {@link JobProcessor} says a job did not work out. {@code retryable} is the processor's
 * judgement of whether another worker could do better: false for an input that is wrong for
 * everyone, true for a problem that belongs to this worker or its environment.
 */
public final class JobProcessingException extends Exception {

    private final boolean retryable;

    public JobProcessingException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public JobProcessingException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
