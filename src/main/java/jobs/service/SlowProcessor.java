package jobs.service;

import jobs.domain.JobSpec;
import jobs.worker.JobProcessingException;
import jobs.worker.JobProcessor;

import java.time.Duration;

/**
 * Makes a processor take at least a given time. For demonstrations only ({@code
 * SIMULATED_WORK_MS}): a PDF of a few pages is extracted in milliseconds, and there would be no
 * window in which to kill the worker that holds the job.
 */
final class SlowProcessor implements JobProcessor {

    private final JobProcessor inner;
    private final Duration delay;

    SlowProcessor(JobProcessor inner, Duration delay) {
        this.inner = inner;
        this.delay = delay;
    }

    @Override
    public String kind() {
        return inner.kind();
    }

    @Override
    public String process(JobSpec job) throws JobProcessingException {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JobProcessingException("interrupted", true, e);
        }
        return inner.process(job);
    }
}
