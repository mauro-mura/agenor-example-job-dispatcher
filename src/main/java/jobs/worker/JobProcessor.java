package jobs.worker;

import jobs.domain.JobSpec;

/** Does the work for one {@code kind} of job. */
public interface JobProcessor {

    /** The job kind this processor handles; it is also the capability the worker advertises. */
    String kind();

    /**
     * Runs the job. Called on the worker's single execution thread, never from a message
     * handler.
     *
     * @return the reference of the output, which is what travels in the INFORM
     * @throws JobProcessingException when the job cannot be done; the exception says whether
     *         another worker should try
     */
    String process(JobSpec job) throws JobProcessingException;
}
