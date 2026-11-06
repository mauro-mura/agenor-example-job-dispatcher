package jobs.domain;

/** Where a job is. {@code DONE} and {@code FAILED} are terminal. */
public enum JobState {
    SUBMITTED, ASSIGNED, DONE, FAILED
}
