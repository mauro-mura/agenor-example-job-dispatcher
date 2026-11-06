package jobs.domain;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * What an operator wants to know without opening the table.
 *
 * @param byState      every state is present, with zero when there are no jobs in it
 * @param doneByKind   per kind, how many jobs reached {@code DONE} and how long they took
 * @param recentFailed the latest {@code FAILED} jobs, newest first, with the reason
 */
public record JobSummary(Map<JobState, Long> byState, List<KindTiming> doneByKind,
                         List<Job> recentFailed) {

    /**
     * @param average {@code updated_at - created_at} of the {@code DONE} jobs of this kind: time
     *                from submission to result, so waiting and retries are in it. How long one
     *                execution took is in the {@code job.execute} span, not here.
     */
    public record KindTiming(String kind, long done, Duration average) {}

    public JobSummary {
        byState = Map.copyOf(byState);
        doneByKind = List.copyOf(doneByKind);
        recentFailed = List.copyOf(recentFailed);
    }
}
