package jobs.store;

import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.domain.JobState;
import jobs.domain.JobSummary;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;

/**
 * The only writer of the {@code jobs} table. Every transition is one statement whose
 * {@code WHERE} clause is the guard, so a late or duplicate message changes nothing and says so by
 * returning {@code false} (or empty).
 *
 * <p>Fencing: an assignment increments {@code attempt}; a result counts only if the row is still
 * {@code ASSIGNED} at that attempt, to that assignee. That one rule covers an at-least-once
 * redelivery and a slow worker that finishes after its job was given to someone else.
 *
 * <p>Time is a parameter, never the database clock, so callers and tests control it.
 */
public final class JobStore {

    private static final String COLUMNS = "job_id, kind, input_ref, size_bytes, state, attempt, "
            + "assignee, deadline_at, excluded, output_ref, last_error, created_at, updated_at";

    private final DataSource dataSource;
    private final int maxAttempts;

    /**
     * @param dataSource  a pool on the database {@link JobSchema#migrate} has run against
     * @param maxAttempts how many assignments a job gets before a failure is final
     */
    public JobStore(DataSource dataSource, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
        this.dataSource = dataSource;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Records a new job as {@code SUBMITTED}. Idempotent on the job id: the first submission wins
     * and a repeat changes nothing.
     *
     * @return {@code true} if the job was inserted, {@code false} if it already existed
     */
    public boolean submit(JobSpec spec) {
        return update("INSERT INTO jobs (job_id, kind, input_ref, size_bytes, state) "
                        + "VALUES (?, ?, ?, ?, 'SUBMITTED') ON CONFLICT (job_id) DO NOTHING",
                spec.jobId(), spec.kind(), spec.inputRef(), spec.sizeBytes()) == 1;
    }

    public Optional<Job> find(String jobId) {
        var jobs = query("SELECT " + COLUMNS + " FROM jobs WHERE job_id = ?", jobId);
        return jobs.stream().findFirst();
    }

    /** Jobs waiting for an auction, oldest first. */
    public List<Job> awaitingAssignment(int limit) {
        return query("SELECT " + COLUMNS + " FROM jobs WHERE state = 'SUBMITTED' "
                + "ORDER BY created_at, job_id LIMIT ?", limit);
    }

    /**
     * Gives a {@code SUBMITTED} job to {@code assignee} and starts its next attempt.
     *
     * @param expectedAttempt the attempt the caller saw; two auctions racing on one job cannot
     *                        both win, because only one of them still matches
     * @return {@code false} if the job is not {@code SUBMITTED} at that attempt any more
     */
    public boolean assign(String jobId, int expectedAttempt, String assignee, Instant deadline) {
        return update("UPDATE jobs SET state = 'ASSIGNED', attempt = attempt + 1, assignee = ?, "
                        + "deadline_at = ?, updated_at = now() "
                        + "WHERE job_id = ? AND state = 'SUBMITTED' AND attempt = ?",
                assignee, utc(deadline), jobId, expectedAttempt) == 1;
    }

    /**
     * Records a result.
     *
     * @return {@code false} for a stale or duplicate result: wrong attempt, wrong assignee, or
     *         the job is no longer {@code ASSIGNED}
     */
    public boolean complete(String jobId, int attempt, String assignee, String outputRef) {
        return update("UPDATE jobs SET state = 'DONE', output_ref = ?, deadline_at = NULL, "
                        + "updated_at = now() "
                        + "WHERE job_id = ? AND state = 'ASSIGNED' AND attempt = ? AND assignee = ?",
                outputRef, jobId, attempt, assignee) == 1;
    }

    /**
     * Records a failed attempt, including an expired deadline (callers pass
     * {@code "deadline exceeded"} and {@code retryable = true}).
     *
     * <p>The assignee goes into {@code excluded} either way: it is the list of workers that have
     * failed on this job. A retryable failure below {@code maxAttempts} puts the job back to
     * {@code SUBMITTED}; anything else ends it in {@code FAILED}.
     *
     * @return the state the job is in now, or empty if the failure was stale (same guard as
     *         {@link #complete})
     */
    public Optional<JobState> fail(String jobId, int attempt, String assignee, String reason,
                                   boolean retryable) {
        var requeue = retryable && attempt < maxAttempts;
        var sql = "UPDATE jobs SET state = ?, last_error = ?, deadline_at = NULL, updated_at = now(), "
                + "excluded = CASE WHEN assignee = ANY(excluded) THEN excluded "
                + "ELSE array_append(excluded, assignee) END, "
                + "assignee = CASE WHEN ? THEN NULL ELSE assignee END "
                + "WHERE job_id = ? AND state = 'ASSIGNED' AND attempt = ? AND assignee = ? "
                + "RETURNING state";
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            bind(ps, (requeue ? JobState.SUBMITTED : JobState.FAILED).name(), reason, requeue,
                    jobId, attempt, assignee);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(JobState.valueOf(rs.getString(1))) : Optional.empty();
            }
        } catch (SQLException e) {
            throw failure("fail " + jobId, e);
        }
    }

    /**
     * {@code ASSIGNED} jobs whose deadline is before {@code now}, earliest deadline first. The
     * deadline lives in the table, so it survives a restart of the manager.
     */
    public List<Job> dueForReassignment(Instant now, int limit) {
        return query("SELECT " + COLUMNS + " FROM jobs WHERE state = 'ASSIGNED' "
                + "AND deadline_at < ? ORDER BY deadline_at, job_id LIMIT ?", utc(now), limit);
    }

    /**
     * A read of the whole table, for the operator: jobs per state, how long each kind takes to
     * reach {@code DONE}, and the latest failures. It changes nothing.
     *
     * @param failedLimit how many of the most recent {@code FAILED} jobs to include
     */
    public JobSummary summary(int failedLimit) {
        var byState = new EnumMap<JobState, Long>(JobState.class);
        for (var state : JobState.values()) {
            byState.put(state, 0L);
        }
        var timings = new ArrayList<JobSummary.KindTiming>();
        try (var conn = dataSource.getConnection()) {
            try (var st = conn.createStatement();
                 var rs = st.executeQuery("SELECT state, count(*) FROM jobs GROUP BY state")) {
                while (rs.next()) {
                    byState.put(JobState.valueOf(rs.getString(1)), rs.getLong(2));
                }
            }
            try (var st = conn.createStatement();
                 var rs = st.executeQuery("SELECT kind, count(*), "
                         + "extract(epoch FROM avg(updated_at - created_at)) FROM jobs "
                         + "WHERE state = 'DONE' GROUP BY kind ORDER BY kind")) {
                while (rs.next()) {
                    timings.add(new JobSummary.KindTiming(rs.getString(1), rs.getLong(2),
                            Duration.ofMillis(Math.round(rs.getDouble(3) * 1000))));
                }
            }
        } catch (SQLException e) {
            throw failure("summary", e);
        }
        var failed = query("SELECT " + COLUMNS + " FROM jobs WHERE state = 'FAILED' "
                + "ORDER BY updated_at DESC, job_id LIMIT ?", failedLimit);
        return new JobSummary(byState, timings, failed);
    }

    private int update(String sql, Object... params) {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw failure(sql, e);
        }
    }

    private List<Job> query(String sql, Object... params) {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (var rs = ps.executeQuery()) {
                var jobs = new ArrayList<Job>();
                while (rs.next()) {
                    jobs.add(read(rs));
                }
                return jobs;
            }
        } catch (SQLException e) {
            throw failure(sql, e);
        }
    }

    private static void bind(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static Job read(ResultSet rs) throws SQLException {
        var excluded = (String[]) rs.getArray("excluded").getArray();
        return new Job(
                rs.getString("job_id"),
                rs.getString("kind"),
                rs.getString("input_ref"),
                rs.getLong("size_bytes"),
                JobState.valueOf(rs.getString("state")),
                rs.getInt("attempt"),
                rs.getString("assignee"),
                instant(rs, "deadline_at"),
                List.of(excluded),
                rs.getString("output_ref"),
                rs.getString("last_error"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        var value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static IllegalStateException failure(String what, SQLException e) {
        return new IllegalStateException("job store: " + what + " failed: " + e.getMessage(), e);
    }
}
