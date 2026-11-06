package jobs.store;

import com.zaxxer.hikari.HikariDataSource;
import dev.agenor.adapters.persistence.directory.JdbcAgentDirectory;
import dev.agenor.adapters.persistence.directory.JdbcDirectoryConfig;
import jobs.domain.Job;
import jobs.domain.JobSpec;
import jobs.domain.JobState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The transitions of {@link JobStore} and the fencing rule, on real PostgreSQL
 * ({@code text[]} and {@code timestamptz} are not worth emulating).
 */
@DisplayName("JobStore on PostgreSQL")
class JobStoreIT {

    private static final Instant T0 = Instant.parse("2026-10-15T12:00:00Z");

    private static HikariDataSource db;
    private static JobStore store;

    @BeforeAll
    static void migrate() {
        db = TestDatabase.freshDatabase();
        JobSchema.migrate(db);
        store = new JobStore(db, 3);
    }

    @AfterAll
    static void close() {
        if (db != null) db.close();
    }

    private static String newId() {
        return "job-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static JobSpec spec(String id) {
        return new JobSpec(id, 0, "pdf-to-text", "in/" + id, 2048);
    }

    private static Job get(String id) {
        return store.find(id).orElseThrow();
    }

    /** Submits {@code id} and assigns it, returning the attempt it is at. */
    private static int assigned(String id, String worker, Instant deadline) {
        store.submit(spec(id));
        var attempt = get(id).attempt();
        assertThat(store.assign(id, attempt, worker, deadline)).isTrue();
        return attempt + 1;
    }

    @Nested
    @DisplayName("submit")
    class Submit {

        @Test
        void insertsAsSubmittedAtAttemptZero() {
            var id = newId();
            assertThat(store.submit(spec(id))).isTrue();

            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.SUBMITTED);
            assertThat(job.attempt()).isZero();
            assertThat(job.assignee()).isNull();
            assertThat(job.excluded()).isEmpty();
            assertThat(job.sizeBytes()).isEqualTo(2048);
            assertThat(job.toSpec()).isEqualTo(spec(id));
        }

        @Test
        void isIdempotentAndTheFirstSubmissionWins() {
            var id = newId();
            assertThat(store.submit(spec(id))).isTrue();
            assertThat(store.submit(new JobSpec(id, 0, "ocr", "other", 1))).isFalse();

            assertThat(get(id).kind()).isEqualTo("pdf-to-text");
            assertThat(get(id).inputRef()).isEqualTo("in/" + id);
        }

        @Test
        void twentyConcurrentSubmissionsInsertOnce() throws Exception {
            var id = newId();
            var winners = new AtomicInteger();
            race(20, () -> {
                if (store.submit(spec(id))) winners.incrementAndGet();
            });
            assertThat(winners.get()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("assign")
    class Assign {

        @Test
        void startsTheNextAttempt() {
            var id = newId();
            store.submit(spec(id));

            assertThat(store.assign(id, 0, "worker-a", T0.plusSeconds(30))).isTrue();

            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.ASSIGNED);
            assertThat(job.attempt()).isEqualTo(1);
            assertThat(job.assignee()).isEqualTo("worker-a");
            assertThat(job.deadlineAt()).isEqualTo(T0.plusSeconds(30));
        }

        @Test
        void refusesAStaleExpectedAttempt() {
            var id = newId();
            store.submit(spec(id));

            assertThat(store.assign(id, 5, "worker-a", T0)).isFalse();
            assertThat(get(id).state()).isEqualTo(JobState.SUBMITTED);
        }

        @Test
        void refusesAJobThatIsAlreadyAssigned() {
            var id = newId();
            assigned(id, "worker-a", T0);

            assertThat(store.assign(id, 1, "worker-b", T0)).isFalse();
            assertThat(get(id).assignee()).isEqualTo("worker-a");
        }

        @Test
        void ofTwoRacingAuctionsExactlyOneWins() throws Exception {
            var id = newId();
            store.submit(spec(id));
            var winners = new AtomicInteger();
            race(2, () -> {
                if (store.assign(id, 0, "worker-" + Thread.currentThread().getId(), T0)) {
                    winners.incrementAndGet();
                }
            });
            assertThat(winners.get()).isEqualTo(1);
            assertThat(get(id).attempt()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("complete and the fencing rule")
    class Complete {

        @Test
        void recordsTheOutputOnce() {
            var id = newId();
            var attempt = assigned(id, "worker-a", T0);

            assertThat(store.complete(id, attempt, "worker-a", "out/" + id)).isTrue();
            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.DONE);
            assertThat(job.outputRef()).isEqualTo("out/" + id);
            assertThat(job.assignee()).isEqualTo("worker-a");
            assertThat(job.deadlineAt()).isNull();

            // an at-least-once redelivery of the same INFORM
            assertThat(store.complete(id, attempt, "worker-a", "out/other")).isFalse();
            assertThat(get(id).outputRef()).isEqualTo("out/" + id);
        }

        @Test
        void rejectsTheWrongAssignee() {
            var id = newId();
            var attempt = assigned(id, "worker-a", T0);

            assertThat(store.complete(id, attempt, "worker-b", "out")).isFalse();
            assertThat(get(id).state()).isEqualTo(JobState.ASSIGNED);
        }

        @Test
        void discardsTheResultOfAnAttemptThatWasReassigned() {
            var id = newId();
            var first = assigned(id, "worker-a", T0);
            assertThat(store.fail(id, first, "worker-a", "deadline exceeded", true))
                    .contains(JobState.SUBMITTED);
            assertThat(store.assign(id, first, "worker-b", T0.plusSeconds(60))).isTrue();

            // worker-a was only slow: its result for attempt 1 arrives during attempt 2
            assertThat(store.complete(id, first, "worker-a", "out/late")).isFalse();

            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.ASSIGNED);
            assertThat(job.attempt()).isEqualTo(2);
            assertThat(job.assignee()).isEqualTo("worker-b");
            assertThat(job.outputRef()).isNull();
        }
    }

    @Nested
    @DisplayName("fail")
    class Fail {

        @Test
        void retryableBelowTheLimitRequeuesAndExcludesTheWorker() {
            var id = newId();
            var attempt = assigned(id, "worker-a", T0);

            assertThat(store.fail(id, attempt, "worker-a", "boom", true)).contains(JobState.SUBMITTED);

            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.SUBMITTED);
            assertThat(job.assignee()).isNull();
            assertThat(job.deadlineAt()).isNull();
            assertThat(job.excluded()).containsExactly("worker-a");
            assertThat(job.lastError()).isEqualTo("boom");
            assertThat(job.attempt()).isEqualTo(1);
        }

        @Test
        void aWorkerAppearsOnceInExcludedEvenIfItFailsTwice() {
            var id = newId();
            var first = assigned(id, "worker-a", T0);
            store.fail(id, first, "worker-a", "boom", true);
            assertThat(store.assign(id, first, "worker-a", T0)).isTrue();
            store.fail(id, first + 1, "worker-a", "boom again", true);

            assertThat(get(id).excluded()).containsExactly("worker-a");
        }

        @Test
        void nonRetryableEndsTheJobAtOnce() {
            var id = newId();
            var attempt = assigned(id, "worker-a", T0);

            assertThat(store.fail(id, attempt, "worker-a", "corrupt input", false))
                    .contains(JobState.FAILED);

            var job = get(id);
            assertThat(job.state()).isEqualTo(JobState.FAILED);
            assertThat(job.lastError()).isEqualTo("corrupt input");
            assertThat(job.assignee()).isEqualTo("worker-a");
        }

        @Test
        void aStaleFailureChangesNothing() {
            var id = newId();
            var attempt = assigned(id, "worker-a", T0);
            assertThat(store.complete(id, attempt, "worker-a", "out")).isTrue();

            assertThat(store.fail(id, attempt, "worker-a", "too late", true)).isEmpty();
            assertThat(get(id).state()).isEqualTo(JobState.DONE);
            assertThat(get(id).lastError()).isNull();
        }

        @Test
        void failingEveryAttemptEndsInFailedWithEveryWorkerExcluded() {
            var id = newId();
            store.submit(spec(id));
            var workers = List.of("worker-a", "worker-b", "worker-c");
            JobState last = null;
            for (int i = 0; i < workers.size(); i++) {
                assertThat(store.assign(id, i, workers.get(i), T0)).isTrue();
                last = store.fail(id, i + 1, workers.get(i), "boom " + (i + 1), true).orElseThrow();
            }

            assertThat(last).isEqualTo(JobState.FAILED);
            var job = get(id);
            assertThat(job.attempt()).isEqualTo(3);
            assertThat(job.excluded()).containsExactlyInAnyOrderElementsOf(workers);
            assertThat(job.lastError()).isEqualTo("boom 3");
        }
    }

    @Nested
    @DisplayName("queries")
    class Queries {

        @Test
        void dueForReassignmentReturnsOnlyExpiredAssignmentsEarliestFirst() {
            var late = newId();
            var early = newId();
            var notYet = newId();
            var done = newId();
            var queued = newId();
            var now = T0.plus(1, ChronoUnit.DAYS);

            assigned(late, "w", now.minusSeconds(5));
            assigned(early, "w", now.minusSeconds(50));
            assigned(notYet, "w", now.plusSeconds(60));
            var doneAttempt = assigned(done, "w", now.minusSeconds(100));
            store.complete(done, doneAttempt, "w", "out");
            store.submit(spec(queued));

            var due = store.dueForReassignment(now, 1000).stream().map(Job::jobId).toList();

            assertThat(due).contains(early, late).doesNotContain(notYet, done, queued);
            assertThat(due.indexOf(early)).isLessThan(due.indexOf(late));
            assertThat(store.dueForReassignment(now, 1)).hasSize(1);
        }

        @Test
        void awaitingAssignmentIsOldestFirstAndOnlySubmitted() throws Exception {
            var first = newId();
            var second = newId();
            var taken = newId();
            store.submit(spec(first));
            Thread.sleep(20);
            store.submit(spec(second));
            assigned(taken, "w", T0);

            var waiting = store.awaitingAssignment(1000).stream().map(Job::jobId).toList();

            assertThat(waiting).contains(first, second).doesNotContain(taken);
            assertThat(waiting.indexOf(first)).isLessThan(waiting.indexOf(second));
        }
    }

    @Nested
    @DisplayName("schema")
    class Schema {

        @Test
        void migratingTwiceChangesNothing() {
            JobSchema.migrate(db);
            JobSchema.migrate(db);
            assertThat(store.submit(spec(newId()))).isTrue();
        }

        @Test
        void theConstraintsRefuseARowTheStoreWouldNeverWrite() throws SQLException {
            try (var conn = db.getConnection(); var st = conn.createStatement()) {
                assertThatThrownBy(() -> st.execute(
                        "INSERT INTO jobs (job_id, kind, input_ref, size_bytes, state) "
                                + "VALUES ('bad-" + newId() + "', 'k', 'i', 1, 'ASSIGNED')"))
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("jobs_assigned_has_owner");
                assertThatThrownBy(() -> st.execute(
                        "INSERT INTO jobs (job_id, kind, input_ref, size_bytes, state) "
                                + "VALUES ('bad-" + newId() + "', 'k', 'i', 1, 'LOST')"))
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("jobs_state_valid");
            }
        }

        /**
         * Why {@link JobSchema} baselines at 0: the database already holds Agenor's directory tables, so
         * Flyway has to baseline, and a baseline of 1 would skip our V1 without an error.
         */
        @Test
        void createsJobsOnADatabaseThatAlreadyHoldsTheAgentDirectory() throws Exception {
            try (var shared = TestDatabase.freshDatabase()) {
                var url = shared.getJdbcUrl();
                var directory = JdbcAgentDirectory.create(
                        JdbcDirectoryConfig.of(url, TestDatabase.USER, TestDatabase.PASSWORD));
                try {
                    JobSchema.migrate(shared);
                    var fresh = new JobStore(shared, 3);

                    assertThat(fresh.submit(spec(newId()))).isTrue();
                    try (var conn = shared.getConnection();
                         var rs = conn.createStatement().executeQuery(
                                 "SELECT count(*) FROM information_schema.tables "
                                         + "WHERE table_name = 'jobs_schema_history'")) {
                        rs.next();
                        assertThat(rs.getInt(1)).isEqualTo(1);
                    }
                } finally {
                    directory.close();
                }
            }
        }

        /**
         * The other order: the directory, which keeps a history table of its own, migrates
         * after us as well.
         */
        @Test
        void createsTheAgentDirectoryOnADatabaseThatAlreadyHoldsJobs() throws Exception {
            try (var shared = TestDatabase.freshDatabase()) {
                JobSchema.migrate(shared);
                var directory = JdbcAgentDirectory.create(JdbcDirectoryConfig.of(
                        shared.getJdbcUrl(), TestDatabase.USER, TestDatabase.PASSWORD));
                try (var conn = shared.getConnection();
                     var rs = conn.createStatement().executeQuery(
                             "SELECT count(*) FROM information_schema.tables "
                                     + "WHERE table_name = 'agenor_agents'")) {
                    rs.next();
                    assertThat(rs.getInt(1)).isEqualTo(1);
                } finally {
                    directory.close();
                }
            }
        }
    }

    /** Runs {@code task} on {@code threads} threads released together. */
    private static void race(int threads, Runnable task) throws Exception {
        var pool = Executors.newFixedThreadPool(threads);
        try {
            var ready = new CountDownLatch(threads);
            var go = new CountDownLatch(1);
            var futures = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    task.run();
                }));
            }
            assertThat(ready.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (var f : futures) {
                f.get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
