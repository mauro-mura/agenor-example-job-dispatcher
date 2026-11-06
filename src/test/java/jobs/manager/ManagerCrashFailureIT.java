package jobs.manager;

import jobs.domain.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The manager process killed with jobs in flight, and restarted.
 * Workers take 4 s per job and bid honestly (initial guess 4 s), so a deadline is about 13 s for
 * the first job and later for those queued behind it.
 */
@Tag("failure")
@DisplayName("Manager killed and restarted, with jobs in flight")
class ManagerCrashFailureIT {

    private Cluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private List<jobs.domain.JobSpec> sixJobsInFlight() throws Exception {
        cluster = new Cluster();
        cluster.startManager();
        cluster.startWorker("a", "HOLD", 4_000, 4_000);
        cluster.startWorker("b", "HOLD", 4_000, 4_000);
        cluster.startProducer();
        var jobs = cluster.submitMany(6, "c");

        // every job assigned and its AGREE gone: killing earlier lands in the window between the
        // table write and the message, which costs a job a full deadline
        var deadline = Instant.now().plusSeconds(60);
        while (Instant.now().isBefore(deadline)
                && !cluster.rows(jobs).stream().allMatch(j -> j != null
                        && j.state() == JobState.ASSIGNED)) {
            ManagerEnv.pause(100);
        }
        ManagerEnv.pause(1_000);
        return jobs;
    }

    @Test
    @DisplayName("back before the deadline: the results that waited are applied, nothing reruns")
    void restartedBeforeTheDeadline() throws Exception {
        var jobs = sixJobsInFlight();
        cluster.kill("manager");
        ManagerEnv.pause(8_000);
        cluster.startManager();

        var rows = cluster.awaitSettled(jobs, Cluster.SETTLE);

        Invariants.hold(jobs, rows);
        assertThat(rows).allSatisfy(j -> assertThat(j.state()).isEqualTo(JobState.DONE));
        assertThat(rows).allSatisfy(j -> assertThat(j.attempt()).isEqualTo(1));
        assertThat(cluster.executions()).as("no job ran twice").isEqualTo(6);
    }

    @Test
    @DisplayName("back after the deadline: some jobs may rerun, none is lost")
    void restartedAfterTheDeadline() throws Exception {
        var jobs = sixJobsInFlight();
        cluster.kill("manager");
        ManagerEnv.pause(16_000);
        cluster.startManager();

        var rows = cluster.awaitSettled(jobs, Cluster.SETTLE);

        Invariants.hold(jobs, rows);
        assertThat(rows).allSatisfy(j -> assertThat(j.state())
                .isIn(JobState.DONE, JobState.FAILED));
    }
}
