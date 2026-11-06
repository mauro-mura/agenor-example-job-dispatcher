package jobs.manager;

import jobs.testsupport.Infra;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transport, and then the database, frozen for a few seconds while jobs are running. Frozen, not stopped: nothing is refused, connections just hang, which
 * is what a network partition looks like from inside.
 *
 * <p>Some jobs may run twice or fail; the assertion is that none is lost.
 */
@Tag("failure")
@DisplayName("Valkey and PostgreSQL frozen for a few seconds")
class InfrastructureFailureIT {

    private Cluster cluster;

    @AfterEach
    void tearDown() {
        // whatever happened, leave the shared containers running
        try {
            Infra.unpauseValkey();
        } catch (RuntimeException ignored) {
            // not paused
        }
        try {
            Infra.unpausePostgres();
        } catch (RuntimeException ignored) {
            // not paused
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private List<jobs.domain.JobSpec> tenJobsRunning() throws Exception {
        cluster = new Cluster();
        cluster.startManager();
        cluster.startWorker("a", "HOLD", 500, 1_000);
        cluster.startWorker("b", "HOLD", 500, 1_000);
        cluster.startProducer();
        var jobs = cluster.submitMany(10, "i");

        var deadline = Instant.now().plusSeconds(60);
        while (cluster.finishedExecutions() < 3 && Instant.now().isBefore(deadline)) {
            ManagerEnv.pause(50);
        }
        return jobs;
    }

    @Test
    @DisplayName("Valkey frozen for ten seconds")
    void valkeyFrozen() throws Exception {
        var jobs = tenJobsRunning();

        Infra.pauseValkey();
        ManagerEnv.pause(10_000);
        Infra.unpauseValkey();

        var rows = cluster.awaitSettled(jobs, Cluster.SETTLE);

        Invariants.hold(jobs, rows);
        assertThat(rows).allSatisfy(j -> assertThat(j.state().name()).isIn("DONE", "FAILED"));
    }

    @Test
    @DisplayName("PostgreSQL frozen for fifteen seconds")
    void postgresFrozen() throws Exception {
        var jobs = tenJobsRunning();

        Infra.pausePostgres();
        ManagerEnv.pause(15_000);
        Infra.unpausePostgres();

        var rows = cluster.awaitSettled(jobs, Cluster.SETTLE);

        Invariants.hold(jobs, rows);
        assertThat(rows).allSatisfy(j -> assertThat(j.state().name()).isIn("DONE", "FAILED"));
    }
}
