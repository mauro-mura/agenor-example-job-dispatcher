package jobs.manager;

import jobs.domain.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** A processor that always fails, and a job that kills its worker. */
@Tag("failure")
@DisplayName("Jobs that fail, or kill whoever runs them")
class ProcessorFailureIT {

    private Cluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @DisplayName("a retryable error every time: three attempts on the same worker, then FAILED")
    void aRetryableErrorEveryTime() throws Exception {
        cluster = new Cluster();
        cluster.startManager();
        cluster.startWorker("only", "FAIL", 0, 1_000);
        cluster.startProducer();
        var job = cluster.job("always");

        cluster.submit(job);
        var rows = cluster.awaitSettled(List.of(job), Duration.ofSeconds(60));
        var row = rows.get(0);

        Invariants.hold(List.of(job), rows);
        assertThat(row.state()).isEqualTo(JobState.FAILED);
        assertThat(row.attempt()).isEqualTo(3);
        assertThat(row.lastError()).contains("boom");
        assertThat(cluster.executions()).isEqualTo(3);
    }

    @Test
    @DisplayName("a job that kills every worker that tries it: FAILED after three, and nobody restarts them")
    void aJobThatKillsItsWorkers() throws Exception {
        cluster = new Cluster();
        cluster.startManager();
        for (var name : List.of("w1", "w2", "w3")) {
            cluster.startWorker(name, "POISON", 200, 1_000);
        }
        cluster.startProducer();
        var poison = cluster.job("poison");

        cluster.submit(poison);
        var rows = cluster.awaitSettled(List.of(poison), Duration.ofSeconds(120));
        var row = rows.get(0);
        var dead = List.of("worker-w1", "worker-w2", "worker-w3").stream()
                .filter(n -> !cluster.child(n).isAlive()).toList();

        // a perfectly good job of the same kind, with nobody left to run it
        var next = cluster.job("after");
        cluster.submit(next);
        ManagerEnv.pause(8_000);
        var waiting = cluster.store.find(next.jobId()).orElseThrow();

        cluster.startWorker("w4", "POISON", 200, 1_000);
        var afterRows = cluster.awaitSettled(List.of(next), Duration.ofSeconds(60));

        Invariants.hold(List.of(poison), rows);
        assertThat(row.state()).isEqualTo(JobState.FAILED);
        assertThat(row.attempt()).isEqualTo(3);
        assertThat(row.lastError()).isEqualTo(JobManager.DEADLINE_EXCEEDED);
        assertThat(dead).as("each attempt cost a worker").hasSize(3);
        assertThat(waiting.state()).isEqualTo(JobState.SUBMITTED);
        assertThat(waiting.attempt()).isZero();
        assertThat(afterRows.get(0).state()).isEqualTo(JobState.DONE);
    }
}
