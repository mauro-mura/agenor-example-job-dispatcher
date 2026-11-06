package jobs.manager;

import jobs.domain.Job;
import jobs.domain.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A batch of jobs on three real worker processes of different speeds, and the same with the
 * fastest one killed in the middle.
 */
@Tag("failure")
@DisplayName("Under load, with processes of the real service")
class LoadFailureIT {

    private Cluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private Cluster threeWorkers() throws Exception {
        cluster = new Cluster();
        cluster.startManager();
        cluster.startWorker("fast", "HOLD", 200, 1_000);
        cluster.startWorker("medium", "HOLD", 600, 1_000);
        cluster.startWorker("slow", "HOLD", 1_500, 1_000);
        cluster.startProducer();
        return cluster;
    }

    private Map<String, Long> perWorker(Cluster c, List<Job> rows) {
        var counts = new TreeMap<String, Long>();
        for (var name : List.of("fast", "medium", "slow")) {
            counts.put(name, rows.stream()
                    .filter(j -> c.workerId(name).equals(j.assignee())).count());
        }
        return counts;
    }

    @Test
    @DisplayName("twenty jobs, three workers: all done, once each, the fast one takes the most")
    void twentyJobsThreeWorkers() throws Exception {
        var c = threeWorkers();
        var jobs = c.submitMany(20, "a");

        var rows = c.awaitSettled(jobs, Cluster.SETTLE);
        var perWorker = perWorker(c, rows);
        var http = c.summary();

        Invariants.hold(jobs, rows);
        assertThat(rows).allSatisfy(j -> assertThat(j.state()).isEqualTo(JobState.DONE));
        assertThat(c.executions()).as("each job ran once").isEqualTo(20);
        assertThat(perWorker.get("fast")).as("the fast worker took more than the slow one")
                .isGreaterThan(perWorker.get("slow"));
        assertThat(http).isNotNull();
        assertThat(http.at("/jobs/DONE").asLong()).isEqualTo(20);
    }

    @Test
    @DisplayName("the fastest worker killed with jobs queued: every job still ends DONE")
    void theFastWorkerDiesMidBatch() throws Exception {
        var c = threeWorkers();
        var jobs = c.submitMany(20, "b");

        var deadline = Instant.now().plusSeconds(60);
        while (c.executions("worker-fast") < 3 && Instant.now().isBefore(deadline)) {
            ManagerEnv.pause(50);
        }
        c.kill("worker-fast");

        var rows = c.awaitSettled(jobs, Cluster.SETTLE);

        Invariants.hold(jobs, rows);
        assertThat(rows.stream().map(Job::state).collect(Collectors.toSet()))
                .containsOnly(JobState.DONE);
        assertThat(rows.stream().mapToInt(Job::attempt).max().orElse(0)).isLessThanOrEqualTo(2);
        assertThat(c.executions()).isGreaterThanOrEqualTo(20);
    }
}
