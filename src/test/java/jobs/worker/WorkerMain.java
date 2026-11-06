package jobs.worker;

import jobs.domain.JobSpec;
import jobs.service.ServiceMain;

import java.time.Duration;
import java.util.List;

/**
 * A worker as its own JVM, started through the service's own {@link ServiceMain} with a processor
 * the test controls; everything else (heartbeat, dead letters, directory) is the production
 * wiring. Killable with SIGKILL in the middle of a job.
 *
 * <p>Besides the variables of {@code ServiceConfig}:
 *
 * <pre>
 * KIND     the job kind it runs
 * HOLD_MS  how long each job takes
 * MODE     HOLD (default) | FAIL (throws a retryable error) | POISON (a job whose id contains
 *          "poison" makes the whole process exit, as an out-of-memory would)
 * </pre>
 *
 * <p>It prints {@code CHECKPOINT started}, and {@code CHECKPOINT running <jobId>} and
 * {@code CHECKPOINT done <jobId>} around each job.
 */
public final class WorkerMain {

    private WorkerMain() {}

    public static void main(String[] args) throws Exception {
        var env = System.getenv();
        var kind = env.get("KIND");
        var hold = Duration.ofMillis(Long.parseLong(env.getOrDefault("HOLD_MS", "0")));
        var mode = env.getOrDefault("MODE", "HOLD");

        var processor = new JobProcessor() {
            @Override public String kind() { return kind; }

            @Override
            public String process(JobSpec job) throws JobProcessingException {
                checkpoint("running " + job.jobId());
                if ("POISON".equals(mode) && job.jobId().contains("poison")) {
                    checkpoint("dying " + job.jobId());
                    Runtime.getRuntime().halt(137);
                }
                if ("FAIL".equals(mode)) {
                    throw new IllegalStateException("boom");
                }
                try {
                    Thread.sleep(hold.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new JobProcessingException("interrupted", true, e);
                }
                checkpoint("done " + job.jobId());
                return "out/" + job.jobId();
            }
        };

        var service = ServiceMain.run(env, config -> List.of(processor));
        checkpoint("started");
        Runtime.getRuntime().addShutdownHook(new Thread(service::close));
        new java.util.concurrent.CountDownLatch(1).await();
    }

    private static void checkpoint(String what) {
        System.out.println("CHECKPOINT " + what);
        System.out.flush();
    }
}
