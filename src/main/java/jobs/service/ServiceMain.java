package jobs.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.agenor.adapters.telemetry.OtelTelemetryFactory;
import dev.agenor.core.telemetry.AgenorTelemetry;
import dev.agenor.tools.console.JettyWebConsole;
import jobs.manager.JobManager;
import jobs.store.JobSchema;
import jobs.store.JobStore;
import jobs.web.SummaryServer;
import jobs.worker.JobProcessor;
import jobs.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.concurrent.CountDownLatch;

/**
 * The service: one executable, a manager, a worker or a submitter according to
 * {@code NODE_ROLE}. {@link #run} is the whole of it, and the tests start it the same way; a
 * test that needs a worker with a processor it controls passes that processor in.
 */
public final class ServiceMain {

    private static final Logger log = LoggerFactory.getLogger(ServiceMain.class);

    private ServiceMain() {}

    /** A started node and what runs beside it. Closing it stops them in reverse order. */
    public static final class Service implements AutoCloseable {
        private final ServiceConfig config;
        private final ServiceNode node;
        private final HikariDataSource pool;
        private final SummaryServer summary;
        private final JettyWebConsole console;

        private Service(ServiceConfig config, ServiceNode node, HikariDataSource pool,
                        SummaryServer summary, JettyWebConsole console) {
            this.config = config;
            this.node = node;
            this.pool = pool;
            this.summary = summary;
            this.console = console;
        }

        public ServiceConfig config() { return config; }

        public ServiceNode node() { return node; }

        @Override
        public void close() {
            if (console != null) {
                console.stop().join();
            }
            if (summary != null) {
                summary.close();
            }
            node.close();
            if (pool != null) {
                pool.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        runAndWait(System.getenv());
    }

    /** Starts the service, closes it on SIGTERM, and returns only when the process ends. */
    public static void runAndWait(Map<String, String> env) throws InterruptedException {
        var service = run(env);
        if (service.config().role() == ServiceConfig.Role.SUBMIT) {
            service.close();
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(service::close, "service-shutdown"));
        new CountDownLatch(1).await();
    }

    public static Service run(Map<String, String> env) {
        return run(env, ProductionProcessors::create);
    }

    /** As {@link #run(Map)}, with the worker's processors chosen by the caller. */
    public static Service run(Map<String, String> env,
                              Function<ServiceConfig, List<JobProcessor>> processors) {
        var config = ServiceConfig.from(env);
        AgenorTelemetry telemetry = config.otlpEndpoint() == null ? null
                : OtelTelemetryFactory.builder()
                        .serviceName("jobs-" + config.role().name().toLowerCase() + "-" + config.nodeId())
                        .exporter("otlp-http").endpoint(config.otlpEndpoint()).build();
        var options = new ServiceNode.Options(config.heartbeat(), telemetry, true);

        return switch (config.role()) {
            case WORKER -> startWorker(config, processors, options);
            case MANAGER -> startManager(config, options);
            case SUBMIT -> startSubmit(config, options);
        };
    }

    private static Service startWorker(ServiceConfig config,
                                       Function<ServiceConfig, List<JobProcessor>> processors,
                                       ServiceNode.Options options) {
        var worker = new JobWorker(config.agentId(), processors.apply(config), config.capacity(),
                config.initialGuess(), Duration.ofSeconds(10));
        var node = ServiceNode.start(config.nodeId(), config.groupPrefix(), config.redisUri(),
                config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.pendingMs(),
                options, worker);
        log.info("worker {} up on node {}", config.agentId(), config.nodeId());
        return new Service(config, node, null, null, null);
    }

    /** Publishes the jobs and prints their ids, one per line; there is nothing left to run. */
    private static Service startSubmit(ServiceConfig config, ServiceNode.Options options) {
        var submitter = new Submitter(config.agentId(), config.submitKind());
        var node = ServiceNode.start(config.nodeId(), config.groupPrefix(), config.redisUri(),
                config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.pendingMs(),
                options, submitter);
        try {
            submitter.submitAll(config.storageRoot()).forEach(id -> System.out.println("submitted " + id));
        } catch (RuntimeException e) {
            node.close();
            throw e;
        }
        return new Service(config, node, null, null, null);
    }

    private static Service startManager(ServiceConfig config, ServiceNode.Options options) {
        var pool = pool(config);
        var store = new JobStore(pool, config.maxAttempts());
        var manager = new JobManager(config.agentId(), store, config.manager(), Clock.systemUTC());
        JobSchema.migrate(pool);
        var node = ServiceNode.start(config.nodeId(), config.groupPrefix(), config.redisUri(),
                config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.pendingMs(),
                options, manager);

        SummaryServer summary = null;
        if (config.summaryPort() > 0) {
            try {
                summary = new SummaryServer(store, config.summaryPort()).start();
            } catch (java.io.IOException e) {
                node.close();
                pool.close();
                throw new IllegalStateException("summary port " + config.summaryPort()
                        + " unavailable: " + e.getMessage(), e);
            }
        }
        JettyWebConsole console = null;
        if (config.consolePort() > 0) {
            console = JettyWebConsole.builder().port(config.consolePort())
                    .runtime(node.runtime()).build();
            console.start().join();
        }
        log.info("manager {} up on node {}", config.agentId(), config.nodeId());
        return new Service(config, node, pool, summary, console);
    }

    private static HikariDataSource pool(ServiceConfig config) {
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.jdbcUser());
        hikari.setPassword(config.jdbcPassword());
        hikari.setMaximumPoolSize(8);
        return new HikariDataSource(hikari);
    }
}
