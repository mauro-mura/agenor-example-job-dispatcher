package jobs.service;

import dev.agenor.adapters.messaging.redis.RedisMessagingFactory;
import dev.agenor.adapters.persistence.directory.JdbcAgentDirectory;
import dev.agenor.adapters.persistence.directory.JdbcDirectoryConfig;
import dev.agenor.core.Agent;
import dev.agenor.core.telemetry.AgenorTelemetry;
import dev.agenor.runtime.AgenorRuntime;

import java.time.Duration;

/**
 * One Agenor node: its own Redis factory (a stream per node), its own handle on the JDBC
 * directory, and a runtime on top of both, with heartbeats, telemetry and the dead letter queue
 * switched on as {@link Options} says.
 */
public final class ServiceNode implements AutoCloseable {

    /**
     * @param heartbeat       how often the directory row of each agent is refreshed; without it a
     *                        worker stops being a candidate once its row is older than the
     *                        manager's {@code staleAfter}, and nothing says so
     * @param telemetry       passed to the runtime, the Redis factory and the directory, which are
     *                        three separate places; {@code null} for none
     * @param deadLetterQueue whether the runtime reads the Redis dead letter stream, which is
     *                        what the console's {@code /api/deadletters} shows
     */
    public record Options(Duration heartbeat, AgenorTelemetry telemetry, boolean deadLetterQueue) {}

    private final RedisMessagingFactory factory;
    private final JdbcAgentDirectory directory;
    private final AgenorRuntime runtime;

    private ServiceNode(RedisMessagingFactory factory, JdbcAgentDirectory directory,
                        AgenorRuntime runtime) {
        this.factory = factory;
        this.directory = directory;
        this.runtime = runtime;
    }

    /**
     * Builds the node, registers the agents and starts it.
     *
     * @param nodeId      stable: the stream, the consumer name and the directory endpoint derive
     *                    from it, so a restart reuses it and finds its own unread messages
     * @param pendingMs   {@code pendingEntriesTimeoutMs}, how long before an unacknowledged
     *                    entry is reclaimed
     */
    public static ServiceNode start(String nodeId, String groupPrefix, String redisUri,
                                    String jdbcUrl, String jdbcUser, String jdbcPassword,
                                    long pendingMs, Options options, Agent... agents) {
        var factoryBuilder = RedisMessagingFactory.builder()
                .uri(redisUri)
                .nodeId(nodeId)
                .consumerGroupPrefix(groupPrefix)
                .pendingEntriesTimeoutMs(pendingMs);
        if (options.telemetry() != null) {
            factoryBuilder.telemetry(options.telemetry());
        }
        var factory = factoryBuilder.build();

        var directoryConfig = JdbcDirectoryConfig.of(jdbcUrl, jdbcUser, jdbcPassword);
        var directory = options.telemetry() != null
                ? JdbcAgentDirectory.create(directoryConfig, options.telemetry())
                : JdbcAgentDirectory.create(directoryConfig);

        var builder = AgenorRuntime.builder()
                .messageDispatcher(factory.messageDispatcher(directory::resolver))
                .agentRegistry(directory.registry())
                .agentDiscovery(directory.discovery())
                .agentResolver(directory.resolver());
        if (options.heartbeat() != null) {
            builder.agentPresence(directory.presence(options.heartbeat().multipliedBy(6)))
                    .heartbeatInterval(options.heartbeat());
        }
        if (options.telemetry() != null) {
            builder.telemetry(options.telemetry());
        }
        if (options.deadLetterQueue()) {
            builder.deadLetterQueue(factory.deadLetterQueue());
        }
        var runtime = builder.build();
        for (var agent : agents) {
            runtime.registerAgent(agent);
        }
        runtime.start().join();
        return new ServiceNode(factory, directory, runtime);
    }

    public AgenorRuntime runtime() { return runtime; }

    public JdbcAgentDirectory directory() { return directory; }

    public RedisMessagingFactory factory() { return factory; }

    @Override
    public void close() {
        if (runtime.isRunning()) {
            runtime.stop().join();
        }
        factory.close();
        directory.close();
    }
}
