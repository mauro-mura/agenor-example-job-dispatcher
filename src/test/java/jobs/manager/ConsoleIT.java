package jobs.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import dev.agenor.tools.console.JettyWebConsole;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static jobs.manager.ManagerEnv.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A message the manager cannot read is not a row of {@code jobs}, so the console's dead
 * letters are the only place it shows up.
 */
@DisplayName("Unreadable submissions on the web console")
class ConsoleIT {

    private static final ManagerConfig CONFIG = new ManagerConfig(
            Duration.ofSeconds(3), Duration.ofSeconds(2), Duration.ofSeconds(3), 10);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @DisplayName("a payload that is not a JobSpec ends in /api/deadletters, with the handler's exception")
    void anUnreadableSubmissionIsListed() throws Exception {
        var env = new ManagerEnv(toClose, CONFIG);
        env.deadLetters = true;
        env.startManager();
        env.startProducer();
        var port = freePort();
        var console = JettyWebConsole.builder().port(port).runtime(env.managerNode.runtime()).build();
        console.start().join();
        toClose.add(() -> console.stop().join());

        env.producer.publish(Map.of("attempt", "not-a-number"));

        var client = HttpClient.newHttpClient();
        var json = new ObjectMapper();
        JsonNode entry = null;
        String last = "";
        var deadline = Instant.now().plus(Duration.ofSeconds(60));
        while (entry == null && Instant.now().isBefore(deadline)) {
            var response = client.send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + port + "/api/deadletters")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            last = response.body();
            for (var node : json.readTree(last).path("data")) {
                if ("jobs.submitted".equals(node.path("topic").asText())) {
                    entry = node;
                }
            }
            if (entry == null) {
                pause(500);
            }
        }

        assertThat(entry).as("a dead letter for jobs.submitted; last body: " + last).isNotNull();
        assertThat(entry.path("attempts").asInt()).isGreaterThanOrEqualTo(3);
        assertThat(entry.path("reason").asText()).contains("IllegalArgumentException");
        assertThat(env.store.summary(10).byState().values()).as("it never became a job")
                .containsOnly(0L);
    }
}
