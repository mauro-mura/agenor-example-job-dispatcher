package jobs.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.agenor.adapters.telemetry.OtelTelemetryFactory;
import jobs.domain.JobState;
import jobs.testsupport.Infra;
import jobs.worker.FakeProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static jobs.manager.ManagerEnv.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Against a real backend: spans leave two services through OTLP, and searching Jaeger for
 * {@code job.id} finds those of both. Spans of different nodes are correlated by attribute, not
 * joined into one trace.
 */
@DisplayName("Jaeger: search by job.id across services")
class JaegerIT {

    private static final ManagerConfig CONFIG = new ManagerConfig(
            Duration.ofSeconds(3), Duration.ofSeconds(2), Duration.ofSeconds(3), 10);

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            toClose.get(i).close();
        }
    }

    /** The first span of {@code service} named {@code operation} that carries {@code job.id}. */
    private Optional<JsonNode> findSpan(String service, String jobId, String operation)
            throws Exception {
        var tags = URLEncoder.encode("{\"job.id\":\"" + jobId + "\"}", StandardCharsets.UTF_8);
        var url = Infra.jaegerQueryUrl() + "/api/traces?service=" + service + "&tags=" + tags
                + "&limit=20";
        var response = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            return Optional.empty();
        }
        for (var trace : json.readTree(response.body()).path("data")) {
            for (var span : trace.path("spans")) {
                if (operation.equals(span.path("operationName").asText())) {
                    return Optional.of(span);
                }
            }
        }
        return Optional.empty();
    }

    private static String tag(JsonNode span, String key) {
        for (var tag : span.path("tags")) {
            if (key.equals(tag.path("key").asText())) {
                return tag.path("value").asText();
            }
        }
        return null;
    }

    private Optional<JsonNode> awaitSpan(String service, String jobId, String operation)
            throws Exception {
        var deadline = Instant.now().plus(Duration.ofSeconds(40));
        while (Instant.now().isBefore(deadline)) {
            var found = findSpan(service, jobId, operation);
            if (found.isPresent()) {
                return found;
            }
            pause(1_000);
        }
        return Optional.empty();
    }

    @Test
    @DisplayName("job.id finds job.auction under the manager's service and job.execute under the worker's")
    void searchByJobIdAcrossServices() throws Exception {
        var env = new ManagerEnv(toClose, CONFIG);
        var managerService = "jobs-manager-" + env.u;
        var workerService = "jobs-worker-" + env.u;
        var endpoint = Infra.jaegerOtlpEndpoint();

        env.telemetry = OtelTelemetryFactory.builder().serviceName(workerService)
                .exporter("otlp-http").endpoint(endpoint).build();
        env.startWorker("a", FakeProcessor.ok(env.kind), Duration.ofSeconds(1));
        env.telemetry = OtelTelemetryFactory.builder().serviceName(managerService)
                .exporter("otlp-http").endpoint(endpoint).build();
        env.startManager();
        env.startProducer();
        var job = env.job("jaeger");

        env.producer.submit(job);
        env.awaitJob(job.jobId(), j -> j.state() == JobState.DONE, "reached DONE");

        var auction = awaitSpan(managerService, job.jobId(), "job.auction");
        var execution = awaitSpan(workerService, job.jobId(), "job.execute");

        assertThat(auction).as("job.auction under " + managerService).isPresent();
        assertThat(execution).as("job.execute under " + workerService).isPresent();
        assertThat(tag(auction.get(), "job.outcome")).isEqualTo("assigned");
        assertThat(tag(execution.get(), "job.outcome")).isEqualTo("done");
        assertThat(tag(auction.get(), "conversation.id")).isNotBlank()
                .isEqualTo(tag(execution.get(), "conversation.id"));
    }
}
