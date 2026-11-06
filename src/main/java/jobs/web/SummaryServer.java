package jobs.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jobs.domain.Job;
import jobs.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * {@code GET /jobs/summary}: what the job table says, as JSON. Agenor's console shows agents and
 * messages; this shows jobs.
 *
 * <p>No authentication: it is meant for the same trusted network as the transport.
 */
public final class SummaryServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SummaryServer.class);
    private static final int DEFAULT_FAILED = 20;
    private static final int MAX_FAILED = 200;

    private final JobStore store;
    private final HttpServer server;
    private final ObjectMapper json = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    /** @param port {@code 0} picks a free one; {@link #port()} says which */
    public SummaryServer(JobStore store, int port) throws IOException {
        this.store = store;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/jobs/summary", this::handle);
        this.server.setExecutor(Executors.newFixedThreadPool(2, r -> {
            var t = new Thread(r, "summary-http");
            t.setDaemon(true);
            return t;
        }));
    }

    public SummaryServer start() {
        server.start();
        log.info("job summary on port {}", port());
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                send(exchange, 405, Map.of("error", "only GET is supported"));
                return;
            }
            if (!"/jobs/summary".equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, Map.of("error", "not found"));
                return;
            }
            try {
                send(exchange, 200, body(failedLimit(exchange.getRequestURI().getQuery())));
            } catch (RuntimeException e) {
                log.warn("summary failed: {}", e.getMessage());
                send(exchange, 500, Map.of("error", "summary unavailable"));
            }
        }
    }

    private Map<String, Object> body(int failedLimit) {
        var summary = store.summary(failedLimit);
        var body = new LinkedHashMap<String, Object>();
        body.put("jobs", summary.byState());
        body.put("doneByKind", summary.doneByKind().stream().map(t -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("kind", t.kind());
            row.put("done", t.done());
            row.put("averageMillis", t.average().toMillis());
            return row;
        }).toList());
        body.put("recentFailed", summary.recentFailed().stream().map(SummaryServer::failed).toList());
        body.put("note", "averageMillis is updated_at - created_at of DONE jobs: from submission "
                + "to result, waiting and retries included");
        return body;
    }

    private static Map<String, Object> failed(Job job) {
        var row = new LinkedHashMap<String, Object>();
        row.put("jobId", job.jobId());
        row.put("kind", job.kind());
        row.put("attempt", job.attempt());
        row.put("lastError", job.lastError());
        row.put("excluded", List.copyOf(job.excluded()));
        row.put("updatedAt", job.updatedAt());
        return row;
    }

    /** {@code ?failed=N}, bounded; anything unreadable falls back to the default. */
    private static int failedLimit(String query) {
        if (query != null) {
            for (var pair : query.split("&")) {
                if (pair.startsWith("failed=")) {
                    try {
                        return Math.max(0, Math.min(MAX_FAILED, Integer.parseInt(pair.substring(7))));
                    } catch (NumberFormatException ignored) {
                        return DEFAULT_FAILED;
                    }
                }
            }
        }
        return DEFAULT_FAILED;
    }

    private void send(HttpExchange exchange, int status, Object body) throws IOException {
        var bytes = json.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
