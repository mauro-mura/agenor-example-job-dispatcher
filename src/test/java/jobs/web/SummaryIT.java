package jobs.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import jobs.domain.JobSpec;
import jobs.domain.JobState;
import jobs.store.JobSchema;
import jobs.store.JobStore;
import jobs.store.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** The summary of the job table, as a query and over HTTP. */
@DisplayName("Job summary")
class SummaryIT {

    private static final Instant T0 = Instant.parse("2026-10-15T12:00:00Z");

    private final ObjectMapper json = new ObjectMapper();
    private HikariDataSource db;
    private JobStore store;

    @BeforeEach
    void setUp() {
        db = TestDatabase.freshDatabase();
        JobSchema.migrate(db);
        store = new JobStore(db, 3);
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    private void submit(String id, String kind) {
        store.submit(new JobSpec(id, 0, kind, "in/" + id, 1));
    }

    /** Pins the two timestamps, so an average is a number the test chose. */
    private void timestamps(String id, Instant created, Duration took) throws SQLException {
        try (var conn = db.getConnection();
             var ps = conn.prepareStatement(
                     "UPDATE jobs SET created_at = ?, updated_at = ? WHERE job_id = ?")) {
            ps.setObject(1, java.time.OffsetDateTime.ofInstant(created, java.time.ZoneOffset.UTC));
            ps.setObject(2, java.time.OffsetDateTime.ofInstant(created.plus(took),
                    java.time.ZoneOffset.UTC));
            ps.setString(3, id);
            ps.executeUpdate();
        }
    }

    private void done(String id, String kind, Duration took) throws SQLException {
        submit(id, kind);
        store.assign(id, 0, "w", T0.plusSeconds(60));
        store.complete(id, 1, "w", "out/" + id);
        timestamps(id, T0, took);
    }

    private void failed(String id, String reason, Duration after) throws SQLException {
        submit(id, "ocr");
        store.assign(id, 0, "w", T0.plusSeconds(60));
        store.fail(id, 1, "w", reason, false);
        timestamps(id, T0, after);
    }

    private void seed() throws SQLException {
        submit("waiting-1", "ocr");
        submit("waiting-2", "pdf");
        submit("running", "ocr");
        store.assign("running", 0, "w", T0.plusSeconds(60));
        done("done-ocr-1", "ocr", Duration.ofSeconds(10));
        done("done-ocr-2", "ocr", Duration.ofSeconds(20));
        done("done-pdf-1", "pdf", Duration.ofSeconds(4));
        failed("failed-old", "corrupt input", Duration.ofSeconds(5));
        failed("failed-new", "input not found", Duration.ofSeconds(50));
    }

    @Test
    @DisplayName("counts per state, average time to DONE per kind, latest failures first")
    void summarisesTheTable() throws Exception {
        seed();

        var summary = store.summary(10);

        assertThat(summary.byState()).containsEntry(JobState.SUBMITTED, 2L)
                .containsEntry(JobState.ASSIGNED, 1L)
                .containsEntry(JobState.DONE, 3L)
                .containsEntry(JobState.FAILED, 2L);
        assertThat(summary.doneByKind()).hasSize(2);
        assertThat(summary.doneByKind().get(0).kind()).isEqualTo("ocr");
        assertThat(summary.doneByKind().get(0).done()).isEqualTo(2);
        assertThat(summary.doneByKind().get(0).average()).isEqualTo(Duration.ofSeconds(15));
        assertThat(summary.doneByKind().get(1).kind()).isEqualTo("pdf");
        assertThat(summary.doneByKind().get(1).average()).isEqualTo(Duration.ofSeconds(4));
        assertThat(summary.recentFailed()).extracting(j -> j.jobId())
                .containsExactly("failed-new", "failed-old");
        assertThat(summary.recentFailed().get(0).lastError()).isEqualTo("input not found");
    }

    @Test
    @DisplayName("the failure list is limited, and only the newest are kept")
    void limitsTheFailures() throws Exception {
        seed();
        assertThat(store.summary(1).recentFailed()).extracting(j -> j.jobId())
                .containsExactly("failed-new");
        assertThat(store.summary(0).recentFailed()).isEmpty();
    }

    @Test
    @DisplayName("an empty table is zeros and empty lists, not an error")
    void emptyTable() {
        var summary = store.summary(10);

        assertThat(summary.byState()).containsOnlyKeys(JobState.values());
        assertThat(summary.byState().values()).containsOnly(0L);
        assertThat(summary.doneByKind()).isEmpty();
        assertThat(summary.recentFailed()).isEmpty();
    }

    @Test
    @DisplayName("GET /jobs/summary answers JSON with the same numbers, and says what 'average' means")
    void servesTheSummary() throws Exception {
        seed();
        try (var server = new SummaryServer(store, 0).start()) {
            var response = get(server.port(), "/jobs/summary");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type").orElse(""))
                    .startsWith("application/json");
            JsonNode body = json.readTree(response.body());
            assertThat(body.at("/jobs/DONE").asLong()).isEqualTo(3);
            assertThat(body.at("/jobs/SUBMITTED").asLong()).isEqualTo(2);
            assertThat(body.at("/doneByKind/0/kind").asText()).isEqualTo("ocr");
            assertThat(body.at("/doneByKind/0/averageMillis").asLong()).isEqualTo(15_000);
            assertThat(body.at("/recentFailed/0/jobId").asText()).isEqualTo("failed-new");
            assertThat(body.at("/recentFailed/0/lastError").asText()).isEqualTo("input not found");
            assertThat(body.at("/note").asText()).contains("waiting and retries included");
        }
    }

    @Test
    @DisplayName("?failed=N bounds the list; anything but GET is refused")
    void boundsAndMethods() throws Exception {
        seed();
        try (var server = new SummaryServer(store, 0).start()) {
            var one = json.readTree(get(server.port(), "/jobs/summary?failed=1").body());
            assertThat(one.at("/recentFailed")).hasSize(1);

            var post = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(uri(server.port(), "/jobs/summary"))
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(post.statusCode()).isEqualTo(405);
            assertThat(post.headers().firstValue("Allow")).contains("GET");

            assertThat(get(server.port(), "/jobs/summary/extra").statusCode()).isEqualTo(404);
        }
    }

    private static URI uri(int port, String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(uri(port, path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
