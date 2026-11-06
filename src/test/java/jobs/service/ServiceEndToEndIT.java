package jobs.service;

import jobs.domain.Job;
import jobs.domain.JobState;
import jobs.testsupport.Infra;
import jobs.store.JobStore;
import jobs.store.TestDatabase;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The three roles of {@link ServiceMain} with the production processors, on
 * Valkey and PostgreSQL: {@code submit} puts the PDFs on the queue and returns, the manager and
 * the worker take it from there, and the text comes out under {@code out/}.
 */
@DisplayName("The service from submission to extracted text")
class ServiceEndToEndIT {

    @TempDir Path storage;

    private Map<String, String> env(String role, String name, String prefix, String jdbcUrl) {
        var env = Infra.env(role, prefix, "node-" + name, name + "-" + prefix);
        env.put("JDBC_URL", jdbcUrl);
        env.put("STORAGE_ROOT", storage.toString());
        env.put("WORKER_CAPABILITIES", "pdf-to-text");
        env.put("SUMMARY_PORT", "0");
        env.put("CONSOLE_PORT", "0");
        env.put("STALE_AFTER_MS", "5000");
        return env;
    }

    @Test
    void submittedPdfsComeOutAsText() throws Exception {
        var u = Infra.unique();
        var prefix = "e2e-" + u;
        var db = TestDatabase.freshDatabase();
        var store = new JobStore(db, 3);
        writePdf(storage.resolve("in/alpha.pdf"), "Alpha document");
        writePdf(storage.resolve("in/beta.pdf"), "Beta document");
        Files.writeString(storage.resolve("in/notes.txt"), "not a PDF: ignored");

        try (var manager = ServiceMain.run(env("manager", "m", prefix, db.getJdbcUrl()));
             var worker = ServiceMain.run(env("worker", "w", prefix, db.getJdbcUrl()))) {
            // returns once the jobs are published: there is nothing left for this role to run
            ServiceMain.runAndWait(env("submit", "s", prefix, db.getJdbcUrl()));

            var deadline = Instant.now().plus(Duration.ofSeconds(60));
            List<Job> rows;
            do {
                Thread.sleep(500);
                rows = allJobs(db);
            } while (Instant.now().isBefore(deadline)
                    && (rows.size() < 2 || rows.stream().anyMatch(j -> j.state() != JobState.DONE)));

            assertThat(rows).as("one job per PDF, none for the text file").hasSize(2);
            assertThat(rows).allSatisfy(j -> assertThat(j.state()).isEqualTo(JobState.DONE));
            assertThat(rows).extracting(Job::inputRef)
                    .containsExactlyInAnyOrder("in/alpha.pdf", "in/beta.pdf");
            for (var job : rows) {
                var text = Files.readString(storage.resolve(job.outputRef()));
                assertThat(text).containsAnyOf("Alpha document", "Beta document");
            }
        }
    }

    @Test
    void aSubmitWithoutAnInboxFailsAndSaysWhere() {
        var u = Infra.unique();
        var db = TestDatabase.freshDatabase();
        assertThatThrownBy(() -> ServiceMain.runAndWait(
                env("submit", "s", "e2e-" + u, db.getJdbcUrl())))
                .hasMessageContaining("cannot list");
    }

    private static void writePdf(Path file, String text) throws Exception {
        Files.createDirectories(file.getParent());
        try (var document = new PDDocument()) {
            var page = new PDPage();
            document.addPage(page);
            try (var content = new PDPageContentStream(document, page)) {
                content.beginText();
                content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                content.newLineAtOffset(50, 700);
                content.showText(text);
                content.endText();
            }
            document.save(file.toFile());
        }
    }

    private static List<Job> allJobs(com.zaxxer.hikari.HikariDataSource db) throws Exception {
        var rows = new java.util.ArrayList<Job>();
        var store = new JobStore(db, 3);
        try (var c = db.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery("SELECT job_id FROM jobs ORDER BY job_id")) {
            while (rs.next()) {
                store.find(rs.getString(1)).ifPresent(rows::add);
            }
        }
        return rows;
    }
}
