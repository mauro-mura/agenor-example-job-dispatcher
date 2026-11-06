package jobs.worker;

import jobs.domain.JobSpec;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PdfToTextProcessor")
class PdfToTextProcessorTest {

    @TempDir Path root;

    static void writePdf(Path file, String text) throws IOException {
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

    private static JobSpec spec(String jobId, String inputRef) {
        return new JobSpec(jobId, 1, PdfToTextProcessor.KIND, inputRef, 1);
    }

    @Test
    void extractsTheTextAndReturnsTheOutputReference() throws Exception {
        writePdf(root.resolve("in/doc.pdf"), "Contract Net works");
        var processor = new PdfToTextProcessor(root);

        var outputRef = processor.process(spec("job-1", "in/doc.pdf"));

        assertThat(outputRef).isEqualTo("out/job-1.txt");
        assertThat(Files.readString(root.resolve(outputRef))).contains("Contract Net works");
        try (var leftovers = Files.list(root.resolve("out"))) {
            assertThat(leftovers.map(p -> p.getFileName().toString()))
                    .as("no temporary file is left behind").containsExactly("job-1.txt");
        }
    }

    @Test
    void aMissingInputIsNotRetryable() {
        var processor = new PdfToTextProcessor(root);
        assertThatThrownBy(() -> processor.process(spec("job-2", "in/absent.pdf")))
                .isInstanceOfSatisfying(JobProcessingException.class, e -> {
                    assertThat(e.isRetryable()).isFalse();
                    assertThat(e).hasMessageContaining("not found");
                });
    }

    @Test
    void aFileThatIsNotAPdfIsNotRetryable() throws Exception {
        Files.createDirectories(root.resolve("in"));
        Files.writeString(root.resolve("in/bad.pdf"), "this is not a pdf", StandardCharsets.UTF_8);
        var processor = new PdfToTextProcessor(root);

        assertThatThrownBy(() -> processor.process(spec("job-3", "in/bad.pdf")))
                .isInstanceOfSatisfying(JobProcessingException.class, e -> {
                    assertThat(e.isRetryable()).isFalse();
                    assertThat(e).hasMessageContaining("not a readable PDF");
                });
    }

    @Test
    void refusesToLeaveTheStorageRoot() throws Exception {
        var processor = new PdfToTextProcessor(root);
        assertThatThrownBy(() -> processor.process(spec("job-4", "../../etc/passwd")))
                .isInstanceOfSatisfying(JobProcessingException.class, e ->
                        assertThat(e.isRetryable()).isFalse());
    }

    @Test
    void refusesAJobIdThatCouldEscapeTheOutputDirectory() throws Exception {
        writePdf(root.resolve("in/doc.pdf"), "x");
        var processor = new PdfToTextProcessor(root);
        assertThatThrownBy(() -> processor.process(spec("../evil", "in/doc.pdf")))
                .isInstanceOfSatisfying(JobProcessingException.class, e ->
                        assertThat(e.isRetryable()).isFalse());
    }
}
