package jobs.worker;

import jobs.domain.JobSpec;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.regex.Pattern;

/**
 * Extracts the text of a PDF. Input and output live under a shared storage root: the job carries
 * a reference relative to it, and the result is {@code out/<jobId>.txt}.
 *
 * <p>The split between retryable and not is where the failure comes from. Reading the bytes and
 * writing the output are storage, which can be transient and differs per worker; parsing is a
 * property of the file, which would fail the same way anywhere.
 */
public final class PdfToTextProcessor implements JobProcessor {

    public static final String KIND = "pdf-to-text";

    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]+");

    private final Path root;

    public PdfToTextProcessor(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    @Override
    public String kind() {
        return KIND;
    }

    @Override
    public String process(JobSpec job) throws JobProcessingException {
        if (!SAFE_ID.matcher(job.jobId()).matches()) {
            throw new JobProcessingException("invalid job id: " + job.jobId(), false);
        }
        var input = root.resolve(job.inputRef()).normalize();
        if (!input.startsWith(root)) {
            throw new JobProcessingException("input outside the storage root: " + job.inputRef(), false);
        }
        if (!Files.isRegularFile(input)) {
            throw new JobProcessingException("input not found: " + job.inputRef(), false);
        }

        byte[] bytes;
        try {
            bytes = Files.readAllBytes(input);
        } catch (IOException e) {
            throw new JobProcessingException("cannot read input: " + e.getMessage(), true, e);
        }

        String text;
        try (var document = Loader.loadPDF(bytes)) {
            text = new PDFTextStripper().getText(document);
        } catch (IOException e) {
            throw new JobProcessingException("not a readable PDF: " + e.getMessage(), false, e);
        }

        var outputRef = "out/" + job.jobId() + ".txt";
        var output = root.resolve(outputRef);
        // A slow worker that finishes after its job was reassigned writes the same bytes to the
        // same name: the move is atomic so nobody ever reads half a file.
        var temporary = root.resolve("out/" + job.jobId() + ".txt.tmp-" + job.attempt());
        try {
            Files.createDirectories(output.getParent());
            Files.writeString(temporary, text, StandardCharsets.UTF_8);
            Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new JobProcessingException("cannot write output: " + e.getMessage(), true, e);
        }
        return outputRef;
    }
}
