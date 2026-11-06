package jobs.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.agenor.core.Message;
import jobs.domain.Bid;
import jobs.domain.JobFailure;
import jobs.domain.JobResult;
import jobs.domain.JobSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The payloads cross a serialising transport as JSON and come back as a {@code Map}, which is
 * what {@code contentAs} converts. This reproduces that path without a container, including the
 * {@code Duration} in {@link Bid}.
 */
@DisplayName("payloads across the wire")
class JobPayloadsTest {

    private final ObjectMapper wire = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @SuppressWarnings("unchecked")
    private <T> T acrossTheWire(T payload, Class<T> type) throws Exception {
        var json = wire.writeValueAsString(payload);
        var asReceived = wire.readValue(json, Map.class);
        return Message.convertContent(asReceived, type);
    }

    @Test
    void jobSpec() throws Exception {
        var spec = new JobSpec("job-1", 2, "ocr", "in/job-1", 9_876_543_210L);
        assertThat(acrossTheWire(spec, JobSpec.class)).isEqualTo(spec);
    }

    @Test
    void bidKeepsItsDuration() throws Exception {
        var bid = new Bid("job-1", 2, Duration.ofMillis(2_500), 4);
        assertThat(acrossTheWire(bid, Bid.class)).isEqualTo(bid);
    }

    @Test
    void jobResult() throws Exception {
        var result = new JobResult("job-1", 2, "out/job-1");
        assertThat(acrossTheWire(result, JobResult.class)).isEqualTo(result);
    }

    @Test
    void jobFailureKeepsItsRetryableFlag() throws Exception {
        var failure = new JobFailure("job-1", 2, "corrupt input", false);
        assertThat(acrossTheWire(failure, JobFailure.class)).isEqualTo(failure);
    }
}
