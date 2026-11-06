package jobs.service;

import dev.agenor.core.Message;
import dev.agenor.core.annotations.Agent;
import dev.agenor.runtime.agent.BaseAgent;
import jobs.domain.JobSpec;
import jobs.manager.JobManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Puts one job per PDF found under {@code <root>/in} on {@code jobs.submitted}. The way into the
 * service for whoever is not a test: the manager is the only writer of the table, so jobs reach
 * it as messages like any other.
 */
@Agent(type = "producer")
public final class Submitter extends BaseAgent {

    private final String id;
    private final String kind;

    public Submitter(String id, String kind) {
        this.id = id;
        this.kind = kind;
    }

    @Override public String getAgentId() { return id; }
    @Override public String getAgentName() { return "Submitter " + id; }

    /** @return the ids of the jobs published, in the order of the file names */
    public List<String> submitAll(Path root) {
        var inbox = root.resolve("in");
        List<Path> files;
        try (Stream<Path> listing = Files.list(inbox)) {
            files = listing.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".pdf"))
                    .sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list " + inbox, e);
        }
        var ids = new ArrayList<String>();
        for (var file : files) {
            var name = file.getFileName().toString();
            var jobId = "job-" + name.substring(0, name.length() - 4).replaceAll("[^A-Za-z0-9._-]", "_")
                    + "-" + UUID.randomUUID().toString().substring(0, 6);
            long size;
            try {
                size = Files.size(file);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot read " + file, e);
            }
            var spec = new JobSpec(jobId, 0, kind, "in/" + name, size);
            var message = Message.builder()
                    .topic(JobManager.SUBMITTED_TOPIC)
                    .senderId(id)
                    .content(spec)
                    .build();
            getMessageDispatcher().publish(message).join();
            ids.add(jobId);
        }
        return ids;
    }
}
