package jobs.service;

import jobs.worker.JobProcessor;
import jobs.worker.PdfToTextProcessor;

import java.util.List;

/** The processors of the service: one per kind a worker is told to run. */
final class ProductionProcessors {

    private ProductionProcessors() {}

    static List<JobProcessor> create(ServiceConfig config) {
        if (config.capabilities().isEmpty()) {
            throw new IllegalArgumentException(
                    "WORKER_CAPABILITIES is empty: a worker has to run at least one kind");
        }
        return config.capabilities().stream().<JobProcessor>map(kind -> switch (kind) {
            case PdfToTextProcessor.KIND -> new PdfToTextProcessor(config.storageRoot());
            default -> throw new IllegalArgumentException("no processor for kind '" + kind
                    + "'; known: " + PdfToTextProcessor.KIND);
        }).map(processor -> config.simulatedWork().isZero() ? processor
                : new SlowProcessor(processor, config.simulatedWork())).toList();
    }
}
