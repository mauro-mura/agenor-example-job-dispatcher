package jobs.worker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The last {@code capacity} {@code (jobId, attempt)} keys a worker accepted, so that an AGREE
 * delivered twice (the transport is at-least-once) is acted on once. In memory on purpose: after
 * a restart the local queue is empty too, and the manager's deadline recovers the job.
 */
final class SeenJobs {

    private final Map<String, Boolean> keys;

    SeenJobs(int capacity) {
        this.keys = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacity;
            }
        };
    }

    /** {@code true} the first time {@code key} is seen, {@code false} for every repeat. */
    synchronized boolean firstTime(String key) {
        return keys.put(key, Boolean.TRUE) == null;
    }
}
