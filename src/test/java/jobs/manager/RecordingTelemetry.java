package jobs.manager;

import dev.agenor.core.telemetry.AgenorTelemetry;
import dev.agenor.core.telemetry.Span;
import dev.agenor.core.telemetry.SpanBuilder;
import dev.agenor.core.telemetry.SpanScope;
import dev.agenor.core.telemetry.SpanStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Keeps every span it is asked for, so a test can read the attributes back. The interface has one
 * method, so this is a few lines and needs no OpenTelemetry on the classpath.
 */
public final class RecordingTelemetry implements AgenorTelemetry {

    /** One span: what it was called, what was put on it, and how it ended. */
    public static final class Recorded implements Span {
        private final String name;
        private final Map<String, Object> attributes = new ConcurrentHashMap<>();
        private volatile SpanStatus status = SpanStatus.UNSET;
        private volatile Throwable error;
        private volatile boolean ended;

        private Recorded(String name, Map<String, Object> initial) {
            this.name = name;
            this.attributes.putAll(initial);
        }

        public String name() { return name; }
        public boolean ended() { return ended; }
        public SpanStatus status() { return status; }
        public Throwable error() { return error; }

        /** The attribute as a string, or {@code null} if it was never set. */
        public String attr(String key) {
            var value = attributes.get(key);
            return value == null ? null : String.valueOf(value);
        }

        public Map<String, Object> attributes() { return Map.copyOf(attributes); }

        @Override public Span setAttribute(String key, String value) { attributes.put(key, value); return this; }
        @Override public Span setAttribute(String key, long value) { attributes.put(key, value); return this; }
        @Override public Span setAttribute(String key, boolean value) { attributes.put(key, value); return this; }
        @Override public Span setAttribute(String key, double value) { attributes.put(key, value); return this; }
        @Override public Span recordException(Throwable t) { error = t; return this; }
        @Override public Span setStatus(SpanStatus s) { status = s; return this; }
        @Override public SpanScope makeCurrent() { return () -> { }; }
        @Override public void end() { ended = true; }

        @Override public String toString() { return name + attributes; }
    }

    private final List<Recorded> spans = new CopyOnWriteArrayList<>();

    @Override
    public SpanBuilder spanBuilder(String operationName) {
        var initial = new ConcurrentHashMap<String, Object>();
        return new SpanBuilder() {
            @Override public SpanBuilder setAttribute(String key, String value) { initial.put(key, value); return this; }
            @Override public SpanBuilder setAttribute(String key, long value) { initial.put(key, value); return this; }
            @Override public SpanBuilder setAttribute(String key, boolean value) { initial.put(key, value); return this; }

            @Override
            public Span startSpan() {
                var span = new Recorded(operationName, initial);
                spans.add(span);
                return span;
            }
        };
    }

    public List<Recorded> spans() {
        return List.copyOf(spans);
    }

    /** Spans of one name, optionally only those whose {@code job.id} is {@code jobId}. */
    public List<Recorded> named(String name, String jobId) {
        return spans.stream()
                .filter(s -> s.name().equals(name))
                .filter(s -> jobId == null || jobId.equals(s.attr("job.id")))
                .toList();
    }
}
