package com.projects.raftkv.dashboard;

import com.projects.raftkv.bench.ResultStore;
import com.projects.raftkv.bench.RunRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * At most one job at a time (DESIGN.md M6-C): benchmarks must not share the machine with anything else,
 * and a second simulation would only slow the first one down. The browser polls {@link #status()}.
 */
final class Jobs {
    private static final Logger log = LoggerFactory.getLogger(Jobs.class);

    /** What a job reports while it runs. {@code live} holds the numbers the page draws as they arrive. */
    record Status(String id, String kind, String label, String state, long startedAtMs, long elapsedMs,
                  Map<String, Object> progress, String resultId, String error) {}

    /** The work itself; it reports progress and returns the record it saved. */
    interface Work {
        RunRecord run(Map<String, Object> progress, AtomicBoolean cancel) throws Exception;
    }

    private final ResultStore store;
    private final AtomicReference<Status> current = new AtomicReference<>();
    private final AtomicBoolean cancel = new AtomicBoolean();
    private volatile Map<String, Object> progress = Map.of();
    private int counter;

    Jobs(ResultStore store) {
        this.store = store;
    }

    ResultStore store() { return store; }

    /** Starts {@code work} unless something is running; returns the job id, or empty if busy. */
    synchronized Optional<String> start(String kind, String label, Work work) {
        Status s = current.get();
        if (s != null && s.state().equals("running")) return Optional.empty();
        String id = kind + "-job-" + (++counter);
        long started = System.currentTimeMillis();
        Map<String, Object> p = java.util.Collections.synchronizedMap(new LinkedHashMap<>());
        progress = p;
        cancel.set(false);
        current.set(new Status(id, kind, label, "running", started, 0, p, null, null));
        Thread.ofPlatform().name("job-" + id).start(() -> {
            try {
                RunRecord r = work.run(p, cancel);
                current.set(new Status(id, kind, label, cancel.get() ? "cancelled" : "done", started,
                        System.currentTimeMillis() - started, p, r == null ? null : r.id(), null));
            } catch (Exception | Error e) {
                log.error("job {} failed", id, e);
                current.set(new Status(id, kind, label, "failed", started, System.currentTimeMillis() - started, p, null,
                        e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        });
        return Optional.of(id);
    }

    void cancel() { cancel.set(true); }

    Optional<Status> status() {
        Status s = current.get();
        if (s == null) return Optional.empty();
        long elapsed = s.state().equals("running") ? System.currentTimeMillis() - s.startedAtMs() : s.elapsedMs();
        Map<String, Object> copy;
        synchronized (progress) {
            copy = new LinkedHashMap<>(progress);
        }
        return Optional.of(new Status(s.id(), s.kind(), s.label(), s.state(), s.startedAtMs(), elapsed, copy, s.resultId(), s.error()));
    }
}
