package com.projects.raftkv.sim;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.SoftState;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Hand-built logs and statuses for checker unit tests. */
final class LogFixtures {
    private LogFixtures() {}

    /** A log whose entry i has the given term and a command named by {@code tag}, index and term. */
    static ListLog log(String tag, long... terms) {
        List<LogEntry> es = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            String s = tag + (i + 1) + "@" + terms[i];
            es.add(new LogEntry(i + 1, terms[i], new EntryPayload.Command(Bytes.copyOf(s.getBytes(StandardCharsets.UTF_8)))));
        }
        return new ListLog(es);
    }

    static RaftStatus status(int id, Role role, long term, long commitIndex, long lastApplied) {
        NodeId n = new NodeId(id);
        Optional<NodeId> leader = role == Role.LEADER ? Optional.of(n) : Optional.empty();
        return new RaftStatus(n, new SoftState(role, term, leader), new HardState(term, Optional.empty()),
                commitIndex, lastApplied);
    }

    /** Builds a tracker that has seen the whole log once. */
    static NodeLogTracker tracked(LogView log) {
        NodeLogTracker t = new NodeLogTracker();
        t.reset();
        t.refresh(log);
        return t;
    }

    static final class ListLog implements LogView {
        final List<LogEntry> entries;
        ListLog(List<LogEntry> entries) { this.entries = new ArrayList<>(entries); }
        @Override public long lastIndex() { return entries.size(); }
        @Override public long lastTerm() { return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term(); }
        @Override public OptionalLong termAt(long index) {
            if (index == 0) return OptionalLong.of(0);
            if (index < 0 || index > lastIndex()) return OptionalLong.empty();
            return OptionalLong.of(entries.get((int) index - 1).term());
        }
        @Override public Optional<LogEntry> entryAt(long index) {
            if (index < 1 || index > lastIndex()) return Optional.empty();
            return Optional.of(entries.get((int) index - 1));
        }
    }
}
