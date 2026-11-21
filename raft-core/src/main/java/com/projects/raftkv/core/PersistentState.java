package com.projects.raftkv.core;

import java.util.List;
import java.util.Objects;

/**
 * Everything the core is constructed from after a (re)start: what Storage.loadAll() returns.
 * M3 adds the snapshot metadata; until then the log must start at index 1.
 */
public record PersistentState(HardState hardState, List<LogEntry> entries) {
    public static final PersistentState EMPTY = new PersistentState(HardState.INITIAL, List.of());

    public PersistentState {
        Objects.requireNonNull(hardState);
        entries = List.copyOf(entries);
        long prevTerm = 0;
        for (int i = 0; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            if (e.index() != i + 1)
                throw new IllegalArgumentException("non-contiguous log at position " + i + ": " + e);
            if (e.term() < prevTerm)
                throw new IllegalArgumentException("log terms must be non-decreasing: " + e);
            if (e.term() > hardState.currentTerm())
                throw new IllegalArgumentException("entry term exceeds currentTerm: " + e);
            prevTerm = e.term();
        }
    }
}
