package com.projects.raftkv.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Everything the core is constructed from after a (re)start: what Storage.loadAll() returns.
 * With a snapshot, the log holds only the entries after it: the first entry has index snapshot.index + 1
 * and terms continue non-decreasing from snapshot.term (DESIGN.md D1a rule 6).
 */
public record PersistentState(HardState hardState, Optional<Snapshot> snapshot, List<LogEntry> entries) {
    public static final PersistentState EMPTY = new PersistentState(HardState.INITIAL, List.of());

    public PersistentState(HardState hardState, List<LogEntry> entries) {
        this(hardState, Optional.empty(), entries);
    }

    public PersistentState {
        Objects.requireNonNull(hardState);
        Objects.requireNonNull(snapshot);
        entries = List.copyOf(entries);
        long base = snapshot.map(Snapshot::index).orElse(0L);
        long prevTerm = snapshot.map(Snapshot::term).orElse(0L);
        if (prevTerm > hardState.currentTerm())
            throw new IllegalArgumentException("snapshot term exceeds currentTerm: " + snapshot.get());
        for (int i = 0; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            if (e.index() != base + i + 1)
                throw new IllegalArgumentException("non-contiguous log at position " + i + " after base " + base + ": " + e);
            if (e.term() < prevTerm)
                throw new IllegalArgumentException("log terms must be non-decreasing: " + e);
            if (e.term() > hardState.currentTerm())
                throw new IllegalArgumentException("entry term exceeds currentTerm: " + e);
            prevTerm = e.term();
        }
    }
}
