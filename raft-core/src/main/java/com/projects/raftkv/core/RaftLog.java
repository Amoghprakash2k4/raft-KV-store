package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The core's in-memory copy of the log (DESIGN.md D1). Entry i (1-based) is at position i-1.
 * M3 adds a snapshot base (snapshotIndex, snapshotTerm).
 */
final class RaftLog implements LogView {
    private final List<LogEntry> entries;

    RaftLog(List<LogEntry> recovered) {
        this.entries = new ArrayList<>(recovered);
    }

    @Override public long lastIndex() { return entries.size(); }

    @Override public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    @Override public OptionalLong termAt(long index) {
        if (index == 0) return OptionalLong.of(0);
        if (index < 0 || index > lastIndex()) return OptionalLong.empty();
        return OptionalLong.of(entries.get((int) (index - 1)).term());
    }

    @Override public Optional<LogEntry> entryAt(long index) {
        if (index < 1 || index > lastIndex()) return Optional.empty();
        return Optional.of(entries.get((int) (index - 1)));
    }

    /**
     * Election restriction (§5.4.1): the candidate's log is at least as up to date as ours if its
     * last term is higher, or the last terms are equal and its log is at least as long.
     */
    boolean isAtLeastAsUpToDateAsMine(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastTerm();
        if (candidateLastTerm != myLastTerm) return candidateLastTerm > myLastTerm;
        return candidateLastIndex >= lastIndex();
    }
}
