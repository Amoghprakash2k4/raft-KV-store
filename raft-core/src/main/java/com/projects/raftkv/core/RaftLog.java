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

    /** Appends entries that must continue the log contiguously with non-decreasing terms. */
    void append(List<LogEntry> newEntries) {
        for (LogEntry e : newEntries) {
            if (e.index() != lastIndex() + 1)
                throw new IllegalStateException("non-contiguous append: expected " + (lastIndex() + 1) + ", got " + e);
            if (e.term() < lastTerm())
                throw new IllegalStateException("term decreases at " + e + " after term " + lastTerm());
            entries.add(e);
        }
    }

    /** Deletes the entries with index >= fromIndex. Only followers do this (§5.3); leaders never do. */
    void truncateFrom(long fromIndex) {
        if (fromIndex < 1 || fromIndex > lastIndex() + 1)
            throw new IllegalStateException("truncateFrom " + fromIndex + " outside [1, " + (lastIndex() + 1) + "]");
        entries.subList((int) (fromIndex - 1), entries.size()).clear();
    }

    /** Up to {@code max} entries starting at {@code from}; empty if from == lastIndex + 1. */
    List<LogEntry> entriesFrom(long from, int max) {
        if (from < 1 || from > lastIndex() + 1)
            throw new IllegalArgumentException("entriesFrom " + from + " outside [1, " + (lastIndex() + 1) + "]");
        int start = (int) (from - 1);
        return List.copyOf(entries.subList(start, Math.min(entries.size(), start + max)));
    }

    /** Fast backup (follower side): the first index of the run of entries with term termAt(index). */
    long firstIndexOfTermAt(long index) {
        long term = termAt(index).orElseThrow();
        long i = index;
        while (i > 1 && termAt(i - 1).getAsLong() == term) i--;
        return i;
    }

    /** Fast backup (leader side): the last index holding {@code term}, or 0 if the log has none. */
    long lastIndexOfTerm(long term) {
        for (long i = lastIndex(); i >= 1; i--) {
            long t = termAt(i).getAsLong();
            if (t == term) return i;
            if (t < term) return 0;   // terms only decrease going backwards
        }
        return 0;
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
