package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The core's in-memory copy of the log (DESIGN.md D1) after a snapshot base:
 * {@code (snapshotIndex, snapshotTerm, entries[snapshotIndex+1 ..])}. Entry i is at position
 * i - snapshotIndex - 1. Without a snapshot the base is (0, 0).
 */
final class RaftLog implements LogView {
    private long snapshotIndex;
    private long snapshotTerm;
    private final List<LogEntry> entries;

    RaftLog(long snapshotIndex, long snapshotTerm, List<LogEntry> recovered) {
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.entries = new ArrayList<>(recovered);
    }

    @Override public long snapshotIndex() { return snapshotIndex; }
    @Override public long snapshotTerm() { return snapshotTerm; }

    @Override public long lastIndex() { return snapshotIndex + entries.size(); }

    @Override public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.get(entries.size() - 1).term();
    }

    @Override public OptionalLong termAt(long index) {
        if (index == snapshotIndex) return OptionalLong.of(snapshotTerm);
        if (index < snapshotIndex || index > lastIndex()) return OptionalLong.empty();
        return OptionalLong.of(at(index).term());
    }

    @Override public Optional<LogEntry> entryAt(long index) {
        if (index <= snapshotIndex || index > lastIndex()) return Optional.empty();
        return Optional.of(at(index));
    }

    private LogEntry at(long index) { return entries.get((int) (index - snapshotIndex - 1)); }

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

    /**
     * Deletes the entries with index >= fromIndex. Only followers do this (§5.3); leaders never do.
     * Never below the snapshot: compacted entries are committed (D1a rule 3).
     */
    void truncateFrom(long fromIndex) {
        if (fromIndex <= snapshotIndex || fromIndex > lastIndex() + 1)
            throw new IllegalStateException("truncateFrom " + fromIndex + " outside [" + (snapshotIndex + 1)
                    + ", " + (lastIndex() + 1) + "]");
        entries.subList((int) (fromIndex - snapshotIndex - 1), entries.size()).clear();
    }

    /** Up to {@code max} entries starting at {@code from}; empty if from == lastIndex + 1. */
    List<LogEntry> entriesFrom(long from, int max) {
        if (from <= snapshotIndex || from > lastIndex() + 1)
            throw new IllegalArgumentException("entriesFrom " + from + " outside [" + (snapshotIndex + 1)
                    + ", " + (lastIndex() + 1) + "]");
        int start = (int) (from - snapshotIndex - 1);
        return List.copyOf(entries.subList(start, Math.min(entries.size(), start + max)));
    }

    /**
     * Moves the base to (index, term) (§7, Figure 13 receiver steps 6-7, D1a rules 1 and 5):
     * if the log holds an entry at index with that term, the entries after it are kept (Log Matching
     * says they follow the same prefix); otherwise the whole log is discarded.
     * @return true if a suffix was kept
     */
    boolean resetBase(long index, long term) {
        if (index < snapshotIndex)
            throw new IllegalStateException("base would move backwards: " + index + " < " + snapshotIndex);
        boolean keep = termAt(index).orElse(-1) == term;
        if (keep) {
            entries.subList(0, (int) (index - snapshotIndex)).clear();
        } else {
            entries.clear();
        }
        snapshotIndex = index;
        snapshotTerm = term;
        return keep;
    }

    /** Fast backup (follower side): the first index of the run of entries with term termAt(index). */
    long firstIndexOfTermAt(long index) {
        long term = termAt(index).orElseThrow();
        long i = index;
        while (i > snapshotIndex + 1 && termAt(i - 1).getAsLong() == term) i--;
        return i;
    }

    /** Fast backup (leader side): the last index holding {@code term}, or 0 if the log has none. */
    long lastIndexOfTerm(long term) {
        for (long i = lastIndex(); i >= Math.max(1, snapshotIndex); i--) {
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
