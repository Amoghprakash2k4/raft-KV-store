package com.projects.raftkv.sim;

import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongUnaryOperator;

/**
 * The simulator's incremental view of one node's log: a prefix hash for every index, where
 * hash(i) = mix(hash(i-1), entry i). Two logs with equal hash(i) hold identical entries 1..i
 * (up to 64-bit collisions), so the checkers compare whole prefixes in O(1).
 *
 * <p>Fed by {@link TrackedStorage} with every log write, and computed at write time from the entries
 * written, because compaction later removes them from the node's log (M3). Hashes at and below a
 * snapshot base are kept across restarts: those entries are committed and never change. When an
 * installed snapshot replaces the log, the hashes up to its index come from the committed-entries
 * registry, since a snapshot holds only committed entries.
 */
final class NodeLogTracker {

    /** What changed since the last refresh: entries from {@code fromIndex} on are new or rewritten. */
    record Change(long fromIndex, boolean truncated) {}

    private final LongUnaryOperator committedHash;
    private long[] hashes = new long[256];   // hashes[i - 1] = prefix hash through index i
    private long[] terms = new long[256];
    private long size;
    private long dirtyFrom = Long.MAX_VALUE;
    private boolean truncated;

    /** @param committedHash prefix hash of a committed index, from the Leader Completeness registry */
    NodeLogTracker(LongUnaryOperator committedHash) { this.committedHash = committedHash; }

    /** A tracker for a log that is never compacted (unit tests). */
    static NodeLogTracker of(LogView log) {
        NodeLogTracker t = new NodeLogTracker(i -> { throw new IllegalStateException("no registry"); });
        for (long i = 1; i <= log.lastIndex(); i++) t.onAppend(List.of(log.entryAt(i).orElseThrow()));
        t.refresh(log);
        return t;
    }

    void onAppend(List<LogEntry> entries) {
        for (LogEntry e : entries) {
            if (e.index() != size + 1) throw new IllegalStateException("tracker: non-contiguous append " + e + " after " + size);
            set(e.index(), chain(e.index() == 1 ? 0 : hashes[(int) e.index() - 2], e), e.term());
            size = e.index();
            dirtyFrom = Math.min(dirtyFrom, e.index());
        }
    }

    void onTruncate(long fromIndex) {
        size = fromIndex - 1;
        dirtyFrom = Math.min(dirtyFrom, fromIndex);
        truncated = true;
    }

    /**
     * Storage.compactLog: if this log holds (index, term), nothing changes (Log Matching). Otherwise an
     * installed snapshot replaced the log: entries 1..index become the committed ones.
     */
    void onRebase(long index, long term) {
        if (index <= size && terms[(int) index - 1] == term) return;
        for (long i = 1; i <= index; i++) set(i, committedHash.applyAsLong(i), i == index ? term : -1);
        size = index;
        dirtyFrom = 1;
        truncated = true;
    }

    /** A restarted node: hashes up to its snapshot are kept; the rest is rebuilt from the recovered log. */
    void restart(PersistentState recovered) {
        long base = recovered.snapshot().map(Snapshot::index).orElse(0L);
        if (base > size) throw new IllegalStateException("tracker: recovered snapshot " + base + " beyond tracked log " + size);
        size = base;
        onAppend(recovered.entries());
        dirtyFrom = 1;
        truncated = false;
    }

    Change refresh(LogView log) {
        if (log.lastIndex() != size)
            throw new IllegalStateException("tracker out of sync: tracked " + size + ", log has " + log.lastIndex());
        Change c = new Change(Math.min(dirtyFrom, size + 1), truncated);
        dirtyFrom = Long.MAX_VALUE;
        truncated = false;
        return c;
    }

    long lastIndex() { return size; }

    long hashAt(long index) {
        if (index < 1 || index > size) throw new IllegalArgumentException("index " + index + " outside [1, " + size + "]");
        return hashes[(int) index - 1];
    }

    private void set(long index, long hash, long term) {
        if (index > hashes.length) {
            int n = (int) Math.max(index, 2L * hashes.length);
            hashes = Arrays.copyOf(hashes, n);
            terms = Arrays.copyOf(terms, n);
        }
        hashes[(int) index - 1] = hash;
        terms[(int) index - 1] = term;
    }

    static long chain(long prev, LogEntry e) {
        long payload = switch (e.payload()) {
            case EntryPayload.Noop n -> 0x6e6f6f70L;
            case EntryPayload.Command c -> c.data().hashCode();
        };
        return mix(mix(prev * 0x9E3779B97F4A7C15L + e.term()) ^ payload);
    }

    /** SplitMix64 finalizer: spreads every input bit over the output. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
