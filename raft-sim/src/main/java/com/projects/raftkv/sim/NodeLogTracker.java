package com.projects.raftkv.sim;

import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import java.util.Arrays;

/**
 * The simulator's incremental view of one node's log: a prefix hash for every index, where
 * hash(i) = mix(hash(i-1), entry i). Two logs with equal hash(i) hold identical entries 1..i
 * (up to 64-bit collisions), so the checkers compare whole prefixes in O(1).
 *
 * <p>Fed by {@link TrackedStorage}: every log change the core makes reaches storage as a write, so the
 * tracker learns the first index that changed and recomputes only from there.
 */
final class NodeLogTracker {

    /** What changed since the last refresh: entries from {@code fromIndex} on are new or rewritten. */
    record Change(long fromIndex, boolean truncated) {}

    private long[] hashes = new long[256];   // hashes[i - 1] = prefix hash through index i
    private long size;
    private long dirtyFrom = Long.MAX_VALUE;
    private boolean truncated;

    void onAppend(long firstIndex) { dirtyFrom = Math.min(dirtyFrom, firstIndex); }

    void onTruncate(long fromIndex) {
        dirtyFrom = Math.min(dirtyFrom, fromIndex);
        truncated = true;
    }

    /** A (re)started node: its whole recovered log is new to us. */
    void reset() {
        size = 0;
        dirtyFrom = 1;
        truncated = false;
    }

    Change refresh(LogView log) {
        long last = log.lastIndex();
        long from = Math.min(Math.min(dirtyFrom, size + 1), last + 1);
        if (last > hashes.length) hashes = Arrays.copyOf(hashes, (int) Math.max(last, 2L * hashes.length));
        for (long i = from; i <= last; i++) {
            long prev = i == 1 ? 0 : hashes[(int) i - 2];
            hashes[(int) i - 1] = chain(prev, log.entryAt(i).orElseThrow());
        }
        size = last;
        Change c = new Change(from, truncated);
        dirtyFrom = Long.MAX_VALUE;
        truncated = false;
        return c;
    }

    long lastIndex() { return size; }

    long hashAt(long index) {
        if (index < 1 || index > size) throw new IllegalArgumentException("index " + index + " outside [1, " + size + "]");
        return hashes[(int) index - 1];
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
