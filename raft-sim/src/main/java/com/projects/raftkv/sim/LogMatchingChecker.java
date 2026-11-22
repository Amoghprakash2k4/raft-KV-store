package com.projects.raftkv.sim;

import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import java.util.HashMap;
import java.util.Map;

/**
 * Invariant 3, Log Matching (§5.3): if two logs contain an entry with the same index and term, the logs
 * are identical in all entries up through that index. Checked over the whole run: every (index, term)
 * any node ever holds is registered with its prefix hash, and any later holder must match it.
 */
public final class LogMatchingChecker {
    private record Key(long index, long term) {}
    private record Seen(long prefixHash, NodeId node, long atMs) {}

    private final Map<Key, Seen> seen = new HashMap<>();

    /** Registers this node's entries from {@code fromIndex} to its last index (the ones that changed). */
    void observe(long now, NodeId node, LogView log, NodeLogTracker tracker, long fromIndex) {
        for (long i = fromIndex; i <= tracker.lastIndex(); i++) {
            Key k = new Key(i, log.termAt(i).getAsLong());
            long h = tracker.hashAt(i);
            Seen first = seen.putIfAbsent(k, new Seen(h, node, now));
            if (first != null && first.prefixHash() != h) {
                throw new InvariantViolation("Log Matching violated at t=" + now + "ms: " + node + " and "
                        + first.node() + " (seen at t=" + first.atMs() + "ms) both hold index " + i + " term "
                        + k.term() + " but their logs differ at or before that index");
            }
        }
    }
}
