package com.projects.raftkv.sim;

import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import java.util.HashMap;
import java.util.Map;

/**
 * Invariant 5, State Machine Safety (§5.4.3): if a server has applied the entry at an index, no other
 * server ever applies a different entry at that index. The first entry applied at each index is
 * recorded; every later application, on any node and after any restart, must be the same entry.
 */
public final class StateMachineSafetyChecker {
    private record Applied(LogEntry entry, NodeId node, long atMs) {}

    // By index, not by position: a node restored from a snapshot starts applying above it, so indexes
    // reach the checker with gaps.
    private final Map<Long, Applied> appliedByIndex = new HashMap<>();
    private final Map<NodeId, Long> lastAppliedSeen = new HashMap<>();

    /** A restarted node rebuilds its state from its snapshot (or from scratch) and counts up again. */
    void onRestart(NodeId node) { lastAppliedSeen.remove(node); }

    void observe(long now, RaftStatus s, LogView log) {
        long prev = lastAppliedSeen.getOrDefault(s.id(), 0L);
        long current = s.lastApplied();
        lastAppliedSeen.put(s.id(), current);
        // Within one incarnation the applied state only moves forward: going back would un-apply
        // committed commands that clients may already have seen (a corollary of §5.4.3).
        if (current < prev) {
            throw new InvariantViolation("State Machine Safety violated at t=" + now + "ms: " + s.id()
                    + " rolled its applied state back from index " + prev + " to " + current + " without restarting");
        }
        // Every re-application after a restart is checked too. Indexes covered by a snapshot were not
        // applied entry by entry here: the Replica state checker compares the restored state instead.
        for (long i = Math.max(prev, log.snapshotIndex()) + 1; i <= current; i++) {
            LogEntry e = log.entryAt(i).orElseThrow(() -> new InvariantViolation(
                    "State Machine Safety: " + s.id() + " applied index beyond its log"));
            Applied first = appliedByIndex.putIfAbsent(i, new Applied(e, s.id(), now));
            if (first == null) continue;
            if (!first.entry().equals(e)) {
                throw new InvariantViolation("State Machine Safety violated at t=" + now + "ms: " + s.id()
                        + " applied " + e + " at index " + i + ", but " + first.node() + " applied "
                        + first.entry() + " there at t=" + first.atMs() + "ms");
            }
        }
    }

    public long appliedEntries() { return appliedByIndex.size(); }
}
