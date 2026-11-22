package com.projects.raftkv.sim;

import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Invariant 5, State Machine Safety (§5.4.3): if a server has applied the entry at an index, no other
 * server ever applies a different entry at that index. The first entry applied at each index is
 * recorded; every later application, on any node and after any restart, must be the same entry.
 */
public final class StateMachineSafetyChecker {
    private record Applied(LogEntry entry, NodeId node, long atMs) {}

    private final List<Applied> appliedByIndex = new ArrayList<>();   // position i - 1 = index i
    private final Map<NodeId, Long> lastAppliedSeen = new HashMap<>();

    void observe(long now, RaftStatus s, LogView log) {
        long prev = lastAppliedSeen.getOrDefault(s.id(), 0L);
        long current = s.lastApplied();
        lastAppliedSeen.put(s.id(), current);
        // After a restart lastApplied is back at 0 and counts up again; every re-application is checked too.
        if (current < prev) return;
        for (long i = prev + 1; i <= current; i++) {
            LogEntry e = log.entryAt(i).orElseThrow(() -> new InvariantViolation(
                    "State Machine Safety: " + s.id() + " applied index beyond its log"));
            if (i > appliedByIndex.size()) {
                appliedByIndex.add(new Applied(e, s.id(), now));
                continue;
            }
            Applied first = appliedByIndex.get((int) i - 1);
            if (!first.entry().equals(e)) {
                throw new InvariantViolation("State Machine Safety violated at t=" + now + "ms: " + s.id()
                        + " applied " + e + " at index " + i + ", but " + first.node() + " applied "
                        + first.entry() + " there at t=" + first.atMs() + "ms");
            }
        }
    }

    public long appliedEntries() { return appliedByIndex.size(); }
}
