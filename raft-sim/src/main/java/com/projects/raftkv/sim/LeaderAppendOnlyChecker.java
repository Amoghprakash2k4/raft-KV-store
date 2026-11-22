package com.projects.raftkv.sim;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import java.util.HashMap;
import java.util.Map;

/**
 * Invariant 2, Leader Append-Only (§5.3, Figure 3): a leader never overwrites or deletes entries in its
 * log; it only appends. Checked between consecutive observations of the same node while it stays leader
 * of the same term.
 */
public final class LeaderAppendOnlyChecker {
    private record Last(long term, long lastIndex) {}

    private final Map<NodeId, Last> leaders = new HashMap<>();

    void observe(long now, RaftStatus s, long lastIndex, boolean truncatedSinceLast) {
        if (s.soft().role() != Role.LEADER) {
            leaders.remove(s.id());
            return;
        }
        Last prev = leaders.put(s.id(), new Last(s.soft().term(), lastIndex));
        if (prev == null || prev.term() != s.soft().term()) return;   // newly elected: nothing to compare
        if (truncatedSinceLast || lastIndex < prev.lastIndex()) {
            throw new InvariantViolation("Leader Append-Only violated at t=" + now + "ms: " + s.id()
                    + ", leader of term " + s.soft().term() + ", deleted entries from its log (last index "
                    + prev.lastIndex() + " -> " + lastIndex + ")");
        }
    }
}
