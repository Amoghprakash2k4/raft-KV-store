package com.projects.raftkv.sim;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import java.util.HashMap;
import java.util.Map;

/**
 * Invariant 1, Election Safety (§5.2, Figure 3): at most one leader per term, checked over the entire
 * history of the run (a leader that already stepped down still owns its term).
 */
public final class ElectionSafetyChecker {
    private final Map<Long, NodeId> leaderByTerm = new HashMap<>();

    public void observe(long now, RaftStatus s) {
        if (s.soft().role() != Role.LEADER) return;
        long term = s.soft().term();
        NodeId previous = leaderByTerm.putIfAbsent(term, s.id());
        if (previous != null && !previous.equals(s.id())) {
            throw new InvariantViolation("Election Safety violated at t=" + now + "ms: term " + term
                    + " has leaders " + previous + " and " + s.id());
        }
    }

    public int leadersElected() { return leaderByTerm.size(); }
}
