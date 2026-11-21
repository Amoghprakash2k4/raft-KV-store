package com.projects.raftkv.sim;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import java.util.HashMap;
import java.util.Map;

/**
 * The per-node facts Election Safety rests on, checked across crashes (§5.1, §5.2, Figure 2):
 * currentTerm never decreases, and once votedFor is set in a term it never changes in that term.
 * Observations happen after the driver has fsynced, so a violation means durable state was lost.
 */
public final class VoteDurabilityChecker {
    private final Map<NodeId, HardState> lastSeen = new HashMap<>();

    public void observe(long now, RaftStatus s) {
        HardState current = s.hard();
        HardState prev = lastSeen.put(s.id(), current);
        if (prev == null) return;
        if (current.currentTerm() < prev.currentTerm()) {
            throw new InvariantViolation("Vote durability violated at t=" + now + "ms: " + s.id()
                    + " term went backwards " + prev.currentTerm() + " -> " + current.currentTerm());
        }
        if (current.currentTerm() == prev.currentTerm() && prev.votedFor().isPresent()
                && !prev.votedFor().equals(current.votedFor())) {
            throw new InvariantViolation("Vote durability violated at t=" + now + "ms: " + s.id()
                    + " changed vote in term " + current.currentTerm() + " from " + prev.votedFor().get()
                    + " to " + current.votedFor().map(NodeId::toString).orElse("none"));
        }
    }
}
