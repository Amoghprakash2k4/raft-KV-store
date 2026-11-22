package com.projects.raftkv.sim;

import com.projects.raftkv.core.NodeId;
import java.util.HashMap;
import java.util.Map;

/**
 * End-to-end check of the apply path: two replicas that have applied the same prefix (equal
 * lastApplied) must hold identical KV state. Catches nondeterminism in the state machine or the driver
 * (applying out of order, twice, or skipping entries), which State Machine Safety on the log cannot see.
 */
public final class ReplicaStateChecker {
    private record Seen(long stateHash, NodeId node) {}

    private final Map<Long, Seen> byLastApplied = new HashMap<>();

    void observe(long now, NodeId node, long lastApplied, long stateHash) {
        Seen first = byLastApplied.putIfAbsent(lastApplied, new Seen(stateHash, node));
        if (first != null && first.stateHash() != stateHash) {
            throw new InvariantViolation("Replica state divergence at t=" + now + "ms: " + node + " and "
                    + first.node() + " both applied up to index " + lastApplied + " but their KV states differ");
        }
    }
}
