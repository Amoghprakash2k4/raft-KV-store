package com.projects.raftkv.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Figure 2 "Persistent state on all servers" minus the log: currentTerm and votedFor.
 * Must be durable before any message that depends on it leaves the node.
 */
public record HardState(long currentTerm, Optional<NodeId> votedFor) {
    public static final HardState INITIAL = new HardState(0, Optional.empty());

    public HardState {
        if (currentTerm < 0) throw new IllegalArgumentException("term < 0");
        Objects.requireNonNull(votedFor);
    }
}
