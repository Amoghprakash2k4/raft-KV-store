package com.projects.raftkv.core;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Static cluster configuration, in ticks.
 * Election timeout is drawn uniformly from [electionTimeoutTicks, 2*electionTimeoutTicks) (§5.2).
 */
public record RaftConfig(NodeId self,
                         List<NodeId> members,
                         int electionTimeoutTicks,
                         int heartbeatIntervalTicks,
                         int maxEntriesPerAppend) {
    public RaftConfig {
        Objects.requireNonNull(self);
        members = List.copyOf(members);
        if (new HashSet<>(members).size() != members.size())
            throw new IllegalArgumentException("duplicate member ids: " + members);
        if (!members.contains(self))
            throw new IllegalArgumentException(self + " not in members " + members);
        if (members.size() != 3 && members.size() != 5)
            throw new IllegalArgumentException("v1 supports 3 or 5 nodes, got " + members.size());
        if (heartbeatIntervalTicks < 1 || electionTimeoutTicks <= heartbeatIntervalTicks * 2)
            throw new IllegalArgumentException("need 1 <= heartbeat and election > 2*heartbeat");
        if (maxEntriesPerAppend < 1) throw new IllegalArgumentException("maxEntriesPerAppend < 1");
    }

    public int quorum() { return members.size() / 2 + 1; }

    public List<NodeId> peers() { return members.stream().filter(m -> !m.equals(self)).toList(); }
}
