package com.projects.raftkv.core;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Role-specific volatile state. Changing role replaces the whole object, so state from a previous
 * role (votes, leader hint, replication progress) cannot leak into the next one.
 */
sealed interface RoleState {

    Role role();

    /** Follower, possibly knowing the leader of the current term. */
    record Follower(Optional<NodeId> leader) implements RoleState {
        public Follower { Objects.requireNonNull(leader); }
        @Override public Role role() { return Role.FOLLOWER; }
    }

    /** Candidate with the set of nodes (including itself) that granted a vote in the current term. */
    record Candidate(Set<NodeId> votes) implements RoleState {
        public Candidate { votes = Set.copyOf(votes); }

        /** A Set makes duplicated vote responses idempotent. */
        Candidate withVote(NodeId voter) {
            Set<NodeId> next = new HashSet<>(votes);
            next.add(voter);
            return new Candidate(next);
        }

        @Override public Role role() { return Role.CANDIDATE; }
    }

    /** Leader. M2 adds per-peer replication progress (nextIndex, matchIndex). */
    record Leader() implements RoleState {
        @Override public Role role() { return Role.LEADER; }
    }
}
