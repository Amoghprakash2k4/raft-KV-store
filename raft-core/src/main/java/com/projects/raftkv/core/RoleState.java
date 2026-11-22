package com.projects.raftkv.core;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    /**
     * Leader with per-peer replication progress (Figure 2: nextIndex[], matchIndex[]). Progress is
     * reinitialized after each election and changes only when replies arrive (DESIGN.md M2-A).
     */
    final class Leader implements RoleState {
        private final Map<NodeId, Progress> progress = new LinkedHashMap<>();

        /** Figure 2: nextIndex = leader last log index + 1 (before the no-op), matchIndex = 0. */
        Leader(List<NodeId> peers, long nextIndex) {
            for (NodeId p : peers) progress.put(p, new Progress(nextIndex, 0));
        }

        Progress progress(NodeId peer) {
            Progress p = progress.get(peer);
            if (p == null) throw new IllegalArgumentException("not a peer: " + peer);
            return p;
        }

        void update(NodeId peer, Progress p) {
            progress(peer);
            progress.put(peer, p);
        }

        Iterable<Progress> all() { return progress.values(); }

        @Override public Role role() { return Role.LEADER; }

        @Override public String toString() { return "Leader" + progress; }
    }
}
