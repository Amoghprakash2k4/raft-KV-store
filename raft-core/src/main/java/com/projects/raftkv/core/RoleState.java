package com.projects.raftkv.core;

import java.util.ArrayList;
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
        /** A snapshot being streamed to one follower, one chunk in flight (DESIGN.md M3-D). */
        record Transfer(long index, long term, long offset) {}

        /** A ReadIndex read with its read index, released once a majority has echoed {@code round}. */
        record PendingRead(long readId, long readIndex, long round) {}

        private final Map<NodeId, Progress> progress = new LinkedHashMap<>();
        private final Map<NodeId, Transfer> transfers = new LinkedHashMap<>();

        // ReadIndex state (thesis §6.4, DESIGN.md M4-B). Volatile and per term, like progress.
        /** The last read round started; every AppendEntries/InstallSnapshot carries it. */
        long readRound;
        /** The highest round each peer has echoed in this term (max, so reordered replies are harmless). */
        private final Map<NodeId, Long> ackedRound = new LinkedHashMap<>();
        /** Reads not yet in a round: waiting for a current-term commit, or for the round in flight. */
        final List<Long> waitingReads = new ArrayList<>();
        /** Reads with a read index, waiting for a majority to echo their round. In arrival order. */
        final List<PendingRead> readsInRound = new ArrayList<>();

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

        Optional<Transfer> transfer(NodeId peer) { return Optional.ofNullable(transfers.get(peer)); }

        void setTransfer(NodeId peer, Transfer t) { transfers.put(peer, t); }

        void ackRound(NodeId peer, long round) {
            progress(peer);
            ackedRound.merge(peer, round, Math::max);
        }

        /**
         * The highest round that a majority (the leader counts itself, at {@link #readRound}) has echoed:
         * every read with a round at or below it is confirmed.
         */
        long roundAckedByMajority(int quorum) {
            List<Long> rounds = new ArrayList<>();
            rounds.add(readRound);
            for (NodeId p : progress.keySet()) rounds.add(ackedRound.getOrDefault(p, 0L));
            rounds.sort(java.util.Comparator.reverseOrder());
            return rounds.get(quorum - 1);
        }

        void endTransfer(NodeId peer) { transfers.remove(peer); }

        @Override public Role role() { return Role.LEADER; }

        @Override public String toString() { return "Leader" + progress; }
    }
}
