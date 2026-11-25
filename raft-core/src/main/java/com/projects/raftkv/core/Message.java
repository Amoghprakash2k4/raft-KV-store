package com.projects.raftkv.core;

import java.util.List;
import java.util.Objects;

/** Peer-to-peer RPCs. Requests and responses are separate one-way messages (no RPC futures in the core). */
public sealed interface Message {
    NodeId from();
    NodeId to();
    long term();

    /** Figure 2 RequestVote arguments. candidateId == from. */
    record RequestVote(NodeId from, NodeId to, long term, long lastLogIndex, long lastLogTerm)
            implements Message {}

    /** Figure 2 RequestVote results. */
    record RequestVoteResponse(NodeId from, NodeId to, long term, boolean voteGranted)
            implements Message {}

    /**
     * PreVote (thesis §9.6): "would you vote for me in {@code term}?", where term = the sender's
     * currentTerm + 1. Neither side changes its term or vote because of it: a receiver never adopts this
     * term, and the sender only raises its own once a majority has answered yes.
     */
    record PreVote(NodeId from, NodeId to, long term, long lastLogIndex, long lastLogTerm) implements Message {}

    /**
     * The answer to a {@link PreVote}. A yes carries the proposed term (so the sender can match it to
     * its current round); a no carries the receiver's currentTerm, so a sender that is behind catches up.
     */
    record PreVoteResponse(NodeId from, NodeId to, long term, boolean granted) implements Message {}

    /**
     * Figure 2 AppendEntries arguments. leaderId == from. Empty entries == heartbeat.
     * {@code readRound} (thesis §6.4, DESIGN.md M4-B): the leader's ReadIndex round when it sent this;
     * the reply echoes it, which proves the follower still accepted this leader's term after the round
     * started. 0 = no read round yet.
     */
    record AppendEntries(NodeId from, NodeId to, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit, long readRound) implements Message {
        public AppendEntries {
            entries = List.copyOf(entries);
            if (readRound < 0) throw new IllegalArgumentException("negative readRound");
        }

        /** Without a read round (tests that do not involve reads). */
        public AppendEntries(NodeId from, NodeId to, long term, long prevLogIndex, long prevLogTerm,
                             List<LogEntry> entries, long leaderCommit) {
            this(from, to, term, prevLogIndex, prevLogTerm, entries, leaderCommit, 0);
        }
    }

    /**
     * Figure 2 AppendEntries results, plus two fields that make the leader's bookkeeping safe under
     * reordered and duplicated replies:
     * <ul>
     *   <li>{@code matchIndex}: on success, prevLogIndex + entries.size(), i.e. the last index this
     *       message proved identical to the leader's log. The leader applies max(old, matchIndex).</li>
     *   <li>{@code rejectedIndex}: on failure, the prevLogIndex that did not match. The leader acts on
     *       a rejection only if it answers its latest probe (rejectedIndex == nextIndex - 1).</li>
     *   <li>{@code conflictTerm}, {@code conflictIndex}: on failure, the fast log-backup hint (thesis
     *       §5.3). If the follower's log is shorter than prevLogIndex: conflictTerm = 0 and conflictIndex
     *       = its lastIndex + 1. Otherwise conflictTerm = the term it holds at prevLogIndex and
     *       conflictIndex = the first index it holds with that term.</li>
     * </ul>
     *   <li>{@code readRound}: echoed from the request, success or not (M4-B), but only when the request
     *       is from the reply's term. A rejection of an older term's request echoes 0: the reply carries
     *       the follower's newer term, and an echoed old round would pass for an ack in that term.</li>
     * Fields that do not apply are 0.
     */
    record AppendEntriesResponse(NodeId from, NodeId to, long term, boolean success, long matchIndex,
                                 long rejectedIndex, long conflictTerm, long conflictIndex, long readRound)
            implements Message {
        public AppendEntriesResponse {
            if (matchIndex < 0 || rejectedIndex < 0 || conflictTerm < 0 || conflictIndex < 0 || readRound < 0)
                throw new IllegalArgumentException("negative index or term");
        }

        /** Without a read round (tests that do not involve reads). */
        public AppendEntriesResponse(NodeId from, NodeId to, long term, boolean success, long matchIndex,
                                     long rejectedIndex, long conflictTerm, long conflictIndex) {
            this(from, to, term, success, matchIndex, rejectedIndex, conflictTerm, conflictIndex, 0);
        }
    }

    /**
     * Figure 13 InstallSnapshot arguments (§7): one chunk of the leader's snapshot through
     * (lastIncludedIndex, lastIncludedTerm), starting at byte {@code offset}. leaderId == from.
     */
    record InstallSnapshot(NodeId from, NodeId to, long term, long lastIncludedIndex, long lastIncludedTerm,
                           long offset, Bytes data, boolean done, long readRound) implements Message {
        public InstallSnapshot {
            Objects.requireNonNull(data);
            if (lastIncludedIndex < 1 || lastIncludedTerm < 1 || offset < 0 || readRound < 0)
                throw new IllegalArgumentException("bad InstallSnapshot header");
        }

        /** Without a read round (tests that do not involve reads). */
        public InstallSnapshot(NodeId from, NodeId to, long term, long lastIncludedIndex, long lastIncludedTerm,
                               long offset, Bytes data, boolean done) {
            this(from, to, term, lastIncludedIndex, lastIncludedTerm, offset, data, done, 0);
        }
    }

    /**
     * Figure 13 InstallSnapshot results (just {@code term} in the paper), plus what the leader needs
     * with one chunk in flight (DESIGN.md M3-D): which snapshot this answers, how many bytes of it the
     * follower holds ({@code nextOffset}: send from there), and {@code done} once it is installed, or
     * was not needed because the follower had already committed past it. {@code readRound} is echoed
     * from the request (M4-B), except in a rejection of an older term's request (0, see above).
     */
    record InstallSnapshotResponse(NodeId from, NodeId to, long term, long lastIncludedIndex, long nextOffset,
                                   boolean done, long readRound) implements Message {
        public InstallSnapshotResponse {
            if (lastIncludedIndex < 0 || nextOffset < 0 || readRound < 0)
                throw new IllegalArgumentException("negative field");
        }

        /** Without a read round (tests that do not involve reads). */
        public InstallSnapshotResponse(NodeId from, NodeId to, long term, long lastIncludedIndex, long nextOffset,
                                       boolean done) {
            this(from, to, term, lastIncludedIndex, nextOffset, done, 0);
        }
    }
}
