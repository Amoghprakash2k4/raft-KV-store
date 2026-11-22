package com.projects.raftkv.core;

import java.util.List;

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

    /** Figure 2 AppendEntries arguments. leaderId == from. Empty entries == heartbeat. */
    record AppendEntries(NodeId from, NodeId to, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements Message {
        public AppendEntries { entries = List.copyOf(entries); }
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
     * Fields that do not apply are 0.
     */
    record AppendEntriesResponse(NodeId from, NodeId to, long term, boolean success, long matchIndex,
                                 long rejectedIndex, long conflictTerm, long conflictIndex) implements Message {
        public AppendEntriesResponse {
            if (matchIndex < 0 || rejectedIndex < 0 || conflictTerm < 0 || conflictIndex < 0)
                throw new IllegalArgumentException("negative index or term");
        }
    }
}
