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
     * Figure 2 AppendEntries results, plus matchIndex (= prevLogIndex + entries.size() on success).
     * Echoing matchIndex lets the leader apply max(old, matchIndex), which is safe under
     * reordered/duplicated replies. M2 adds conflictTerm/conflictIndex for fast backup.
     */
    record AppendEntriesResponse(NodeId from, NodeId to, long term, boolean success, long matchIndex)
            implements Message {}
}
