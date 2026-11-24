package com.projects.raftkv.core;

/**
 * Deliberate protocol bugs for mutation testing (DESIGN.md M2-C). A core built with
 * {@link RaftNodeCore#withMutationsForTesting} breaks exactly the named rules; the simulator must then
 * report a safety violation. Production code never enables these: the public constructor uses none.
 */
public enum ProtocolMutation {
    /** Grant votes without the election restriction (§5.4.1): breaks Leader Completeness. */
    SKIP_UP_TO_DATE_CHECK,
    /** Commit an entry from an older term by counting replicas: the Figure 8 bug (§5.4.2). */
    COMMIT_OLD_TERM_BY_COUNT,
    /** On AppendEntries, delete everything after prevLogIndex even when it matches (§5.3, step 3). */
    TRUNCATE_ON_MATCH,
    /**
     * Install every complete InstallSnapshot: no stale check (lastIncludedIndex <= commitIndex) and always
     * discard the log (Figure 13 steps 6-7, D1a rule 5). Rolls a follower back to an older state.
     */
    NAIVE_INSTALL_SNAPSHOT,
    /**
     * ReadIndex without step 3 (thesis §6.4): answer a read at commitIndex without confirming leadership
     * with a majority. A deposed leader that has not heard of the new term serves stale reads.
     */
    READ_WITHOUT_QUORUM_CHECK,
    /**
     * ReadIndex without step 1 (thesis §6.4): take readIndex = commitIndex before this leader has committed
     * an entry of its own term. A new leader's commitIndex can lag behind writes already acknowledged.
     */
    READ_BEFORE_CURRENT_TERM_COMMIT
}
