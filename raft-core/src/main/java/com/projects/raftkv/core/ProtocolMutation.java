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
    READ_BEFORE_CURRENT_TERM_COMMIT,
    /**
     * PreVote that raises (and persists) the pre-candidate's term on every attempt, as a real election
     * would (thesis §9.6). An isolated node inflates its term and deposes a healthy leader when it rejoins.
     * Liveness, not safety: caught by the disruption scenario, not by the invariant checkers.
     */
    PREVOTE_BUMPS_TERM,
    /**
     * PreVote granted without the up-to-date check (§5.4.1). A teaching control: it must cause NO safety
     * violation, because the real RequestVote that follows still checks the log.
     */
    PREVOTE_SKIPS_LOG_CHECK,
    /**
     * Driver, not core (E1, DESIGN.md M6-A): send each step's messages before the fsync that makes their
     * state durable. A node that crashes in that fsync has promised votes or entries it no longer has.
     */
    SEND_BEFORE_SYNC
}
