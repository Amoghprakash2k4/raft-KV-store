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
    TRUNCATE_ON_MATCH
}
