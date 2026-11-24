package com.projects.raftkv.statemachine;

import com.projects.raftkv.core.Bytes;

/**
 * The replicated application. The driver calls {@link #apply} for each committed command entry, in
 * strictly increasing index order, exactly once per incarnation (Raft no-ops are skipped, so indexes
 * may have gaps).
 *
 * <p>Must be deterministic (DESIGN.md E1a rule 4): the result and the new state may depend only on the
 * current state and the command bytes. No clock, no randomness, no local configuration, no I/O. That
 * is what turns "same log" into "same state" on every replica (State Machine Safety, §5.4.3).
 */
public interface StateMachine {

    /** Applies the command committed at {@code index} and returns the encoded result for the client. */
    Bytes apply(long index, Bytes command);

    /** The index of the last entry applied, or of the snapshot restored (0 if none). */
    long lastAppliedIndex();

    /**
     * The whole current state in a canonical encoding: equal states give byte-identical snapshots, so
     * a snapshot depends only on the applied prefix of the log (DESIGN.md E1a rule 4, M3-E).
     */
    Bytes snapshot();

    /**
     * Replaces the whole state with {@code snapshot}, taken after applying index {@code index}
     * (Figure 13 step 8, or recovery). The next apply must be for an index above {@code index}.
     * @throws IllegalArgumentException if the bytes are not a valid snapshot
     */
    void restore(long index, Bytes snapshot);
}
