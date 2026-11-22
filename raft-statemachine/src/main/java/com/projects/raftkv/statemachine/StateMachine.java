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

    /** The index of the last command applied (0 if none). */
    long lastAppliedIndex();
}
