package com.projects.raftkv.statemachine;

import java.util.Objects;
import java.util.Optional;

/** What applying a {@link KvRequest} returns to the client. */
public sealed interface KvResult {

    /** For Put/Delete: the previous value. For Get: the current value. */
    record Value(Optional<String> value) implements KvResult {
        public Value { Objects.requireNonNull(value); }
    }

    /** For Cas: whether it swapped, and the value before the operation. */
    record CasOutcome(boolean swapped, Optional<String> previous) implements KvResult {
        public CasOutcome { Objects.requireNonNull(previous); }
    }

    /**
     * The request's seq is older than the last one this client's session executed (thesis §6.3). It is a
     * delayed copy of a request the client has already moved past; nothing was executed. With one
     * request in flight per client nobody is waiting for this reply.
     */
    record Stale(long lastSeq) implements KvResult {}

    /** The command bytes could not be decoded. Deterministic, so every replica answers the same. */
    record Invalid(String reason) implements KvResult {
        public Invalid { Objects.requireNonNull(reason); }
    }
}
