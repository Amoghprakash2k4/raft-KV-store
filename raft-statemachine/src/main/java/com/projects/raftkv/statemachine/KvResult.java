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

    /** The command bytes could not be decoded. Deterministic, so every replica answers the same. */
    record Invalid(String reason) implements KvResult {
        public Invalid { Objects.requireNonNull(reason); }
    }
}
