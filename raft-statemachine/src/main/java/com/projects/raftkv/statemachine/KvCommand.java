package com.projects.raftkv.statemachine;

import java.util.Objects;
import java.util.Optional;

/** Operations of the key-value store. All of them go through the log in M2 (reads too). */
public sealed interface KvCommand {
    String key();

    /** Set key to value. Result: {@link KvResult.Value} with the previous value. */
    record Put(String key, String value) implements KvCommand {
        public Put { Objects.requireNonNull(key); Objects.requireNonNull(value); }
    }

    /** Read key. Result: {@link KvResult.Value} with the current value. (M4 adds ReadIndex reads.) */
    record Get(String key) implements KvCommand {
        public Get { Objects.requireNonNull(key); }
    }

    /** Remove key. Result: {@link KvResult.Value} with the previous value. */
    record Delete(String key) implements KvCommand {
        public Delete { Objects.requireNonNull(key); }
    }

    /**
     * Compare-and-set: if the current value equals {@code expected} (empty = absent), set it to
     * {@code newValue}. Result: {@link KvResult.CasOutcome} with the value before the operation.
     */
    record Cas(String key, Optional<String> expected, String newValue) implements KvCommand {
        public Cas { Objects.requireNonNull(key); Objects.requireNonNull(expected); Objects.requireNonNull(newValue); }
    }
}
