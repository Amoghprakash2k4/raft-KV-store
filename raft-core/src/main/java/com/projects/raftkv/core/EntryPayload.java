package com.projects.raftkv.core;

/** What a log entry carries. The core never interprets command bytes. */
public sealed interface EntryPayload {
    /** Appended by a new leader in its own term (Raft paper §5.4.2, §8; thesis §6.4). */
    record Noop() implements EntryPayload {}

    /** Opaque client command; decoded only by the StateMachine. */
    record Command(Bytes data) implements EntryPayload {
        public Command { java.util.Objects.requireNonNull(data); }
    }
}
