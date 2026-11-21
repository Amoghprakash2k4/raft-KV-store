package com.projects.raftkv.core;

import java.util.Objects;

/** Every input to the core. The core is a function of (state, event). */
public sealed interface Event {
    /** One unit of logical time. Timeouts are counted in ticks; the core never reads a clock. */
    record Tick() implements Event {}

    record MessageReceived(Message message) implements Event {
        public MessageReceived { Objects.requireNonNull(message); }
    }

    /** proposalId is chosen by the driver and echoed in ClientNotice so it can correlate. */
    record ProposeCommand(long proposalId, Bytes command) implements Event {
        public ProposeCommand { Objects.requireNonNull(command); }
    }

    /** Start a ReadIndex read (thesis §6.4). Answered by ReadReady or ReadRejected. */
    record ReadRequested(long readId) implements Event {}
}
