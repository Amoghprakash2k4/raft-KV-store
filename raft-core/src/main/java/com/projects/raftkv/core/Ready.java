package com.projects.raftkv.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Effects of one step. The driver MUST execute them in this order:
 * <ol>
 *   <li>{@code persist}: all ops in order, then one fsync. Nothing below may happen first.</li>
 *   <li>{@code messages}: hand to the transport.</li>
 *   <li>{@code toApply}: apply to the state machine in index order.</li>
 *   <li>{@code notices}: complete client futures (reads only after lastApplied >= readIndex).</li>
 * </ol>
 * The core has already updated its in-memory state as if all of this happened. That is safe
 * only because the driver is single-threaded and won't feed the next event until this Ready
 * is fully handled (or, under group commit, until the fsync covering it completes).
 */
public record Ready(List<PersistOp> persist,
                    List<Message> messages,
                    List<LogEntry> toApply,
                    List<ClientNotice> notices,
                    Optional<SoftState> softState) {

    public static final Ready EMPTY =
            new Ready(List.of(), List.of(), List.of(), List.of(), Optional.empty());

    public Ready {
        persist = List.copyOf(persist);
        messages = List.copyOf(messages);
        toApply = List.copyOf(toApply);
        notices = List.copyOf(notices);
        Objects.requireNonNull(softState);
    }

    public boolean mustSync() { return !persist.isEmpty(); }

    public boolean isEmpty() {
        return persist.isEmpty() && messages.isEmpty() && toApply.isEmpty()
                && notices.isEmpty() && softState.isEmpty();
    }
}
