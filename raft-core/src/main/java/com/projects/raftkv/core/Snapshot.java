package com.projects.raftkv.core;

import java.util.Objects;

/**
 * A state machine snapshot (§7, Figure 12): the state after applying every entry up to and including
 * {@code index}, whose term is {@code term}. {@code data} is opaque to the core (the state machine's
 * canonical encoding); it holds committed state only.
 */
public record Snapshot(long index, long term, Bytes data) {
    public Snapshot {
        if (index < 1) throw new IllegalArgumentException("snapshot index must be >= 1: " + index);
        if (term < 1) throw new IllegalArgumentException("snapshot term must be >= 1: " + term);
        Objects.requireNonNull(data);
    }

    @Override public String toString() { return "Snapshot{index=" + index + ", term=" + term + ", " + data + "}"; }
}
