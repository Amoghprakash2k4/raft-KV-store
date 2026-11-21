package com.projects.raftkv.core;

import java.util.Objects;

/** One Raft log entry (Figure 2, "log[]"). Indexes and terms start at 1; 0 means "none". */
public record LogEntry(long index, long term, EntryPayload payload) {
    public LogEntry {
        if (index < 1) throw new IllegalArgumentException("index must be >= 1: " + index);
        if (term < 1) throw new IllegalArgumentException("term must be >= 1: " + term);
        Objects.requireNonNull(payload);
    }
}
