package com.projects.raftkv.core;

import java.util.Optional;
import java.util.OptionalLong;

/** Read-only view of the core's in-memory log, for invariant checkers and tests. */
public interface LogView {
    long lastIndex();
    long lastTerm();
    /** Term at index; 0 for index 0; empty if index is out of range. */
    OptionalLong termAt(long index);
    Optional<LogEntry> entryAt(long index);
}
