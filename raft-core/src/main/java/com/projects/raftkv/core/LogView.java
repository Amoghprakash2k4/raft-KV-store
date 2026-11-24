package com.projects.raftkv.core;

import java.util.Optional;
import java.util.OptionalLong;

/** Read-only view of the core's in-memory log, for invariant checkers and tests. */
public interface LogView {
    long lastIndex();
    long lastTerm();
    /**
     * Term at index. The snapshot boundary has a term ({@link #snapshotTerm()}; 0 at index 0 when there
     * is no snapshot). Empty if the index is beyond the log or compacted into the snapshot (D1a rule 2).
     */
    OptionalLong termAt(long index);
    /** The entry at index; empty at or below the snapshot boundary and beyond the log. */
    Optional<LogEntry> entryAt(long index);
    /** Last index covered by the snapshot (0 if none). Entries up to here have been compacted away. */
    default long snapshotIndex() { return 0; }
    default long snapshotTerm() { return 0; }
}
