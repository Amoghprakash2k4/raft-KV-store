package com.projects.raftkv.storage;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import java.util.List;

/**
 * Durable Raft state. Writes are buffered until {@link #sync()}; only synced data survives a crash.
 * M3 adds snapshot operations and the file WAL implementation.
 */
public interface Storage {

    void saveHardState(HardState state);

    /** Appends entries; the first must have index lastIndex + 1. */
    void append(List<LogEntry> entries);

    /** Deletes entries with index >= fromIndex. */
    void truncateFrom(long fromIndex);

    /**
     * Makes all preceding writes durable (fsync).
     * @throws StorageException on failure; the caller must treat the node as dead (DESIGN.md E1a).
     */
    void sync();

    /** Durable state as of the last successful sync. Called once, on (re)start. */
    PersistentState loadAll();
}
