package com.projects.raftkv.storage;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import java.util.List;

/**
 * Durable Raft state: hard state, the log after its snapshot base, and the latest snapshot.
 * Log and hard-state writes are buffered until {@link #sync()}; only synced data survives a crash.
 */
public interface Storage {

    void saveHardState(HardState state);

    /** Appends entries; the first must have index lastIndex + 1. */
    void append(List<LogEntry> entries);

    /** Deletes entries with index >= fromIndex (never at or below the snapshot base). */
    void truncateFrom(long fromIndex);

    /**
     * Atomically replaces the stored snapshot. Durable when this returns (it syncs by itself), so a
     * {@link #compactLog} that depends on it can follow. After a crash, the old or the new snapshot is
     * there, never a mix.
     */
    void saveSnapshot(Snapshot snapshot);

    /**
     * Moves the log base to (index, term): if the log holds that entry, entries up to it are dropped and
     * the rest kept; otherwise the whole log is dropped (Figure 13, DESIGN.md D1a). Durable at the next
     * sync. The snapshot covering index must already have been saved.
     */
    void compactLog(long index, long term);

    /**
     * Makes all preceding writes durable (fsync).
     * @throws StorageException on failure; the caller must treat the node as dead (DESIGN.md E1a).
     */
    void sync();

    /**
     * Durable state as of the last successful sync. Called once, on (re)start. A snapshot newer than
     * the log's base (saved just before a crash) wins: the log is rebased onto it.
     */
    PersistentState loadAll();
}
