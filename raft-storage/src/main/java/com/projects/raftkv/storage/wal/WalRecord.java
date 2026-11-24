package com.projects.raftkv.storage.wal;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import java.util.List;

/** The logical records of the write-ahead log (DESIGN.md M3-A). Replaying them in order rebuilds the state. */
sealed interface WalRecord {
    /** currentTerm and votedFor (Figure 2 persistent state). */
    record SaveHardState(HardState state) implements WalRecord {}

    /** A contiguous batch continuing the log. */
    record Entries(List<LogEntry> entries) implements WalRecord {
        public Entries { entries = List.copyOf(entries); }
    }

    /** Entries with index >= fromIndex are deleted (§5.3). Logical: earlier bytes are not rewritten. */
    record Truncate(long fromIndex) implements WalRecord {}

    /** The log base moves to (index, term): Storage.compactLog. The snapshot covering it is already saved. */
    record SnapshotMark(long index, long term) implements WalRecord {}

    /** First record of a checkpoint segment: forget everything replayed so far; the checkpoint restates it. */
    record Reset() implements WalRecord {}
}
