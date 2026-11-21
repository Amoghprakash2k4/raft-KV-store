package com.projects.raftkv.core;

import java.util.List;

/**
 * Durable writes, executed IN LIST ORDER, then fsynced once, before anything else in the Ready.
 * Ordered ops (not separate fields) so several Readys can be concatenated for group commit.
 */
public sealed interface PersistOp {
    record SaveHardState(HardState state) implements PersistOp {}

    /** Delete log entries with index >= fromIndex. Emitted only on a real term conflict (§5.3). */
    record TruncateSuffix(long fromIndex) implements PersistOp {
        public TruncateSuffix {
            if (fromIndex < 1) throw new IllegalArgumentException("fromIndex < 1");
        }
    }

    record AppendEntries(List<LogEntry> entries) implements PersistOp {
        public AppendEntries {
            entries = List.copyOf(entries);
            if (entries.isEmpty()) throw new IllegalArgumentException("empty append");
        }
    }
}
