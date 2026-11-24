package com.projects.raftkv.core;

import java.util.List;
import java.util.Objects;

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

    /**
     * Local compaction (§7): the snapshot through (index, term) is already durable; drop the log entries
     * up to index. Storage must never apply this before the snapshot it depends on is on disk.
     */
    record CompactLog(long index, long term) implements PersistOp {
        public CompactLog {
            if (index < 1 || term < 1) throw new IllegalArgumentException("bad compaction point " + index + "/" + term);
        }
    }

    /**
     * Figure 13 receiver, steps 5-7: save the leader's snapshot durably, then move the log base to it,
     * keeping the suffix after (index, term) if the log holds that entry, else discarding the log.
     */
    record InstallSnapshot(Snapshot snapshot) implements PersistOp {
        public InstallSnapshot { Objects.requireNonNull(snapshot); }
    }
}
