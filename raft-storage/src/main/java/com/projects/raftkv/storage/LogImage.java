package com.projects.raftkv.storage;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Storage's own model of the persistent state: hard state and the log after a base (index, term).
 * Replaying a WAL builds one; the WAL also keeps one current, to write checkpoints. Every method throws
 * {@link IllegalStateException} on a write that cannot follow the current state.
 */
public final class LogImage {
    private HardState hardState = HardState.INITIAL;
    private long baseIndex;
    private long baseTerm;
    private final List<LogEntry> entries = new ArrayList<>();

    public HardState hardState() { return hardState; }
    public long baseIndex() { return baseIndex; }
    public long baseTerm() { return baseTerm; }
    public long lastIndex() { return baseIndex + entries.size(); }
    public List<LogEntry> entries() { return List.copyOf(entries); }

    public void saveHardState(HardState hs) { hardState = hs; }

    public void append(List<LogEntry> es) {
        long expected = lastIndex() + 1;
        for (LogEntry e : es) {
            if (e.index() != expected)
                throw new IllegalStateException("non-contiguous append: expected " + expected + ", got " + e);
            expected++;
        }
        entries.addAll(es);
    }

    public void truncateFrom(long from) {
        if (from <= baseIndex || from > lastIndex() + 1)
            throw new IllegalStateException("truncateFrom " + from + " outside [" + (baseIndex + 1) + ", " + (lastIndex() + 1) + "]");
        entries.subList((int) (from - baseIndex - 1), entries.size()).clear();
    }

    /** The base moves to (index, term); the suffix after a matching entry is kept, otherwise all is dropped. */
    public void rebase(long index, long term) {
        if (index < baseIndex)
            throw new IllegalStateException("log base moves backwards: " + index + " < " + baseIndex);
        boolean keep = termAt(index) == term;
        if (keep) entries.subList(0, (int) (index - baseIndex)).clear();
        else entries.clear();
        baseIndex = index;
        baseTerm = term;
    }

    private long termAt(long index) {
        if (index == baseIndex) return baseTerm;
        if (index < baseIndex || index > lastIndex()) return -1;
        return entries.get((int) (index - baseIndex - 1)).term();
    }

    /**
     * Combines this log with the stored snapshot. The snapshot must cover the base: a base beyond the
     * snapshot means the log was compacted before its snapshot was durable, and the compacted entries
     * are gone. A snapshot beyond the base (saved, then a crash before the compaction record was
     * synced) rebases this image onto it.
     * @return true if the image had to be rebased onto the snapshot
     */
    public boolean alignWith(Optional<Snapshot> snapshot) {
        long si = snapshot.map(Snapshot::index).orElse(0L);
        if (si < baseIndex)
            throw new IllegalStateException("the log is compacted through index " + baseIndex
                    + " but the snapshot only covers " + si + ": entries " + (si + 1) + ".." + baseIndex + " are lost");
        if (si == baseIndex) {
            if (si > 0 && snapshot.get().term() != baseTerm)
                throw new IllegalStateException("snapshot term " + snapshot.get().term() + " differs from the log base term " + baseTerm);
            return false;
        }
        rebase(si, snapshot.get().term());
        return true;
    }

    public PersistentState toPersistentState(Optional<Snapshot> snapshot) {
        return new PersistentState(hardState, snapshot, entries);
    }

    public LogImage copy() {
        LogImage c = new LogImage();
        c.hardState = hardState;
        c.baseIndex = baseIndex;
        c.baseTerm = baseTerm;
        c.entries.addAll(entries);
        return c;
    }
}
