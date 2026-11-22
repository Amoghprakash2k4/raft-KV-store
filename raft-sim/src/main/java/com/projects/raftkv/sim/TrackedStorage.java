package com.projects.raftkv.sim;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.storage.InMemoryStorage;
import com.projects.raftkv.storage.Storage;
import java.util.List;

/** Passes every call to the node's InMemoryStorage and tells its NodeLogTracker which indexes changed. */
final class TrackedStorage implements Storage {
    private final InMemoryStorage inner;
    private final NodeLogTracker tracker;

    TrackedStorage(InMemoryStorage inner, NodeLogTracker tracker) {
        this.inner = inner;
        this.tracker = tracker;
    }

    @Override public void saveHardState(HardState state) { inner.saveHardState(state); }

    @Override public void append(List<LogEntry> entries) {
        if (!entries.isEmpty()) tracker.onAppend(entries.get(0).index());
        inner.append(entries);
    }

    @Override public void truncateFrom(long fromIndex) {
        tracker.onTruncate(fromIndex);
        inner.truncateFrom(fromIndex);
    }

    @Override public void sync() { inner.sync(); }

    @Override public PersistentState loadAll() { return inner.loadAll(); }
}
