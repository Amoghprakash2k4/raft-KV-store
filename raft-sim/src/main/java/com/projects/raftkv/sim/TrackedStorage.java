package com.projects.raftkv.sim;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.storage.Storage;
import java.util.List;

/**
 * Passes every call to the node's real storage (the WAL), and tells the checkers about it: the
 * NodeLogTracker hashes every log write, and the DurabilityChecker's shadow model learns each write,
 * each saved snapshot and each fsync that returned successfully.
 */
final class TrackedStorage implements Storage {
    private final Storage inner;
    private final NodeLogTracker tracker;
    private final DurabilityChecker durability;
    private final NodeId id;
    private long snapshotsSaved;

    TrackedStorage(Storage inner, NodeLogTracker tracker, DurabilityChecker durability, NodeId id) {
        this.inner = inner;
        this.tracker = tracker;
        this.durability = durability;
        this.id = id;
    }

    @Override public void saveHardState(HardState state) {
        inner.saveHardState(state);
        durability.onSaveHardState(id, state);
    }

    @Override public void append(List<LogEntry> entries) {
        inner.append(entries);
        tracker.onAppend(entries);
        durability.onAppend(id, entries);
    }

    @Override public void truncateFrom(long fromIndex) {
        inner.truncateFrom(fromIndex);
        tracker.onTruncate(fromIndex);
        durability.onTruncate(id, fromIndex);
    }

    @Override public void saveSnapshot(Snapshot snapshot) {
        inner.saveSnapshot(snapshot);           // durable when it returns; throws otherwise
        durability.onSnapshotSaved(id, snapshot);
        snapshotsSaved++;
    }

    @Override public void compactLog(long index, long term) {
        inner.compactLog(index, term);
        tracker.onRebase(index, term);
        durability.onCompactLog(id, index, term);
    }

    @Override public void sync() {
        inner.sync();                 // throws on failure: then nothing new is durable
        durability.onSync(id);
    }

    @Override public PersistentState loadAll() { return inner.loadAll(); }

    long snapshotsSaved() { return snapshotsSaved; }
}
