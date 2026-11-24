package com.projects.raftkv.storage;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Storage for unit tests. Keeps a "working" image (written, not yet synced) and a "durable" one.
 * {@link #crash()} discards the working image, modelling a power loss with only fsynced data on disk.
 * Snapshots are durable as soon as they are saved, as {@link Storage#saveSnapshot} requires.
 * Fault hooks: {@link #failNextSync()} and {@link #setFsyncIsNoop(boolean)} (a disk whose fsync lies).
 */
public final class InMemoryStorage implements Storage {

    private LogImage working = new LogImage();
    private final LogImage durable = new LogImage();
    // Writes since the last sync, replayed onto the durable image at sync: O(changes), not O(log).
    private final List<Consumer<LogImage>> unsynced = new ArrayList<>();
    private Optional<Snapshot> snapshot = Optional.empty();

    private boolean failNextSync;
    private boolean fsyncIsNoop;

    @Override
    public void saveHardState(HardState state) {
        Objects.requireNonNull(state);
        record(img -> img.saveHardState(state));
    }

    @Override
    public void append(List<LogEntry> entries) {
        List<LogEntry> copy = List.copyOf(entries);
        record(img -> img.append(copy));
    }

    @Override
    public void truncateFrom(long fromIndex) {
        record(img -> img.truncateFrom(fromIndex));
    }

    @Override
    public void saveSnapshot(Snapshot s) {
        snapshot = Optional.of(Objects.requireNonNull(s));
    }

    @Override
    public void compactLog(long index, long term) {
        record(img -> img.rebase(index, term));
    }

    /** Applies the write to the working image now (so invalid writes fail at once) and queues it for sync. */
    private void record(Consumer<LogImage> write) {
        write.accept(working);
        unsynced.add(write);
    }

    @Override
    public void sync() {
        if (failNextSync) {
            failNextSync = false;
            throw new StorageException("injected fsync failure");
        }
        if (fsyncIsNoop) return;
        for (Consumer<LogImage> w : unsynced) w.accept(durable);
        unsynced.clear();
    }

    @Override
    public PersistentState loadAll() {
        LogImage img = durable.copy();
        try {
            img.alignWith(snapshot);
        } catch (IllegalStateException e) {
            throw new StorageException("inconsistent storage: " + e.getMessage(), e);
        }
        return img.toPersistentState(snapshot);
    }

    /** Power loss: everything not fsynced is gone. */
    public void crash() {
        durable.alignWith(snapshot);
        working = durable.copy();
        unsynced.clear();
    }

    public void failNextSync() { failNextSync = true; }

    /** Simulates a disk with a volatile write cache that acknowledges fsync without persisting. */
    public void setFsyncIsNoop(boolean noop) { fsyncIsNoop = noop; }
}
