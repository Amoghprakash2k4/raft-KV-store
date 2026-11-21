package com.projects.raftkv.storage;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Storage for simulation. Keeps a "working" copy (written, not yet synced) and a "durable" copy.
 * {@link #crash()} discards the working copy, modelling a power loss with only fsynced data on disk.
 * Fault hooks: {@link #failNextSync()} and {@link #setFsyncIsNoop(boolean)} (a disk whose fsync lies).
 */
public final class InMemoryStorage implements Storage {

    private HardState workingHardState = HardState.INITIAL;
    private final List<LogEntry> workingEntries = new ArrayList<>();

    private HardState durableHardState = HardState.INITIAL;
    private List<LogEntry> durableEntries = List.of();

    private boolean failNextSync;
    private boolean fsyncIsNoop;

    @Override
    public void saveHardState(HardState state) {
        workingHardState = Objects.requireNonNull(state);
    }

    @Override
    public void append(List<LogEntry> entries) {
        for (LogEntry e : entries) {
            long expected = workingEntries.size() + 1L;
            if (e.index() != expected)
                throw new IllegalStateException("non-contiguous append: expected " + expected + ", got " + e);
            workingEntries.add(e);
        }
    }

    @Override
    public void truncateFrom(long fromIndex) {
        if (fromIndex < 1 || fromIndex > workingEntries.size() + 1L)
            throw new IllegalStateException("truncateFrom " + fromIndex + " outside [1, " + (workingEntries.size() + 1) + "]");
        workingEntries.subList((int) (fromIndex - 1), workingEntries.size()).clear();
    }

    @Override
    public void sync() {
        if (failNextSync) {
            failNextSync = false;
            throw new StorageException("injected fsync failure");
        }
        if (fsyncIsNoop) return;
        durableHardState = workingHardState;
        durableEntries = List.copyOf(workingEntries);
    }

    @Override
    public PersistentState loadAll() {
        return new PersistentState(durableHardState, durableEntries);
    }

    /** Power loss: everything not fsynced is gone. */
    public void crash() {
        workingHardState = durableHardState;
        workingEntries.clear();
        workingEntries.addAll(durableEntries);
    }

    public void failNextSync() { failNextSync = true; }

    /** Simulates a disk with a volatile write cache that acknowledges fsync without persisting. */
    public void setFsyncIsNoop(boolean noop) { fsyncIsNoop = noop; }
}
