package com.projects.raftkv.server;

import com.projects.raftkv.storage.disk.Disk;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

/** A {@link Disk} that reports how long each fsync takes (the brief's "fsync latency" metric). */
final class TimedDisk implements Disk {
    private final Disk disk;
    private final LongConsumer fsyncNanos;

    TimedDisk(Disk disk, LongConsumer fsyncNanos) {
        this.disk = Objects.requireNonNull(disk);
        this.fsyncNanos = Objects.requireNonNull(fsyncNanos);
    }

    @Override public List<String> list() { return disk.list(); }
    @Override public boolean exists(String name) { return disk.exists(name); }
    @Override public long size(String name) { return disk.size(name); }
    @Override public byte[] read(String name) { return disk.read(name); }
    @Override public void append(String name, byte[] data) { disk.append(name, data); }
    @Override public void truncate(String name, long size) { disk.truncate(name, size); }
    @Override public void rename(String from, String to) { disk.rename(from, to); }
    @Override public void delete(String name) { disk.delete(name); }
    @Override public void close() { disk.close(); }

    @Override
    public void fsync(String name) {
        long start = System.nanoTime();
        disk.fsync(name);
        fsyncNanos.accept(System.nanoTime() - start);
    }

    @Override
    public void fsyncDir() {
        long start = System.nanoTime();
        disk.fsyncDir();
        fsyncNanos.accept(System.nanoTime() - start);
    }
}
