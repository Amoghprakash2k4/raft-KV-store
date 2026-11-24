package com.projects.raftkv.storage.disk;

import com.projects.raftkv.storage.StorageException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;

/**
 * An in-memory {@link Disk} that models a page cache, so the simulator can crash a node at any point
 * and run the real WAL recovery code on what "the platter" holds.
 * <p>
 * Crash model ({@link #crash()}):
 * <ul>
 *   <li>The directory reverts to its state at the last {@link #fsyncDir}: unsynced creates, renames
 *       and deletes are undone.</li>
 *   <li>A file's synced bytes never change.</li>
 *   <li>If the file was only appended to since its last fsync, a random prefix of the appended bytes
 *       survives: none, all, or part (a torn write). Part of the surviving region may hold zeros or
 *       garbage, as when a file system has extended the file size but not written all its blocks.</li>
 *   <li>If it was truncated since its last fsync, the truncate and everything after it are lost.</li>
 * </ul>
 * All randomness comes from the given seeded generator, so a crash is reproducible.
 */
public final class SimDisk implements Disk {

    /** One file's contents, independent of its name(s). */
    private static final class Inode {
        byte[] buf = new byte[64];
        int len;
        int durableLen;
        /** null while the durable bytes are buf[0, durableLen); a separate copy after a truncate below it. */
        byte[] durableCopy;

        void append(byte[] data) {
            if (len + data.length > buf.length) buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + data.length));
            System.arraycopy(data, 0, buf, len, data.length);
            len += data.length;
        }

        void truncate(int size) {
            if (size >= len) return;
            if (durableCopy == null && size < durableLen) durableCopy = Arrays.copyOf(buf, durableLen);
            len = size;
        }

        void sync() {
            durableCopy = null;
            durableLen = len;
        }
    }

    private final SplittableRandom rng;
    private Map<String, Inode> dir = new TreeMap<>();
    private Map<String, Inode> durableDir = new TreeMap<>();
    private int fsyncsBeforeFailure = -1;   // -1: no failure armed
    private boolean fsyncIsNoop;
    private long fsyncs;
    private long bytesWritten;

    public SimDisk(SplittableRandom rng) { this.rng = rng; }

    @Override public List<String> list() { return new ArrayList<>(dir.keySet()); }

    @Override public boolean exists(String name) { return dir.containsKey(name); }

    @Override public long size(String name) { return inode(name).len; }

    @Override public byte[] read(String name) {
        Inode f = inode(name);
        return Arrays.copyOf(f.buf, f.len);
    }

    @Override public void append(String name, byte[] data) {
        dir.computeIfAbsent(name, n -> new Inode()).append(data);
        bytesWritten += data.length;
    }

    @Override public void truncate(String name, long size) { inode(name).truncate((int) size); }

    @Override public void fsync(String name) {
        Inode f = inode(name);
        checkFsync();
        if (!fsyncIsNoop) f.sync();
    }

    @Override public void rename(String from, String to) {
        Inode f = inode(from);
        dir.remove(from);
        dir.put(to, f);
    }

    @Override public void delete(String name) { dir.remove(name); }

    @Override public void fsyncDir() {
        checkFsync();
        if (!fsyncIsNoop) durableDir = new TreeMap<>(dir);
    }

    @Override public void close() {}

    /** Power loss. Afterwards the disk holds only what survived, and can be used again. */
    public void crash() {
        dir = new TreeMap<>(durableDir);
        for (Inode f : dir.values()) {
            if (f.durableCopy != null) {
                f.buf = f.durableCopy;
                f.len = f.durableLen;
            } else {
                f.len = f.durableLen + survivingBytes(f.len - f.durableLen);
                scribble(f);
            }
            f.sync();
        }
        fsyncsBeforeFailure = -1;
    }

    /** The next fsync or fsyncDir throws, without making anything durable. */
    public void failNextFsync() { failFsyncAfter(0); }

    /** The next {@code n} fsyncs (file or directory) succeed and the one after them throws. */
    public void failFsyncAfter(int n) {
        if (n < 0) throw new IllegalArgumentException("n < 0");
        fsyncsBeforeFailure = n;
    }

    public void clearFailNextFsync() { fsyncsBeforeFailure = -1; }

    /** A disk with a volatile write cache that acknowledges fsync without persisting anything. */
    public void setFsyncIsNoop(boolean noop) { fsyncIsNoop = noop; }

    public long fsyncs() { return fsyncs; }

    public long bytesWritten() { return bytesWritten; }

    /** Total bytes currently stored, over all files. */
    public long totalBytes() { return dir.values().stream().mapToLong(f -> f.len).sum(); }

    private void checkFsync() {
        fsyncs++;
        if (fsyncsBeforeFailure == 0) {
            fsyncsBeforeFailure = -1;
            throw new StorageException("injected fsync failure");
        }
        if (fsyncsBeforeFailure > 0) fsyncsBeforeFailure--;
    }

    private int survivingBytes(int unsynced) {
        if (unsynced == 0) return 0;
        return switch (rng.nextInt(3)) {
            case 0 -> 0;
            case 1 -> unsynced;
            default -> rng.nextInt(unsynced + 1);
        };
    }

    /** Sometimes fills part of the surviving unsynced region with zeros or garbage. */
    private void scribble(Inode f) {
        int from = f.durableLen;
        int n = f.len - from;
        if (n == 0 || rng.nextInt(4) != 0) return;
        int start = from + rng.nextInt(n);
        int end = start + 1 + rng.nextInt(f.len - start);
        boolean zeros = rng.nextBoolean();
        for (int i = start; i < end; i++) f.buf[i] = zeros ? 0 : (byte) rng.nextInt(256);
    }

    private Inode inode(String name) {
        Inode f = dir.get(name);
        if (f == null) throw new StorageException("no such file: " + name);
        return f;
    }
}
