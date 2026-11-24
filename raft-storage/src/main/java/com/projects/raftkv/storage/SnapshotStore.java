package com.projects.raftkv.storage;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.storage.disk.Disk;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * The latest snapshot, in one file replaced atomically (DESIGN.md M3-A):
 * <ol>
 *   <li>write {@code snapshot.snap.tmp};</li>
 *   <li>fsync it, so its bytes are on disk before its name can point at them;</li>
 *   <li>rename it over {@code snapshot.snap} (atomic);</li>
 *   <li>fsync the directory, so the rename itself is durable.</li>
 * </ol>
 * A crash anywhere leaves the old or the new snapshot. File layout:
 * {@code magic "RSNP", version u8, index u64, term u64, length u32, data, crc32c u32 (of all before it)}.
 */
public final class SnapshotStore {
    public static final String FILE = "snapshot.snap";
    static final String TMP = FILE + ".tmp";
    private static final int MAGIC = 0x52534e50;   // "RSNP"
    private static final byte VERSION = 1;
    private static final int HEADER = 4 + 1 + 8 + 8 + 4;

    private final Disk disk;
    private final boolean skipDataFsync;

    /** @param skipDataFsync MUTATION (test only): StorageMutation.SKIP_SNAPSHOT_DATA_FSYNC */
    public SnapshotStore(Disk disk, boolean skipDataFsync) {
        this.disk = disk;
        this.skipDataFsync = skipDataFsync;
    }

    public void save(Snapshot s) { saveSteps(s, Integer.MAX_VALUE); }

    /** Test hook: performs only the first {@code steps} of the save (0 = none, 4 = all). */
    void saveSteps(Snapshot s, int steps) {
        if (disk.exists(TMP)) disk.delete(TMP);
        if (steps < 1) return;
        disk.append(TMP, encode(s));
        if (steps < 2) return;
        if (!skipDataFsync) disk.fsync(TMP);              // MUTATION (test only) when skipped
        if (steps < 3) return;
        disk.rename(TMP, FILE);
        if (steps < 4) return;
        disk.fsyncDir();
    }

    /** The stored snapshot; a leftover temp file from an interrupted save is removed. */
    public Optional<Snapshot> load() {
        if (disk.exists(TMP)) disk.delete(TMP);
        if (!disk.exists(FILE)) return Optional.empty();
        return Optional.of(decode(disk.read(FILE)));
    }

    static byte[] encode(Snapshot s) {
        byte[] data = s.data().toByteArray();
        ByteBuffer b = ByteBuffer.allocate(HEADER + data.length + 4);
        b.putInt(MAGIC).put(VERSION).putLong(s.index()).putLong(s.term()).putInt(data.length).put(data);
        CRC32C crc = new CRC32C();
        crc.update(b.array(), 0, b.position());
        b.putInt((int) crc.getValue());
        return b.array();
    }

    static Snapshot decode(byte[] bytes) {
        if (bytes.length < HEADER + 4) throw corrupt("too short (" + bytes.length + " bytes)");
        ByteBuffer b = ByteBuffer.wrap(bytes);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        if ((int) crc.getValue() != b.getInt(bytes.length - 4)) throw corrupt("bad checksum");
        if (b.getInt() != MAGIC || b.get() != VERSION) throw corrupt("bad header");
        long index = b.getLong();
        long term = b.getLong();
        int len = b.getInt();
        if (len != bytes.length - HEADER - 4) throw corrupt("bad length " + len);
        byte[] data = new byte[len];
        b.get(data);
        try {
            return new Snapshot(index, term, Bytes.copyOf(data));
        } catch (IllegalArgumentException e) {
            throw corrupt(e.getMessage());
        }
    }

    private static StorageException corrupt(String why) {
        return new StorageException("corrupt snapshot file " + FILE + ": " + why);
    }
}
