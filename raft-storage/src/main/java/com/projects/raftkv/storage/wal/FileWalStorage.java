package com.projects.raftkv.storage.wal;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.storage.LogImage;
import com.projects.raftkv.storage.SnapshotStore;
import com.projects.raftkv.storage.Storage;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.storage.StorageMutation;
import com.projects.raftkv.storage.disk.Disk;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link Storage} as a segmented write-ahead log plus a snapshot file on a {@link Disk} (DESIGN.md M3-A).
 * <p>
 * <b>Writes.</b> Every write appends one record to the current segment {@code wal-<seq>.log};
 * {@link #sync()} is one fsync. A segment is closed only right after a successful sync, so every segment
 * except the last is fully durable. That is what lets recovery tell the two kinds of bad bytes apart:
 * <ul>
 *   <li>bad bytes at the end of the <b>last</b> segment are a torn write that was never acknowledged:
 *       cut them off, fsync, carry on;</li>
 *   <li>bad bytes anywhere else were once synced: that is corruption, and the node must stop rather
 *       than guess (fail-stop).</li>
 * </ul>
 * A new segment's name is made durable (fsyncDir) in the same sync as its first records.
 * <p>
 * <b>Compaction.</b> {@link #compactLog} appends a SNAPSHOT_MARK record. Once the WAL has grown past
 * one segment, the next sync writes a <i>checkpoint</i>: a new segment holding RESET, the hard state,
 * the mark and the retained entries, written as a temp file, fsynced, renamed, directory fsynced; only
 * then are the older segments deleted. A crash at any step leaves either the old segments (complete) or
 * old segments plus a complete checkpoint, which replays to the same state.
 */
public final class FileWalStorage implements Storage {

    static final String PREFIX = "wal-";
    static final String SUFFIX = ".log";
    private static final int ENTRIES_PER_CHECKPOINT_RECORD = 1_000;

    private final Disk disk;
    private final WalOptions options;
    private final SnapshotStore snapshots;
    private final PersistentState recovered;
    /** Everything written so far (synced or not): the source for checkpoints and write validation. */
    private LogImage image;

    private long seq;
    private String current;          // null until the first write on an empty disk
    private long currentSize;
    private long olderSegmentsBytes;
    private boolean rollBeforeNextWrite;
    private boolean dirNeedsSync;
    private boolean compactedSinceCheckpoint;
    private long tornBytesCut;
    private long checkpoints;

    private FileWalStorage(Disk disk, WalOptions options) {
        this.disk = Objects.requireNonNull(disk);
        this.options = Objects.requireNonNull(options);
        this.snapshots = new SnapshotStore(disk, options.mutations().contains(StorageMutation.SKIP_SNAPSHOT_DATA_FSYNC));
        this.recovered = recover();
    }

    /** Opens the WAL in {@code disk}, recovering whatever a previous incarnation left there. */
    public static FileWalStorage open(Disk disk, WalOptions options) {
        return new FileWalStorage(disk, options);
    }

    // ---------------------------------------------------------------- recovery

    private PersistentState recover() {
        for (String name : disk.list()) {
            if (name.startsWith(PREFIX) && name.endsWith(SUFFIX + ".tmp")) disk.delete(name);   // unfinished checkpoint
        }
        List<String> segments = disk.list().stream().filter(FileWalStorage::isSegment).toList();
        image = new LogImage();
        for (int i = 0; i < segments.size(); i++) {
            String name = segments.get(i);
            byte[] bytes = disk.read(name);
            WalCodec.Scan scan = WalCodec.scan(bytes, name);
            boolean last = i == segments.size() - 1;
            if (!scan.clean()) {
                if (!last) {
                    throw new StorageException("corrupt WAL: bad record in " + name + " at offset "
                            + scan.validBytes() + ", which is not the last segment, so it was once synced");
                }
                tornBytesCut = bytes.length - scan.validBytes();
                if (!options.mutations().contains(StorageMutation.SKIP_TORN_TAIL_CUT)) {   // MUTATION (test only)
                    disk.truncate(name, scan.validBytes());
                    disk.fsync(name);
                }
            }
            for (WalRecord r : scan.records()) {
                try {
                    replay(r);
                } catch (IllegalStateException e) {
                    throw new StorageException("corrupt WAL: " + name + ": " + e.getMessage(), e);
                }
            }
            seq = sequenceOf(name);
            current = name;
            currentSize = options.mutations().contains(StorageMutation.SKIP_TORN_TAIL_CUT) ? bytes.length : scan.validBytes();
            if (!last) olderSegmentsBytes += bytes.length;
        }
        rollBeforeNextWrite = current != null && currentSize >= options.maxSegmentBytes();

        Optional<Snapshot> snapshot = snapshots.load();
        boolean rebased;
        try {
            rebased = image.alignWith(snapshot);
        } catch (IllegalStateException e) {
            throw new StorageException("corrupt WAL: " + e.getMessage(), e);
        }
        if (rebased) {
            // The snapshot was saved but the record compacting the log was not synced before the crash.
            // Write it now, so the entries that follow in the WAL continue from the snapshot.
            write(new WalRecord.SnapshotMark(snapshot.get().index(), snapshot.get().term()));
            sync();
        }
        try {
            return image.toPersistentState(snapshot);
        } catch (IllegalArgumentException e) {
            throw new StorageException("corrupt WAL: recovered state is invalid: " + e.getMessage(), e);
        }
    }

    private void replay(WalRecord r) {
        switch (r) {
            case WalRecord.SaveHardState(HardState hs) -> image.saveHardState(hs);
            case WalRecord.Entries(List<LogEntry> entries) -> image.append(entries);
            case WalRecord.Truncate(long from) -> image.truncateFrom(from);
            case WalRecord.SnapshotMark(long index, long term) -> image.rebase(index, term);
            case WalRecord.Reset x -> image = new LogImage();
        }
    }

    // ---------------------------------------------------------------- Storage

    @Override
    public void saveHardState(HardState state) {
        Objects.requireNonNull(state);
        image.saveHardState(state);
        write(new WalRecord.SaveHardState(state));
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (entries.isEmpty()) return;
        image.append(entries);
        write(new WalRecord.Entries(entries));
    }

    @Override
    public void truncateFrom(long fromIndex) {
        image.truncateFrom(fromIndex);
        write(new WalRecord.Truncate(fromIndex));
    }

    @Override
    public void saveSnapshot(Snapshot snapshot) {
        snapshots.save(snapshot);
    }

    @Override
    public void compactLog(long index, long term) {
        image.rebase(index, term);
        write(new WalRecord.SnapshotMark(index, term));
        compactedSinceCheckpoint = true;
    }

    @Override
    public void sync() {
        if (current == null) return;
        disk.fsync(current);
        if (dirNeedsSync) {
            disk.fsyncDir();
            dirNeedsSync = false;
        }
        if (currentSize >= options.maxSegmentBytes()) rollBeforeNextWrite = true;
        if (compactedSinceCheckpoint && olderSegmentsBytes + currentSize > options.maxSegmentBytes()) checkpoint();
    }

    /** The state recovered when this WAL was opened. */
    @Override
    public PersistentState loadAll() {
        return recovered;
    }

    /** Bytes of torn tail removed during recovery (0 if the WAL ended cleanly). */
    public long tornBytesCut() { return tornBytesCut; }

    /** Checkpoints written since this WAL was opened. */
    public long checkpoints() { return checkpoints; }

    public int segmentCount() { return (int) disk.list().stream().filter(FileWalStorage::isSegment).count(); }

    // ---------------------------------------------------------------- internals

    private void write(WalRecord record) {
        if (current == null || rollBeforeNextWrite) {
            if (current != null) olderSegmentsBytes += currentSize;
            seq++;
            current = segmentName(seq);
            currentSize = 0;
            rollBeforeNextWrite = false;
            dirNeedsSync = true;
        }
        byte[] frame = WalCodec.encode(record);
        disk.append(current, frame);
        currentSize += frame.length;
    }

    /** Called right after a successful sync, so everything in {@link #image} is durable. */
    private void checkpoint() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.writeBytes(WalCodec.encode(new WalRecord.Reset()));
        buf.writeBytes(WalCodec.encode(new WalRecord.SaveHardState(image.hardState())));
        buf.writeBytes(WalCodec.encode(new WalRecord.SnapshotMark(image.baseIndex(), image.baseTerm())));
        List<LogEntry> retained = image.entries();
        for (int i = 0; i < retained.size(); i += ENTRIES_PER_CHECKPOINT_RECORD) {
            buf.writeBytes(WalCodec.encode(new WalRecord.Entries(
                    retained.subList(i, Math.min(retained.size(), i + ENTRIES_PER_CHECKPOINT_RECORD)))));
        }
        byte[] bytes = buf.toByteArray();
        List<String> old = disk.list().stream().filter(FileWalStorage::isSegment).toList();

        String name = segmentName(seq + 1);
        String tmp = name + ".tmp";
        disk.append(tmp, bytes);
        disk.fsync(tmp);
        disk.rename(tmp, name);
        disk.fsyncDir();                 // the checkpoint now exists durably
        for (String o : old) disk.delete(o);
        disk.fsyncDir();                 // the old segments are durably gone

        seq++;
        current = name;
        currentSize = bytes.length;
        olderSegmentsBytes = 0;
        rollBeforeNextWrite = currentSize >= options.maxSegmentBytes();
        compactedSinceCheckpoint = false;
        checkpoints++;
    }

    public static boolean isSegment(String name) {
        return name.startsWith(PREFIX) && name.endsWith(SUFFIX);
    }

    static String segmentName(long seq) {
        return PREFIX + String.format("%016d", seq) + SUFFIX;
    }

    private static long sequenceOf(String name) {
        try {
            return Long.parseLong(name.substring(PREFIX.length(), name.length() - SUFFIX.length()));
        } catch (NumberFormatException e) {
            throw new StorageException("unexpected WAL file name " + name, e);
        }
    }
}
