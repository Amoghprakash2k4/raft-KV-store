package com.projects.raftkv.storage.wal;

import static com.projects.raftkv.storage.wal.FileWalStorageTest.cmd;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.storage.StorageMutation;
import com.projects.raftkv.storage.disk.RealDisk;
import com.projects.raftkv.storage.disk.SimDisk;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshots and log compaction in the WAL (DESIGN.md M3-A, D1a). */
class WalSnapshotTest {

    static final HardState T2 = new HardState(2, Optional.empty());

    static Snapshot snap(long index, long term, String contents) {
        return new Snapshot(index, term, Bytes.copyOf(contents.getBytes()));
    }

    static List<LogEntry> range(long from, long to, long term) {
        List<LogEntry> out = new ArrayList<>();
        for (long i = from; i <= to; i++) out.add(cmd(i, term, "v" + i));
        return out;
    }

    static FileWalStorage open(SimDisk d, long segmentBytes) {
        return FileWalStorage.open(d, new WalOptions(segmentBytes));
    }

    @Test
    void compactionDropsThePrefixAndRecoveryReturnsTheSnapshotAndTheSuffix() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        FileWalStorage s = open(d, 1 << 20);
        s.saveHardState(T2);
        s.append(range(1, 10, 2));
        s.sync();
        s.saveSnapshot(snap(6, 2, "state@6"));
        s.compactLog(6, 2);
        s.sync();
        d.crash();

        PersistentState p = open(d, 1 << 20).loadAll();
        assertThat(p.snapshot()).hasValue(snap(6, 2, "state@6"));
        assertThat(p.entries()).isEqualTo(range(7, 10, 2));
        assertThat(p.hardState()).isEqualTo(T2);
    }

    @Test
    void installingASnapshotWithoutAMatchingEntryDiscardsTheLog() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        FileWalStorage s = open(d, 1 << 20);
        s.saveHardState(new HardState(3, Optional.empty()));
        s.append(range(1, 4, 1));
        s.saveSnapshot(snap(8, 3, "leader's state@8"));
        s.compactLog(8, 3);
        s.append(range(9, 9, 3));
        s.sync();
        d.crash();

        PersistentState p = open(d, 1 << 20).loadAll();
        assertThat(p.snapshot().orElseThrow().index()).isEqualTo(8);
        assertThat(p.entries()).isEqualTo(range(9, 9, 3));
    }

    @Test
    void aSnapshotSavedButNotYetMarkedInTheWalStillWinsAtRecovery() {
        // Crash between saving the snapshot file and syncing the WAL record that compacts the log.
        SimDisk d = new SimDisk(new SplittableRandom(1));
        FileWalStorage s = open(d, 1 << 20);
        s.saveHardState(new HardState(3, Optional.empty()));
        s.append(range(1, 4, 1));
        s.sync();
        s.saveSnapshot(snap(8, 3, "installed"));
        d.crash();

        FileWalStorage r = open(d, 1 << 20);
        assertThat(r.loadAll().snapshot().orElseThrow().index()).isEqualTo(8);
        assertThat(r.loadAll().entries()).isEmpty();
        r.append(range(9, 10, 3));                 // later writes continue after the snapshot
        r.sync();
        d.crash();
        assertThat(open(d, 1 << 20).loadAll().entries()).isEqualTo(range(9, 10, 3));
    }

    @Test
    void aWalCompactedPastItsSnapshotIsCorruption() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        FileWalStorage s = open(d, 1 << 20);
        s.saveHardState(T2);
        s.append(range(1, 5, 2));
        s.compactLog(3, 2);                        // wrong order: no snapshot saved first
        s.sync();
        d.crash();
        assertThatThrownBy(() -> open(d, 1 << 20)).isInstanceOf(StorageException.class).hasMessageContaining("snapshot");
    }

    @Test
    void checkpointsDeleteOldSegmentsAndKeepTheWalSmall() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        FileWalStorage s = open(d, 400);
        s.saveHardState(T2);
        long index = 0;
        for (int round = 0; round < 30; round++) {
            for (int i = 0; i < 10; i++) {
                index++;
                s.append(List.of(cmd(index, 2, "value-" + index)));
                s.sync();
            }
            s.saveSnapshot(snap(index - 2, 2, "state@" + (index - 2)));
            s.compactLog(index - 2, 2);
            s.sync();
        }
        assertThat(s.checkpoints()).isPositive();
        assertThat(d.list().stream().filter(FileWalStorage::isSegment).count()).isLessThanOrEqualTo(3);
        assertThat(d.totalBytes()).isLessThan(2_000);
        d.crash();

        PersistentState p = open(d, 400).loadAll();
        assertThat(p.snapshot().orElseThrow().index()).isEqualTo(index - 2);
        assertThat(p.entries()).isEqualTo(List.of(cmd(index - 1, 2, "value-" + (index - 1)), cmd(index, 2, "value-" + index)));
    }

    @Test
    void aCrashAtAnyPointOfACheckpointRecoversTheSameState() {
        // A run that checkpoints often. Its last sync fails after k more successful fsyncs (k >= 1, so the
        // WAL segment itself is synced): the failure lands in the middle of the checkpoint that sync may
        // start (tmp written, fsynced, renamed, old segments deleted). Recovery must give the same state.
        int checkpointsCut = 0;
        for (long seed = 0; seed < 400; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = open(d, 300);
            s.saveHardState(T2);
            SplittableRandom rng = new SplittableRandom(seed);
            int rounds = 1 + rng.nextInt(60);
            Optional<Snapshot> snapshot = Optional.empty();
            long index = 0;
            boolean failed = false;
            for (int round = 0; round < rounds && !failed; round++) {
                index++;
                s.append(List.of(cmd(index, 2, "v" + index)));
                s.sync();
                if (index % 7 == 0) {
                    snapshot = Optional.of(snap(index, 2, "state@" + index));
                    s.saveSnapshot(snapshot.get());
                    s.compactLog(index, 2);
                    if (round == rounds - 1) d.failFsyncAfter(1 + rng.nextInt(4));
                    long before = s.checkpoints();
                    try {
                        s.sync();
                    } catch (StorageException e) {
                        failed = true;
                        if (s.checkpoints() == before) checkpointsCut++;
                    }
                }
            }
            d.clearFailNextFsync();
            d.crash();
            long base = snapshot.map(Snapshot::index).orElse(0L);
            PersistentState expected = new PersistentState(T2, snapshot, range(base + 1, index, 2).stream()
                    .map(e -> cmd(e.index(), 2, "v" + e.index())).toList());
            assertThat(open(d, 300).loadAll()).as("seed %d", seed).isEqualTo(expected);
        }
        assertThat(checkpointsCut).as("runs whose crash interrupted a checkpoint").isPositive();
    }

    @Test
    void snapshotsWorkOnARealDirectory(@TempDir Path dir) {
        try (RealDisk d = new RealDisk(dir)) {
            FileWalStorage s = FileWalStorage.open(d, new WalOptions(200));
            s.saveHardState(T2);
            s.append(range(1, 20, 2));
            s.sync();
            s.saveSnapshot(snap(15, 2, "state@15"));
            s.compactLog(15, 2);
            s.sync();
        }
        try (RealDisk d = new RealDisk(dir)) {
            PersistentState p = FileWalStorage.open(d, WalOptions.DEFAULTS).loadAll();
            assertThat(p.snapshot()).hasValue(snap(15, 2, "state@15"));
            assertThat(p.entries()).isEqualTo(range(16, 20, 2));
        }
    }

    // ---------------------------------------------------------------- storage mutations (M3-F), sanity

    @Test
    void withoutFsyncingItsDataARenamedSnapshotCanBeLostInACrash() {
        int corrupt = 0;
        for (long seed = 0; seed < 50; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = FileWalStorage.open(d, new WalOptions(1 << 20, Set.of(StorageMutation.SKIP_SNAPSHOT_DATA_FSYNC)));
            s.saveHardState(T2);
            s.append(range(1, 5, 2));
            s.sync();
            s.saveSnapshot(snap(4, 2, "state@4"));
            s.compactLog(4, 2);
            s.sync();
            d.crash();
            try {
                open(d, 1 << 20);
            } catch (StorageException e) {
                corrupt++;
            }
        }
        assertThat(corrupt).isPositive();
    }

    @Test
    void withoutCuttingTheTornTailLaterWritesBecomeUnreachable() {
        int lost = 0;
        for (long seed = 0; seed < 100; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = open(d, 1 << 20);
            s.saveHardState(T2);
            s.append(range(1, 1, 2));
            s.sync();
            s.append(range(2, 2, 2));
            d.crash();                                     // maybe torn
            FileWalStorage r = FileWalStorage.open(d, new WalOptions(1 << 20, Set.of(StorageMutation.SKIP_TORN_TAIL_CUT)));
            long last = r.loadAll().entries().size();
            r.append(range(last + 1, last + 1, 2));
            r.sync();
            d.crash();
            if (open(d, 1 << 20).loadAll().entries().size() != last + 1) lost++;
        }
        assertThat(lost).isPositive();
    }
}
