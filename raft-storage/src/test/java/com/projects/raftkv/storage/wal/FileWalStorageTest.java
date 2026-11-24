package com.projects.raftkv.storage.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.storage.disk.RealDisk;
import com.projects.raftkv.storage.disk.SimDisk;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileWalStorageTest {

    static final HardState T3_N2 = new HardState(3, Optional.of(new NodeId(2)));

    static LogEntry cmd(long index, long term, String s) {
        return new LogEntry(index, term, new EntryPayload.Command(Bytes.copyOf(s.getBytes())));
    }

    static LogEntry noop(long index, long term) { return new LogEntry(index, term, new EntryPayload.Noop()); }

    static SimDisk disk() { return new SimDisk(new SplittableRandom(7)); }

    static FileWalStorage open(SimDisk d) { return FileWalStorage.open(d, WalOptions.DEFAULTS); }

    @Test
    void anEmptyDirectoryRecoversTheInitialState() {
        assertThat(open(disk()).loadAll()).isEqualTo(PersistentState.EMPTY);
    }

    @Test
    void syncedStateSurvivesACrash() {
        SimDisk d = disk();
        FileWalStorage s = open(d);
        s.saveHardState(T3_N2);
        s.append(List.of(noop(1, 1), cmd(2, 3, "x")));
        s.sync();
        d.crash();

        PersistentState p = open(d).loadAll();
        assertThat(p.hardState()).isEqualTo(T3_N2);
        assertThat(p.entries()).containsExactly(noop(1, 1), cmd(2, 3, "x"));
    }

    @Test
    void truncationIsReplayed() {
        SimDisk d = disk();
        FileWalStorage s = open(d);
        s.saveHardState(new HardState(3, Optional.empty()));
        s.append(List.of(cmd(1, 1, "a"), cmd(2, 1, "b"), cmd(3, 2, "c")));
        s.truncateFrom(2);
        s.append(List.of(cmd(2, 3, "B")));
        s.sync();
        d.crash();

        assertThat(open(d).loadAll().entries()).containsExactly(cmd(1, 1, "a"), cmd(2, 3, "B"));
    }

    @Test
    void writesAfterTheLastSyncNeverReappearAsAnythingButAPrefix() {
        // Whatever survives the crash must be a state the node actually passed through, in op order.
        for (long seed = 0; seed < 300; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = open(d);
            s.saveHardState(new HardState(1, Optional.empty()));
            s.append(List.of(cmd(1, 1, "a")));
            s.sync();
            s.saveHardState(T3_N2);
            s.append(List.of(cmd(2, 3, "b")));
            s.append(List.of(cmd(3, 3, "c")));
            d.crash();

            PersistentState p = open(d).loadAll();
            List<LogEntry> e = p.entries();
            assertThat(e).startsWith(cmd(1, 1, "a"));
            if (e.size() > 1) assertThat(p.hardState()).isEqualTo(T3_N2);   // ops replay in order
            assertThat(List.of(cmd(1, 1, "a"), cmd(2, 3, "b"), cmd(3, 3, "c"))).startsWith(e.toArray(LogEntry[]::new));
        }
    }

    @Test
    void recoveryCutsTheTornTailSoNewWritesFollowValidRecords() {
        for (long seed = 0; seed < 200; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = open(d);
            s.saveHardState(new HardState(1, Optional.empty()));
            s.append(List.of(cmd(1, 1, "a")));
            s.sync();
            s.append(List.of(cmd(2, 1, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")));
            d.crash();

            FileWalStorage r = open(d);
            long last = r.loadAll().entries().size();
            r.append(List.of(cmd(last + 1, 1, "after")));
            r.sync();
            d.crash();

            List<LogEntry> e = open(d).loadAll().entries();
            assertThat(e).hasSize((int) last + 1);
            assertThat(e.get(e.size() - 1)).isEqualTo(cmd(last + 1, 1, "after"));
        }
    }

    @Test
    void segmentsRollAfterASyncAndAllAreReplayed() {
        SimDisk d = disk();
        FileWalStorage s = FileWalStorage.open(d, new WalOptions(100));
        s.saveHardState(new HardState(1, Optional.empty()));
        for (long i = 1; i <= 40; i++) {
            s.append(List.of(cmd(i, 1, "value-" + i)));
            s.sync();
        }
        assertThat(d.list().stream().filter(n -> n.startsWith("wal-")).count()).isGreaterThan(5);
        d.crash();

        PersistentState p = open(d).loadAll();
        assertThat(p.entries()).hasSize(40);
        assertThat(p.entries().get(39)).isEqualTo(cmd(40, 1, "value-40"));
    }

    @Test
    void aNewSegmentsNameIsDurableOnceSynced() {
        // Rolling creates a file; its directory entry must be fsynced too, or a crash loses synced data.
        for (long seed = 0; seed < 100; seed++) {
            SimDisk d = new SimDisk(new SplittableRandom(seed));
            FileWalStorage s = FileWalStorage.open(d, new WalOptions(60));
            s.saveHardState(new HardState(1, Optional.empty()));
            for (long i = 1; i <= 10; i++) {
                s.append(List.of(cmd(i, 1, "vvvvvvvvvvvvvvvvvvvvvvv")));
                s.sync();
            }
            d.crash();
            assertThat(open(d).loadAll().entries()).hasSize(10);
        }
    }

    @Test
    void aBadRecordInAnEarlierSegmentIsCorruptionNotATornTail() {
        SimDisk d = disk();
        FileWalStorage s = FileWalStorage.open(d, new WalOptions(100));
        s.saveHardState(new HardState(1, Optional.empty()));
        for (long i = 1; i <= 20; i++) {
            s.append(List.of(cmd(i, 1, "value-" + i)));
            s.sync();
        }
        String first = d.list().get(0);
        byte[] bytes = d.read(first);
        bytes[bytes.length - 3] ^= 0x55;            // flip bits inside a synced record
        d.truncate(first, 0);
        d.append(first, bytes);
        d.fsync(first);

        assertThatThrownBy(() -> open(d)).isInstanceOf(StorageException.class).hasMessageContaining("corrupt");
    }

    @Test
    void appendsMustBeContiguous() {
        FileWalStorage s = open(disk());
        s.append(List.of(cmd(1, 1, "a")));
        assertThatThrownBy(() -> s.append(List.of(cmd(3, 1, "c")))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> s.truncateFrom(5)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aFailedFsyncPropagates() {
        SimDisk d = disk();
        FileWalStorage s = open(d);
        s.append(List.of(cmd(1, 1, "a")));
        d.failNextFsync();
        assertThatThrownBy(s::sync).isInstanceOf(StorageException.class);
    }

    @Test
    void worksOnARealDirectory(@TempDir Path dir) {
        try (RealDisk d = new RealDisk(dir)) {
            FileWalStorage s = FileWalStorage.open(d, new WalOptions(200));
            s.saveHardState(T3_N2);
            for (long i = 1; i <= 30; i++) {
                s.append(List.of(cmd(i, 3, "v" + i)));
                s.sync();
            }
            s.truncateFrom(25);
            s.sync();
        }
        try (RealDisk d = new RealDisk(dir)) {
            PersistentState p = FileWalStorage.open(d, WalOptions.DEFAULTS).loadAll();
            assertThat(p.hardState()).isEqualTo(T3_N2);
            assertThat(p.entries()).hasSize(24);
        }
    }
}
