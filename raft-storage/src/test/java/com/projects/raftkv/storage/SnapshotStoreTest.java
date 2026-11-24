package com.projects.raftkv.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.storage.disk.SimDisk;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/** The snapshot file is replaced atomically: after any crash it is the old one or the new one, never a mix. */
class SnapshotStoreTest {

    static Snapshot snap(long index, String s) { return new Snapshot(index, 1, Bytes.copyOf(s.getBytes())); }

    @Test
    void noSnapshotYet() {
        assertThat(new SnapshotStore(new SimDisk(new SplittableRandom(1)), false).load()).isEmpty();
    }

    @Test
    void aSavedSnapshotSurvivesACrash() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        new SnapshotStore(d, false).save(snap(5, "five"));
        d.crash();
        assertThat(new SnapshotStore(d, false).load()).hasValue(snap(5, "five"));
    }

    @Test
    void aCrashDuringReplacementLeavesTheOldOrTheNewSnapshot() {
        // Crash after each of the disk operations in save(): tmp write, fsync, rename, fsync dir.
        for (int stopAfter = 0; stopAfter <= 4; stopAfter++) {
            for (long seed = 0; seed < 50; seed++) {
                SimDisk d = new SimDisk(new SplittableRandom(seed));
                SnapshotStore store = new SnapshotStore(d, false);
                store.save(snap(5, "old state"));
                store.saveSteps(snap(9, "new state, longer than the old one"), stopAfter);
                d.crash();
                Optional<Snapshot> loaded = new SnapshotStore(d, false).load();
                assertThat(loaded).as("stop after %d, seed %d", stopAfter, seed)
                        .isIn(Optional.of(snap(5, "old state")), Optional.of(snap(9, "new state, longer than the old one")));
            }
        }
    }

    @Test
    void aCorruptSnapshotFileFailsLoudly() {
        SimDisk d = new SimDisk(new SplittableRandom(1));
        new SnapshotStore(d, false).save(snap(5, "five"));
        byte[] b = d.read(SnapshotStore.FILE);
        b[b.length / 2] ^= 1;
        d.truncate(SnapshotStore.FILE, 0);
        d.append(SnapshotStore.FILE, b);
        assertThatThrownBy(() -> new SnapshotStore(d, false).load()).isInstanceOf(StorageException.class);
    }
}
