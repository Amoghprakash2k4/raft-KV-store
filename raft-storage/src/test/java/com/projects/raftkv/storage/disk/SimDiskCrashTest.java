package com.projects.raftkv.storage.disk;

import static com.projects.raftkv.storage.disk.DiskContractTest.b;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.storage.StorageException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/** The crash model: what survives a power loss on SimDisk. */
class SimDiskCrashTest {

    private static SimDisk disk(long seed) { return new SimDisk(new SplittableRandom(seed)); }

    @Test
    void syncedDataSurvivesACrash() {
        SimDisk d = disk(1);
        d.append("a", b("durable"));
        d.fsync("a");
        d.fsyncDir();
        d.crash();
        assertThat(d.read("a")).isEqualTo(b("durable"));
    }

    @Test
    void aFileWhoseNameWasNeverSyncedDisappears() {
        SimDisk d = disk(1);
        d.append("a", b("data"));
        d.fsync("a");            // data synced, but the directory entry is not
        d.crash();
        assertThat(d.exists("a")).isFalse();
    }

    @Test
    void anUnsyncedRenameIsUndoneByACrash() {
        SimDisk d = disk(1);
        d.append("snap", b("old"));
        d.fsync("snap");
        d.fsyncDir();
        d.append("snap.tmp", b("new"));
        d.fsync("snap.tmp");
        d.rename("snap.tmp", "snap");
        d.crash();
        assertThat(d.read("snap")).isEqualTo(b("old"));
    }

    @Test
    void aSyncedRenameSurvives() {
        SimDisk d = disk(1);
        d.append("snap.tmp", b("new"));
        d.fsync("snap.tmp");
        d.rename("snap.tmp", "snap");
        d.fsyncDir();
        d.crash();
        assertThat(d.read("snap")).isEqualTo(b("new"));
        assertThat(d.exists("snap.tmp")).isFalse();
    }

    @Test
    void anUnsyncedDeleteIsUndoneByACrash() {
        SimDisk d = disk(1);
        d.append("a", b("x"));
        d.fsync("a");
        d.fsyncDir();
        d.delete("a");
        d.crash();
        assertThat(d.read("a")).isEqualTo(b("x"));
    }

    @Test
    void renamingWithoutSyncingTheDataCanLeaveAnEmptyOrPartialFile() {
        // The classic bug: write tmp, rename, fsync the directory, but never fsync tmp's data.
        Set<Integer> sizes = new HashSet<>();
        for (long seed = 0; seed < 200; seed++) {
            SimDisk d = disk(seed);
            d.append("snap.tmp", b("0123456789"));
            d.rename("snap.tmp", "snap");
            d.fsyncDir();
            d.crash();
            sizes.add((int) d.size("snap"));
        }
        assertThat(sizes).contains(0).contains(10).anyMatch(s -> s > 0 && s < 10);
    }

    @Test
    void unsyncedAppendsSurviveAsSomePrefixOfAllTheWrites() {
        byte[] synced = b("AAAA");
        byte[] unsynced = b("BBBBBBBBBBBBBBBBBBBB");
        Set<Integer> survivingLengths = new HashSet<>();
        for (long seed = 0; seed < 300; seed++) {
            SimDisk d = disk(seed);
            d.append("wal", synced);
            d.fsync("wal");
            d.fsyncDir();
            d.append("wal", unsynced);
            d.crash();

            byte[] after = d.read("wal");
            assertThat(after.length).isBetween(synced.length, synced.length + unsynced.length);
            assertThat(Arrays.copyOf(after, synced.length)).isEqualTo(synced);   // synced bytes never change
            survivingLengths.add(after.length);
        }
        // All, none, and partial (torn) outcomes all occur.
        assertThat(survivingLengths).contains(4, 24).anyMatch(n -> n > 4 && n < 24);
    }

    @Test
    void aTornTailCanContainGarbageButSyncedBytesNeverChange() {
        boolean sawGarbage = false;
        for (long seed = 0; seed < 300; seed++) {
            SimDisk d = disk(seed);
            d.append("wal", b("AAAA"));
            d.fsync("wal");
            d.fsyncDir();
            d.append("wal", b("BBBBBBBBBBBBBBBB"));
            d.crash();
            byte[] after = d.read("wal");
            assertThat(Arrays.copyOf(after, 4)).isEqualTo(b("AAAA"));
            for (int i = 4; i < after.length; i++) if (after[i] != 'B') sawGarbage = true;
        }
        assertThat(sawGarbage).as("some crash leaves zeroed or random bytes in the torn region").isTrue();
    }

    @Test
    void anUnsyncedTruncateIsUndoneByACrash() {
        SimDisk d = disk(1);
        d.append("wal", b("abcdef"));
        d.fsync("wal");
        d.fsyncDir();
        d.truncate("wal", 2);
        d.crash();
        assertThat(d.read("wal")).isEqualTo(b("abcdef"));
    }

    @Test
    void aSyncedTruncateSurvives() {
        SimDisk d = disk(1);
        d.append("wal", b("abcdef"));
        d.fsync("wal");
        d.fsyncDir();
        d.truncate("wal", 2);
        d.fsync("wal");
        d.crash();
        assertThat(d.read("wal")).isEqualTo(b("ab"));
    }

    @Test
    void aFailedFsyncThrowsAndPersistsNothingExtra() {
        SimDisk d = disk(1);
        d.append("wal", b("AAAA"));
        d.fsync("wal");
        d.fsyncDir();
        d.append("wal", b("BBBB"));
        d.failNextFsync();
        assertThatThrownBy(() -> d.fsync("wal")).isInstanceOf(StorageException.class);
        d.append("wal", b("C"));
        d.fsync("wal");          // the failure is one-shot
        d.crash();
        assertThat(d.read("wal")).isEqualTo(b("AAAABBBBC"));
    }

    @Test
    void aLyingDiskLosesEverythingAfterTheLastHonestSync() {
        SimDisk d = disk(1);
        d.append("wal", b("AAAA"));
        d.fsync("wal");
        d.fsyncDir();
        d.setFsyncIsNoop(true);
        d.append("wal", b("BBBB"));
        d.fsync("wal");
        boolean lost = false;
        for (long seed = 0; seed < 50 && !lost; seed++) {
            SimDisk x = disk(seed);
            x.append("wal", b("AAAA"));
            x.fsync("wal");
            x.fsyncDir();
            x.setFsyncIsNoop(true);
            x.append("wal", b("BBBB"));
            x.fsync("wal");
            x.crash();
            lost = x.size("wal") < 8;
        }
        assertThat(lost).isTrue();
    }

    @Test
    void crashesAreDeterministicPerSeed() {
        byte[][] out = new byte[2][];
        for (int run = 0; run < 2; run++) {
            SimDisk d = disk(42);
            d.append("wal", b("AAAA"));
            d.fsync("wal");
            d.fsyncDir();
            d.append("wal", b("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"));
            d.crash();
            out[run] = d.read("wal");
        }
        assertThat(out[0]).isEqualTo(out[1]);
    }

    @Test
    void writesAfterACrashWork() {
        SimDisk d = disk(3);
        d.append("wal", b("AAAA"));
        d.fsync("wal");
        d.fsyncDir();
        d.crash();
        d.append("wal", b("B"));
        d.fsync("wal");
        d.crash();
        assertThat(d.read("wal")).isEqualTo(b("AAAAB"));
    }
}
