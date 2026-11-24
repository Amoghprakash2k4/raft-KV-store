package com.projects.raftkv.storage.wal;

import static com.projects.raftkv.storage.wal.FileWalStorageTest.cmd;
import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.storage.disk.SimDisk;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.TreeMap;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

/**
 * Cut a valid WAL at EVERY byte offset (and optionally add garbage after the cut). Recovery must
 * return exactly the state after the last whole record before the cut, and later writes must work.
 */
class WalTornTailProperties {

    /** A WAL written by a random op sequence, with the expected state at every record boundary. */
    record Written(byte[] file, TreeMap<Long, PersistentState> stateAtOffset) {}

    static Written write(long seed) {
        SplittableRandom rng = new SplittableRandom(seed);
        SimDisk d = new SimDisk(rng.split());
        FileWalStorage s = FileWalStorage.open(d, WalOptions.DEFAULTS);
        TreeMap<Long, PersistentState> states = new TreeMap<>();
        HardState hs = HardState.INITIAL;
        List<LogEntry> log = new ArrayList<>();
        states.put(0L, PersistentState.EMPTY);
        for (int op = 0; op < 40; op++) {
            int kind = rng.nextInt(10);
            if (kind < 2) {
                hs = new HardState(hs.currentTerm() + 1, rng.nextBoolean() ? Optional.of(new NodeId(rng.nextInt(5))) : Optional.empty());
                s.saveHardState(hs);
            } else if (kind < 3 && !log.isEmpty()) {
                long from = 1 + rng.nextInt(log.size());
                s.truncateFrom(from);
                log.subList((int) from - 1, log.size()).clear();
            } else {
                long term = Math.max(1, hs.currentTerm());
                if (hs.currentTerm() == 0) { hs = new HardState(1, Optional.empty()); s.saveHardState(hs); states.put(size(d), new PersistentState(hs, log)); }
                List<LogEntry> batch = new ArrayList<>();
                int n = 1 + rng.nextInt(3);
                for (int i = 0; i < n; i++) batch.add(cmd(log.size() + batch.size() + 1, term, "v" + rng.nextInt(1000)));
                s.append(batch);
                log.addAll(batch);
            }
            states.put(size(d), new PersistentState(hs, log));
            if (rng.nextInt(3) == 0) s.sync();
        }
        s.sync();
        String name = d.list().get(0);
        return new Written(d.read(name), states);
    }

    private static long size(SimDisk d) {
        return d.list().isEmpty() ? 0 : d.size(d.list().get(0));
    }

    static PersistentState recoverFrom(byte[] contents) {
        SimDisk d = new SimDisk(new SplittableRandom(0));
        d.append("wal-0000000000000001.log", contents);
        d.fsync("wal-0000000000000001.log");
        d.fsyncDir();
        return FileWalStorage.open(d, WalOptions.DEFAULTS).loadAll();
    }

    @Test
    void everyCutOffsetRecoversTheLastWholeRecord() {
        for (long seed = 0; seed < 20; seed++) {
            Written w = write(seed);
            for (int cut = 0; cut <= w.file().length; cut++) {
                PersistentState expected = w.stateAtOffset().floorEntry((long) cut).getValue();
                assertThat(recoverFrom(Arrays.copyOf(w.file(), cut)))
                        .as("seed %d, cut at %d of %d", seed, cut, w.file().length)
                        .isEqualTo(expected);
            }
        }
    }

    @Property(tries = 300)
    void garbageAfterACutIsDiscarded(@ForAll @LongRange(max = 1_000) long seed,
                                     @ForAll @IntRange(min = 1, max = 64) int garbage,
                                     @ForAll boolean zeros) {
        Written w = write(seed);
        SplittableRandom rng = new SplittableRandom(seed * 31 + garbage);
        int cut = rng.nextInt(w.file().length + 1);
        byte[] torn = Arrays.copyOf(w.file(), cut + garbage);
        for (int i = cut; i < torn.length; i++) torn[i] = zeros ? 0 : (byte) rng.nextInt(256);

        PersistentState expected = w.stateAtOffset().floorEntry((long) cut).getValue();
        assertThat(recoverFrom(torn)).isEqualTo(expected);
    }
}
