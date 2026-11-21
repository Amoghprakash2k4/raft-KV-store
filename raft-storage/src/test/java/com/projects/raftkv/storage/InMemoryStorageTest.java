package com.projects.raftkv.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InMemoryStorageTest {

    private static final HardState T3_N2 = new HardState(3, Optional.of(new NodeId(2)));

    @Test
    void syncedWritesSurviveCrash() {
        InMemoryStorage s = new InMemoryStorage();
        s.saveHardState(T3_N2);
        s.append(List.of(entry(1, 1), entry(2, 3)));
        s.sync();

        s.crash();

        assertThat(s.loadAll().hardState()).isEqualTo(T3_N2);
        assertThat(s.loadAll().entries()).containsExactly(entry(1, 1), entry(2, 3));
    }

    @Test
    void unsyncedWritesAreLostOnCrash() {
        InMemoryStorage s = new InMemoryStorage();
        s.saveHardState(new HardState(1, Optional.empty()));
        s.sync();
        s.saveHardState(T3_N2);
        s.append(List.of(entry(1, 3)));

        s.crash();

        assertThat(s.loadAll().hardState()).isEqualTo(new HardState(1, Optional.empty()));
        assertThat(s.loadAll().entries()).isEmpty();
    }

    @Test
    void truncateRemovesSuffix() {
        InMemoryStorage s = new InMemoryStorage();
        s.saveHardState(new HardState(3, Optional.empty()));
        s.append(List.of(entry(1, 1), entry(2, 1), entry(3, 2)));
        s.truncateFrom(2);
        s.append(List.of(entry(2, 3)));
        s.sync();

        assertThat(s.loadAll().entries()).containsExactly(entry(1, 1), entry(2, 3));
    }

    @Test
    void rejectsNonContiguousAppend() {
        InMemoryStorage s = new InMemoryStorage();
        assertThatThrownBy(() -> s.append(List.of(entry(2, 1)))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedSyncThrowsAndMakesNothingDurable() {
        InMemoryStorage s = new InMemoryStorage();
        s.saveHardState(T3_N2);
        s.failNextSync();

        assertThatThrownBy(s::sync).isInstanceOf(StorageException.class);
        s.crash();
        assertThat(s.loadAll().hardState()).isEqualTo(HardState.INITIAL);
    }

    @Test
    void lyingFsyncLosesEverythingOnCrash() {
        InMemoryStorage s = new InMemoryStorage();
        s.setFsyncIsNoop(true);
        s.saveHardState(T3_N2);
        s.sync();

        s.crash();

        assertThat(s.loadAll().hardState()).isEqualTo(HardState.INITIAL);
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, new EntryPayload.Noop());
    }
}
