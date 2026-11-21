package com.projects.raftkv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.storage.InMemoryStorage;
import com.projects.raftkv.storage.Storage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class NodeDriverTest {

    private static final NodeId N1 = new NodeId(1);
    private static final RaftConfig CONFIG =
            new RaftConfig(N1, List.of(N1, new NodeId(2), new NodeId(3)), 10, 2, 64);

    /** E1: every message leaves only after the state it depends on is fsynced. */
    @Test
    void persistsAndSyncsBeforeSending() {
        List<String> ops = new ArrayList<>();
        Storage storage = new RecordingStorage(ops);
        NodeDriver d = NodeDriver.recover(CONFIG, storage, m -> ops.add("send " + m.getClass().getSimpleName()),
                new SplittableRandom(1));

        tickUntilCandidate(d);

        assertThat(ops).containsExactly("saveHardState", "sync", "send RequestVote", "send RequestVote");
    }

    /** E1a: a failed fsync halts the node; nothing that depends on the failed write is sent. */
    @Test
    void failedSyncHaltsNodeAndSendsNothing() {
        InMemoryStorage storage = new InMemoryStorage();
        List<Message> sent = new ArrayList<>();
        NodeDriver d = NodeDriver.recover(CONFIG, storage, sent::add, new SplittableRandom(1));
        storage.failNextSync();

        assertThatThrownBy(() -> tickUntilCandidate(d)).isInstanceOf(NodeHaltedException.class);
        assertThat(sent).isEmpty();
        assertThat(d.isHalted()).isTrue();
        assertThatThrownBy(() -> d.handle(new Event.Tick())).isInstanceOf(NodeHaltedException.class);
    }

    @Test
    void recoversHardStateFromStorage() {
        InMemoryStorage storage = new InMemoryStorage();
        storage.saveHardState(new HardState(7, Optional.of(new NodeId(2))));
        storage.sync();

        NodeDriver d = NodeDriver.recover(CONFIG, storage, m -> {}, new SplittableRandom(1));

        assertThat(d.status().hard()).isEqualTo(new HardState(7, Optional.of(new NodeId(2))));
    }

    private static void tickUntilCandidate(NodeDriver d) {
        for (int i = 0; i < 20 && d.status().soft().role() != Role.CANDIDATE; i++) {
            d.handle(new Event.Tick());
        }
    }

    private record RecordingStorage(List<String> ops) implements Storage {
        @Override public void saveHardState(HardState state) { ops.add("saveHardState"); }
        @Override public void append(List<LogEntry> entries) { ops.add("append"); }
        @Override public void truncateFrom(long fromIndex) { ops.add("truncateFrom"); }
        @Override public void sync() { ops.add("sync"); }
        @Override public PersistentState loadAll() { return PersistentState.EMPTY; }
    }

}
