package com.projects.raftkv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.ClientNotice;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.PersistOp;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import com.projects.raftkv.statemachine.KvResult;
import com.projects.raftkv.statemachine.KvStateMachine;
import com.projects.raftkv.statemachine.StateMachine;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.Snapshot;
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
                new KvStateMachine(), o -> {}, new SplittableRandom(1));

        tickUntilCandidate(d);

        assertThat(ops).containsExactly("saveHardState", "sync", "send RequestVote", "send RequestVote");
    }

    /** E1a: a failed fsync halts the node; nothing that depends on the failed write is sent. */
    @Test
    void failedSyncHaltsNodeAndSendsNothing() {
        InMemoryStorage storage = new InMemoryStorage();
        List<Message> sent = new ArrayList<>();
        NodeDriver d = NodeDriver.recover(CONFIG, storage, sent::add, new KvStateMachine(), o -> {}, new SplittableRandom(1));
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

        NodeDriver d = NodeDriver.recover(CONFIG, storage, m -> {}, new KvStateMachine(), o -> {}, new SplittableRandom(1));

        assertThat(d.status().hard()).isEqualTo(new HardState(7, Optional.of(new NodeId(2))));
    }

    // ---------------------------------------------------------------- apply path (M2)

    private static final Bytes PUT_A = KvCodec.encode(new KvRequest(1, 1, new KvCommand.Put("a", "1")));
    private static final Bytes PUT_B = KvCodec.encode(new KvRequest(1, 2, new KvCommand.Put("b", "2")));

    private static LogEntry command(long index, long term, Bytes cmd) {
        return new LogEntry(index, term, new EntryPayload.Command(cmd));
    }

    private static Ready ready(List<PersistOp> persist, List<Message> msgs, List<LogEntry> apply, List<ClientNotice> notices) {
        return new Ready(persist, msgs, apply, notices, Optional.empty());
    }

    /** E1: persist, fsync, send, and only then apply. */
    @Test
    void appliesCommittedEntriesAfterPersistingAndSending() {
        List<String> ops = new ArrayList<>();
        LogEntry e = command(1, 1, PUT_A);
        Message msg = new Message.AppendEntriesResponse(N1, new NodeId(2), 1, true, 1, 0, 0, 0);
        StateMachine sm = new StateMachine() {
            @Override public Bytes apply(long index, Bytes command) { ops.add("apply " + index); return Bytes.EMPTY; }
            @Override public long lastAppliedIndex() { return 0; }
            @Override public Bytes snapshot() { throw new UnsupportedOperationException(); }
            @Override public void restore(long index, Bytes snapshot) { throw new UnsupportedOperationException(); }
        };
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(List.of(new PersistOp.AppendEntries(List.of(e))),
                List.of(msg), List.of(e), List.of())), new RecordingStorage(ops), m -> ops.add("send"), sm, o -> {});

        d.handle(new Event.Tick());

        assertThat(ops).containsExactly("append", "sync", "send", "apply 1");
    }

    @Test
    void noOpEntriesAreNotGivenToTheStateMachine() {
        KvStateMachine sm = new KvStateMachine();
        LogEntry noop = new LogEntry(1, 1, new EntryPayload.Noop());
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(List.of(), List.of(), List.of(noop, command(2, 1, PUT_A)), List.of())),
                new InMemoryStorage(), m -> {}, sm, o -> {});

        d.handle(new Event.Tick());

        assertThat(sm.lastAppliedIndex()).isEqualTo(2);
        assertThat(sm.view()).containsEntry("a", "1");
    }

    /** M2-E: an accepted proposal completes with the state machine's result when (index, term) is applied. */
    @Test
    void acceptedProposalCompletesWithTheResultWhenItsEntryIsApplied() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(
                        ready(List.of(), List.of(), List.of(), List.of(new ClientNotice.ProposalAccepted(9, 1, 2))),
                        ready(List.of(), List.of(), List.of(command(1, 2, PUT_A)), List.of())),
                new InMemoryStorage(), m -> {}, new KvStateMachine(), outcomes::add);

        d.handle(new Event.ProposeCommand(9, PUT_A));
        assertThat(outcomes).isEmpty();
        d.handle(new Event.Tick());

        assertThat(outcomes).containsExactly(new ProposalOutcome.Committed(9, 1,
                KvCodec.encode(new KvResult.Value(Optional.empty()))));
    }

    /** Thesis §6.3: another leader's entry was committed at our index, so our proposal was overwritten. */
    @Test
    void proposalIsLostWhenADifferentTermIsAppliedAtItsIndex() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        KvStateMachine sm = new KvStateMachine();
        NodeDriver d = NodeDriver.create(new ScriptedCore(
                        ready(List.of(), List.of(), List.of(), List.of(new ClientNotice.ProposalAccepted(9, 1, 2))),
                        ready(List.of(), List.of(), List.of(command(1, 3, PUT_B)), List.of())),
                new InMemoryStorage(), m -> {}, sm, outcomes::add);

        d.handle(new Event.ProposeCommand(9, PUT_A));
        d.handle(new Event.Tick());

        assertThat(outcomes).containsExactly(new ProposalOutcome.Lost(9, 1));
        assertThat(sm.view()).containsOnlyKeys("b");
    }

    /** A newer proposal accepted at the same index means the older one's entry was truncated away. */
    @Test
    void olderProposalAtAReusedIndexIsLost() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(
                        ready(List.of(), List.of(), List.of(), List.of(new ClientNotice.ProposalAccepted(1, 5, 2))),
                        ready(List.of(), List.of(), List.of(), List.of(new ClientNotice.ProposalAccepted(2, 5, 4)))),
                new InMemoryStorage(), m -> {}, new KvStateMachine(), outcomes::add);

        d.handle(new Event.ProposeCommand(1, PUT_A));
        d.handle(new Event.ProposeCommand(2, PUT_B));

        assertThat(outcomes).containsExactly(new ProposalOutcome.Lost(1, 5));
    }

    @Test
    void rejectedProposalIsReportedWithTheLeaderHint() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(List.of(), List.of(), List.of(),
                        List.of(new ClientNotice.ProposalRejected(4, Optional.of(new NodeId(3)))))),
                new InMemoryStorage(), m -> {}, new KvStateMachine(), outcomes::add);

        d.handle(new Event.ProposeCommand(4, PUT_A));

        assertThat(outcomes).containsExactly(new ProposalOutcome.Rejected(4, Optional.of(new NodeId(3))));
    }

    /** Hands out pre-built Readys in order; lets driver tests control effects exactly. */
    private static final class ScriptedCore implements RaftCore {
        private final java.util.ArrayDeque<Ready> script;
        ScriptedCore(Ready... readys) { script = new java.util.ArrayDeque<>(List.of(readys)); }
        @Override public Ready step(Event event) { return script.isEmpty() ? Ready.EMPTY : script.removeFirst(); }
        @Override public RaftStatus status() { throw new UnsupportedOperationException(); }
        @Override public LogView log() { throw new UnsupportedOperationException(); }
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
        @Override public void saveSnapshot(Snapshot snapshot) { ops.add("saveSnapshot@" + snapshot.index()); }
        @Override public void compactLog(long index, long term) { ops.add("compactLog@" + index); }
        @Override public void sync() { ops.add("sync"); }
        @Override public PersistentState loadAll() { return PersistentState.EMPTY; }
    }


    // ---------------------------------------------------------------- M3: snapshots

    private static Message.AppendEntries appendFromLeader(long count, long leaderCommit) {
        List<LogEntry> es = new ArrayList<>();
        for (long i = 1; i <= count; i++) {
            es.add(command(i, 1, KvCodec.encode(new KvRequest(1, i, new KvCommand.Put("k" + i, "v" + i)))));
        }
        return new Message.AppendEntries(new NodeId(2), N1, 1, 0, 0, es, leaderCommit);
    }

    /** M3-E: the snapshot is durable before the compaction record that depends on it is written. */
    @Test
    void takesASnapshotEveryNAppliedEntriesAndSavesItBeforeCompacting() {
        List<String> ops = new ArrayList<>();
        KvStateMachine sm = new KvStateMachine();
        NodeDriver d = NodeDriver.recover(CONFIG, new RecordingStorage(ops), m -> {}, sm, o -> {}, new SplittableRandom(1), 3);

        d.handle(new Event.MessageReceived(appendFromLeader(5, 4)));

        assertThat(ops).containsSubsequence("append", "sync", "saveSnapshot@4", "compactLog@4", "sync");
        assertThat(ops.indexOf("saveSnapshot@4")).isLessThan(ops.indexOf("compactLog@4"));
        assertThat(d.log().snapshotIndex()).isEqualTo(4);
        assertThat(d.snapshotsTaken()).isEqualTo(1);
    }

    @Test
    void aNodeRecoversItsStateMachineFromTheSnapshot() {
        InMemoryStorage storage = new InMemoryStorage();
        NodeDriver first = NodeDriver.recover(CONFIG, storage, m -> {}, new KvStateMachine(), o -> {}, new SplittableRandom(1), 3);
        first.handle(new Event.MessageReceived(appendFromLeader(5, 5)));
        storage.crash();

        KvStateMachine sm = new KvStateMachine();
        NodeDriver second = NodeDriver.recover(CONFIG, storage, m -> {}, sm, o -> {}, new SplittableRandom(1), 3);

        assertThat(second.log().snapshotIndex()).isEqualTo(5);
        assertThat(second.status().lastApplied()).isEqualTo(5);
        assertThat(sm.view()).containsEntry("k1", "v1").containsEntry("k5", "v5");
    }

    @Test
    void anInstalledSnapshotReplacesTheStateMachine() {
        KvStateMachine leaderState = new KvStateMachine();
        leaderState.apply(1, KvCodec.encode(new KvRequest(1, 1, new KvCommand.Put("x", "from leader"))));
        Snapshot s = new Snapshot(9, 1, leaderState.snapshot());
        KvStateMachine sm = new KvStateMachine();
        NodeDriver d = NodeDriver.recover(CONFIG, new InMemoryStorage(), m -> {}, sm, o -> {}, new SplittableRandom(1), 0);

        d.handle(new Event.MessageReceived(new Message.InstallSnapshot(new NodeId(2), N1, 1, 9, 1, 0, s.data(), true)));

        assertThat(sm.view()).containsExactlyEntriesOf(java.util.Map.of("x", "from leader"));
        assertThat(sm.lastAppliedIndex()).isEqualTo(9);
        assertThat(d.snapshotsInstalled()).isEqualTo(1);
    }

    @Test
    void proposalsCoveredByAnInstalledSnapshotEndAsUnknown() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        KvStateMachine sm = new KvStateMachine();
        Ready accepted = ready(List.of(), List.of(), List.of(), List.of(new ClientNotice.ProposalAccepted(7, 3, 1)));
        Ready install = new Ready(List.of(), List.of(), List.of(), List.of(), Optional.empty(),
                Optional.of(new Snapshot(5, 2, new KvStateMachine().snapshot())));
        NodeDriver d = NodeDriver.create(new ScriptedCore(accepted, install), new InMemoryStorage(), m -> {}, sm, outcomes::add);

        d.handle(new Event.Tick());
        d.handle(new Event.Tick());

        assertThat(outcomes).containsExactly(new ProposalOutcome.Unknown(7, 3));
        assertThat(d.pendingProposals()).isZero();
    }
}
