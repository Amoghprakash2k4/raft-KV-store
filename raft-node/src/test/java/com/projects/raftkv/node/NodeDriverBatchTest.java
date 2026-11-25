package com.projects.raftkv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistOp;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import com.projects.raftkv.statemachine.KvResult;
import com.projects.raftkv.statemachine.KvStateMachine;
import com.projects.raftkv.storage.InMemoryStorage;
import com.projects.raftkv.storage.Storage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * Group commit (DESIGN.md M6-A): {@link NodeDriver#handleBatch} steps the core once per event, writes
 * every persist op, does ONE fsync, and only then sends, applies and answers, in event order. Nothing
 * leaves the node before the fsync that covers it (E1).
 */
class NodeDriverBatchTest {

    private static final NodeId N1 = new NodeId(1), N2 = new NodeId(2);

    /** A scripted step that appends entry {@code index} and sends one message. */
    private static Ready ready(long index) {
        LogEntry e = new LogEntry(index, 1, new EntryPayload.Noop());
        return new Ready(List.of(new PersistOp.AppendEntries(List.of(e))),
                List.of(new Message.RequestVoteResponse(N1, N2, 1, true)), List.of(), List.of(), Optional.empty());
    }

    @Test
    void everyOpOfTheBatchIsPersistedWithOneSyncBeforeAnythingIsSent() {
        List<String> ops = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(1), ready(2), ready(3)),
                new RecordingStorage(ops), m -> ops.add("send"), new KvStateMachine(), o -> {});

        d.handleBatch(List.of(new Event.Tick(), new Event.Tick(), new Event.Tick()));

        assertThat(ops).containsExactly("append", "append", "append", "sync", "send", "send", "send");
    }

    @Test
    void aBatchThatPersistsNothingDoesNotSync() {
        List<String> ops = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(Ready.EMPTY, Ready.EMPTY), new RecordingStorage(ops),
                m -> ops.add("send"), new KvStateMachine(), o -> {});
        d.handleBatch(List.of(new Event.Tick(), new Event.Tick()));
        assertThat(ops).isEmpty();
    }

    @Test
    void aFailedBatchSyncHaltsTheNodeAndSendsNothingFromTheWholeBatch() {
        InMemoryStorage storage = new InMemoryStorage();
        List<Message> sent = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(1), ready(2)), storage, sent::add,
                new KvStateMachine(), o -> {});
        storage.failNextSync();

        assertThatThrownBy(() -> d.handleBatch(List.of(new Event.Tick(), new Event.Tick())))
                .isInstanceOf(NodeHaltedException.class);
        assertThat(sent).isEmpty();
        assertThat(d.isHalted()).isTrue();
    }

    /** The point of group commit: five writes cost the leader one fsync, not five. */
    @Test
    void fiveProposalsInOneBatchCommitWithOneSyncAndAnswerInOrder() {
        CountingStorage storage = new CountingStorage();
        List<ProposalOutcome> outcomes = new ArrayList<>();
        NodeDriver d = leaderOfThree(storage, outcomes);
        long syncsBefore = storage.syncs;

        List<Event> batch = new ArrayList<>();
        for (int i = 1; i <= 5; i++) batch.add(new Event.ProposeCommand(i, put(i)));
        d.handleBatch(batch);
        assertThat(storage.syncs - syncsBefore).isEqualTo(1);
        assertThat(outcomes).isEmpty();          // not committed yet: no follower has them

        d.handle(ack(6));                        // n2 has everything up to 6 (no-op + 5 writes)

        assertThat(outcomes).extracting(ProposalOutcome::proposalId).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(outcomes).allMatch(o -> o instanceof ProposalOutcome.Committed);
        assertThat(KvCodec.decodeResult(((ProposalOutcome.Committed) outcomes.get(4)).result()))
                .isEqualTo(new KvResult.Value(Optional.of("v2")));   // write 5 is to k2; write 2 wrote it before
    }

    /** Batching changes when things are written, never what happens: same events → same results. */
    @Test
    void aBatchEndsInTheSameStateAsTheSameEventsOneByOne() {
        for (int seed = 1; seed <= 50; seed++) {
            SplittableRandom rng = new SplittableRandom(seed);
            List<Event> events = new ArrayList<>();
            int proposals = 0;
            for (int i = 0; i < 200; i++) {
                int dice = rng.nextInt(4);
                if (dice == 0) {
                    events.add(new Event.Tick());
                } else if (dice == 1) {
                    events.add(ack(1 + proposals));   // n2 acknowledges everything proposed so far
                } else {
                    proposals++;
                    events.add(new Event.ProposeCommand(1000 + i, put(i)));
                }
            }
            List<ProposalOutcome> one = new ArrayList<>(), many = new ArrayList<>();
            NodeDriver a = leaderOfThree(new CountingStorage(), one);
            NodeDriver b = leaderOfThree(new CountingStorage(), many);
            for (Event e : events) a.handle(e);
            for (int i = 0; i < events.size(); ) {
                int n = 1 + rng.nextInt(20);
                b.handleBatch(events.subList(i, Math.min(events.size(), i + n)));
                i += n;
            }
            assertThat(many).as("seed %d", seed).isEqualTo(one);
            assertThat(b.status()).isEqualTo(a.status());
        }
    }

    @Test
    void aReadCanWaitInTheBatchLikeAnyOtherEvent() {
        List<ProposalOutcome> outcomes = new ArrayList<>();
        List<ReadOutcome> reads = new ArrayList<>();
        NodeDriver d = leaderOfThree(new CountingStorage(), outcomes, reads);
        Bytes get = KvCodec.encode(new KvRequest(0, 0, new KvCommand.Get("k1")));

        d.handleBatch(List.of(new Event.ProposeCommand(1, put(1)), ack(2), d.prepareRead(77, get)));
        assertThat(reads).isEmpty();             // ReadIndex: waits for a heartbeat round (§6.4)
        d.handle(new Event.MessageReceived(new Message.AppendEntriesResponse(N2, N1, 1, true, 2, 0, 0, 0, 1)));

        assertThat(reads).hasSize(1);
        assertThat(KvCodec.decodeResult(((ReadOutcome.Ok) reads.get(0)).result()))
                .isEqualTo(new KvResult.Value(Optional.of("v1")));   // the read sees the write before it
        d.prepareRead(78, get);
        assertThatThrownBy(() -> d.prepareRead(78, get)).isInstanceOf(IllegalArgumentException.class);
    }

    /** MUTATION SEND_BEFORE_SYNC: the messages leave before the fsync (the simulator must catch this). */
    @Test
    void mutationSendBeforeSyncSendsFirst() {
        List<String> ops = new ArrayList<>();
        NodeDriver d = NodeDriver.create(new ScriptedCore(ready(1), ready(2)), new RecordingStorage(ops),
                m -> ops.add("send"), new KvStateMachine(), o -> {})
                .withMutationsForTesting(Set.of(ProtocolMutation.SEND_BEFORE_SYNC));
        d.handleBatch(List.of(new Event.Tick(), new Event.Tick()));
        assertThat(ops.indexOf("send")).isLessThan(ops.indexOf("sync"));
    }

    // ---------------------------------------------------------------- helpers

    private static Bytes put(int i) {
        return KvCodec.encode(new KvRequest(1, i, new KvCommand.Put("k" + (i % 3), "v" + i)));
    }

    private static NodeDriver leaderOfThree(Storage storage, List<ProposalOutcome> outcomes) {
        return leaderOfThree(storage, outcomes, new ArrayList<>());
    }

    /** n2 acknowledges the leader's log up to {@code matchIndex}, in term 1. */
    private static Event ack(long matchIndex) {
        return new Event.MessageReceived(new Message.AppendEntriesResponse(N2, N1, 1, true, matchIndex, 0, 0, 0, 0));
    }

    /** n1 leader of term 1 in a 3-node cluster (n2 voted for it); its no-op is entry 1. */
    private static NodeDriver leaderOfThree(Storage storage, List<ProposalOutcome> outcomes, List<ReadOutcome> reads) {
        RaftConfig cfg = new RaftConfig(N1, List.of(N1, N2, new NodeId(3)), 10, 2, 64);
        KvStateMachine sm = new KvStateMachine();
        NodeDriver d = NodeDriver.create(new com.projects.raftkv.core.RaftNodeCore(cfg, PersistentState.EMPTY,
                new SplittableRandom(7)), storage, m -> {}, sm, outcomes::add, reads::add, 0);
        for (int i = 0; i < 40 && d.status().soft().role() != Role.CANDIDATE; i++) d.handle(new Event.Tick());
        d.handle(new Event.MessageReceived(new Message.RequestVoteResponse(N2, N1, 1, true)));
        assertThat(d.status().soft().role()).isEqualTo(Role.LEADER);
        return d;
    }

    private static final class ScriptedCore implements RaftCore {
        private final ArrayDeque<Ready> script;
        ScriptedCore(Ready... readys) { script = new ArrayDeque<>(List.of(readys)); }
        @Override public Ready step(Event event) { return script.isEmpty() ? Ready.EMPTY : script.removeFirst(); }
        @Override public RaftStatus status() { throw new UnsupportedOperationException(); }
        @Override public LogView log() { throw new UnsupportedOperationException(); }
    }

    private record RecordingStorage(List<String> ops) implements Storage {
        @Override public void saveHardState(HardState state) { ops.add("saveHardState"); }
        @Override public void append(List<LogEntry> entries) { ops.add("append"); }
        @Override public void truncateFrom(long fromIndex) { ops.add("truncateFrom"); }
        @Override public void saveSnapshot(Snapshot snapshot) { ops.add("saveSnapshot"); }
        @Override public void compactLog(long index, long term) { ops.add("compactLog"); }
        @Override public void sync() { ops.add("sync"); }
        @Override public PersistentState loadAll() { return PersistentState.EMPTY; }
    }

    private static final class CountingStorage implements Storage {
        private final InMemoryStorage inner = new InMemoryStorage();
        long syncs;
        @Override public void saveHardState(HardState state) { inner.saveHardState(state); }
        @Override public void append(List<LogEntry> entries) { inner.append(entries); }
        @Override public void truncateFrom(long fromIndex) { inner.truncateFrom(fromIndex); }
        @Override public void saveSnapshot(Snapshot snapshot) { inner.saveSnapshot(snapshot); }
        @Override public void compactLog(long index, long term) { inner.compactLog(index, term); }
        @Override public void sync() { syncs++; inner.sync(); }
        @Override public PersistentState loadAll() { return inner.loadAll(); }
    }
}
