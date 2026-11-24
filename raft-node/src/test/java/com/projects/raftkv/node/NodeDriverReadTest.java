package com.projects.raftkv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.ClientNotice;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import com.projects.raftkv.statemachine.KvResult;
import com.projects.raftkv.statemachine.KvStateMachine;
import com.projects.raftkv.storage.InMemoryStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The driver's half of ReadIndex (thesis §6.4 step 4): serve a ready read only once lastApplied >= readIndex. */
class NodeDriverReadTest {

    private static final Bytes PUT_A1 = KvCodec.encode(new KvRequest(1, 1, new KvCommand.Put("a", "1")));
    private static final Bytes PUT_A2 = KvCodec.encode(new KvRequest(1, 2, new KvCommand.Put("a", "2")));
    private static final Bytes GET_A = KvCodec.encode(new KvRequest(0, 0, new KvCommand.Get("a")));

    private final List<ReadOutcome> reads = new ArrayList<>();

    private static Ready ready(List<LogEntry> apply, List<ClientNotice> notices) {
        return new Ready(List.of(), List.of(), apply, notices, Optional.empty());
    }

    private static LogEntry put(long index, Bytes cmd) {
        return new LogEntry(index, 1, new EntryPayload.Command(cmd));
    }

    private NodeDriver driver(Ready... script) {
        return NodeDriver.create(new ScriptedCore(script), new InMemoryStorage(), m -> {}, new KvStateMachine(),
                o -> {}, reads::add, 0);
    }

    private static Bytes value(String v) {
        return KvCodec.encode(new KvResult.Value(Optional.ofNullable(v)));
    }

    @Test
    void aReadyReadIsAnsweredFromTheStateMachine() {
        NodeDriver d = driver(ready(List.of(put(1, PUT_A1)), List.of()),
                ready(List.of(), List.of(new ClientNotice.ReadReady(5, 1))));
        d.handle(new Event.Tick());
        d.read(5, GET_A);
        assertThat(reads).containsExactly(new ReadOutcome.Ok(5, 1, value("1")));
    }

    /** Thesis §6.4 step 4: the state machine must have applied at least through readIndex. */
    @Test
    void aReadWaitsUntilItsReadIndexIsApplied() {
        NodeDriver d = driver(ready(List.of(put(1, PUT_A1)), List.of(new ClientNotice.ReadReady(5, 3))),
                ready(List.of(new LogEntry(2, 1, new EntryPayload.Noop())), List.of()),
                ready(List.of(put(3, PUT_A2)), List.of()));
        d.read(5, GET_A);
        assertThat(reads).isEmpty();
        d.handle(new Event.Tick());              // index 2: a no-op still counts as applied
        assertThat(reads).isEmpty();
        d.handle(new Event.Tick());              // index 3
        assertThat(reads).containsExactly(new ReadOutcome.Ok(5, 3, value("2")));
    }

    /** Entries applied in the same Ready as the release come first (Ready order: apply, then notify). */
    @Test
    void entriesInTheSameReadyAreAppliedBeforeTheReadIsServed() {
        NodeDriver d = driver(ready(List.of(put(1, PUT_A1)), List.of(new ClientNotice.ReadReady(5, 1))));
        d.read(5, GET_A);
        assertThat(reads).containsExactly(new ReadOutcome.Ok(5, 1, value("1")));
    }

    @Test
    void aRejectedReadIsReportedWithTheHint() {
        NodeDriver d = driver(ready(List.of(), List.of(new ClientNotice.ReadRejected(5, Optional.of(new NodeId(2))))));
        d.read(5, GET_A);
        assertThat(reads).containsExactly(new ReadOutcome.Rejected(5, Optional.of(new NodeId(2))));
    }

    @Test
    void aReadIdCanOnlyBeInFlightOnce() {
        NodeDriver d = driver();
        d.read(5, GET_A);
        assertThatThrownBy(() -> d.read(5, GET_A)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Hands out pre-built Readys in order. */
    private static final class ScriptedCore implements RaftCore {
        private final java.util.ArrayDeque<Ready> script;
        ScriptedCore(Ready... readys) { script = new java.util.ArrayDeque<>(List.of(readys)); }
        @Override public Ready step(Event event) { return script.isEmpty() ? Ready.EMPTY : script.removeFirst(); }
        @Override public RaftStatus status() { throw new UnsupportedOperationException(); }
        @Override public LogView log() { throw new UnsupportedOperationException(); }
    }
}
