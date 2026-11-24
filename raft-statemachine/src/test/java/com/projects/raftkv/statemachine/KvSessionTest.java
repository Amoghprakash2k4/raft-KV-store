package com.projects.raftkv.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Client sessions (thesis §6.3): the state machine remembers, per client, the last seq it executed and
 * that request's reply, and uses them to make retried requests take effect exactly once.
 */
class KvSessionTest {

    private final KvStateMachine sm = new KvStateMachine();
    private long index;

    private KvResult apply(KvStateMachine m, long clientId, long seq, KvCommand c) {
        return KvCodec.decodeResult(m.apply(++index, KvCodec.encode(new KvRequest(clientId, seq, c))));
    }

    private KvResult apply(long clientId, long seq, KvCommand c) {
        return apply(sm, clientId, seq, c);
    }

    /** The example from EXPLANATION.md M4 §0: a retried PUT must not undo another client's later write. */
    @Test
    void aRetriedRequestReturnsTheSavedReplyAndDoesNotRunAgain() {
        KvResult first = apply(1, 7, new KvCommand.Put("x", "1"));
        apply(2, 1, new KvCommand.Put("x", "2"));

        KvResult retry = apply(1, 7, new KvCommand.Put("x", "1"));

        assertThat(retry).isEqualTo(first);
        assertThat(sm.view()).isEqualTo(Map.of("x", "2"));
    }

    @Test
    void theSavedReplyIsTheOriginalOneEvenForCas() {
        KvResult first = apply(1, 1, new KvCommand.Cas("k", Optional.empty(), "a"));
        assertThat(first).isEqualTo(new KvResult.CasOutcome(true, Optional.empty()));
        // Re-running it would now fail (k is "a"), so a wrong dedup would answer swapped=false.
        assertThat(apply(1, 1, new KvCommand.Cas("k", Optional.empty(), "a"))).isEqualTo(first);
    }

    @Test
    void anOlderSeqIsStaleAndChangesNothing() {
        apply(1, 1, new KvCommand.Put("x", "1"));
        apply(1, 2, new KvCommand.Put("x", "2"));
        long before = sm.stateHash();

        assertThat(apply(1, 1, new KvCommand.Put("x", "1"))).isEqualTo(new KvResult.Stale(2));

        assertThat(sm.view()).isEqualTo(Map.of("x", "2"));
        assertThat(sm.stateHash()).isEqualTo(before);
    }

    @Test
    void sessionsAreIndependentPerClient() {
        apply(1, 5, new KvCommand.Put("a", "1"));
        assertThat(apply(2, 1, new KvCommand.Put("a", "2"))).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(sm.sessions()).containsOnlyKeys(1L, 2L);
        assertThat(sm.sessions().get(1L).lastSeq()).isEqualTo(5);
    }

    /** Gaps in seq are fine: a client may give up on a seq (or the cluster never saw it). */
    @Test
    void aHigherSeqRunsEvenAfterAGap() {
        apply(1, 1, new KvCommand.Put("a", "1"));
        assertThat(apply(1, 9, new KvCommand.Put("a", "2"))).isEqualTo(new KvResult.Value(Optional.of("1")));
    }

    /** clientId 0 means "no session": never deduplicated (tools and tests). */
    @Test
    void clientZeroHasNoSession() {
        apply(0, 0, new KvCommand.Put("a", "1"));
        apply(0, 0, new KvCommand.Put("a", "2"));
        assertThat(sm.view()).isEqualTo(Map.of("a", "2"));
        assertThat(sm.sessions()).isEmpty();
    }

    @Test
    void aSessionRequestNeedsAPositiveSeq() {
        assertThat(apply(1, 0, new KvCommand.Put("a", "1"))).isInstanceOf(KvResult.Invalid.class);
        assertThat(sm.view()).isEmpty();
        assertThat(sm.sessions()).isEmpty();
    }

    /** The session table is replicated state: it must survive snapshot + restore exactly. */
    @Test
    void sessionsAreInTheSnapshot() {
        KvResult first = apply(1, 3, new KvCommand.Put("x", "1"));
        apply(2, 1, new KvCommand.Put("x", "2"));

        KvStateMachine restored = new KvStateMachine();
        restored.restore(index, sm.snapshot());

        assertThat(restored.sessions()).isEqualTo(sm.sessions());
        assertThat(restored.stateHash()).isEqualTo(sm.stateHash());
        assertThat(apply(restored, 1, 3, new KvCommand.Put("x", "1"))).isEqualTo(first);
        assertThat(restored.view()).isEqualTo(Map.of("x", "2"));
    }

    /** Two replicas with the same data but different sessions are NOT in the same state. */
    @Test
    void theStateHashCoversSessions() {
        KvStateMachine other = new KvStateMachine();
        apply(sm, 1, 1, new KvCommand.Put("a", "1"));
        index = 0;
        apply(other, 1, 2, new KvCommand.Put("a", "1"));
        assertThat(other.view()).isEqualTo(sm.view());
        assertThat(other.stateHash()).isNotEqualTo(sm.stateHash());
    }

    /** ReadIndex reads (thesis §6.4) are answered by query: read-only, no session, no index. */
    @Test
    void queryReadsWithoutChangingAnything() {
        apply(1, 1, new KvCommand.Put("a", "1"));
        long hash = sm.stateHash();
        long applied = sm.lastAppliedIndex();

        Bytes reply = sm.query(KvCodec.encode(new KvRequest(0, 0, new KvCommand.Get("a"))));

        assertThat(KvCodec.decodeResult(reply)).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(sm.stateHash()).isEqualTo(hash);
        assertThat(sm.lastAppliedIndex()).isEqualTo(applied);
    }

    @Test
    void queryRefusesWrites() {
        Bytes reply = sm.query(KvCodec.encode(new KvRequest(0, 0, new KvCommand.Put("a", "1"))));
        assertThat(KvCodec.decodeResult(reply)).isInstanceOf(KvResult.Invalid.class);
        assertThat(sm.view()).isEmpty();
    }

    /** Mutation SKIP_DEDUP (M4-F): the retried PUT runs again and undoes client 2's write. */
    @Test
    void withoutDedupARetryRunsTwice() {
        KvStateMachine broken = KvStateMachine.withoutDedupForTesting();
        apply(broken, 1, 7, new KvCommand.Put("x", "1"));
        apply(broken, 2, 1, new KvCommand.Put("x", "2"));
        apply(broken, 1, 7, new KvCommand.Put("x", "1"));
        assertThat(broken.view()).isEqualTo(Map.of("x", "1"));
    }

    @Test
    void snapshotWithSessionsOutOfOrderIsRejected() {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(buf)) {
            out.writeByte(2);
            out.writeInt(0);          // no keys
            out.writeInt(2);          // two sessions: client 5 then client 3
            for (long id : new long[] {5, 3}) {
                out.writeLong(id);
                out.writeLong(1);
                out.writeInt(0);      // empty reply bytes
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        assertThatThrownBy(() -> KvCodec.decodeSnapshotState(Bytes.copyOf(buf.toByteArray())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("order");
    }
}
