package com.projects.raftkv.statemachine;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.Bytes;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KvStateMachineTest {

    private final KvStateMachine sm = new KvStateMachine();
    private long index;

    private KvResult run(KvCommand c) {
        return KvCodec.decodeResult(sm.apply(++index, KvCodec.encode(new KvRequest(1, index, c))));
    }

    @Test
    void putReturnsThePreviousValue() {
        assertThat(run(new KvCommand.Put("a", "1"))).isEqualTo(new KvResult.Value(Optional.empty()));
        assertThat(run(new KvCommand.Put("a", "2"))).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(sm.view()).isEqualTo(Map.of("a", "2"));
    }

    @Test
    void getAndDelete() {
        run(new KvCommand.Put("a", "1"));
        assertThat(run(new KvCommand.Get("a"))).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(run(new KvCommand.Delete("a"))).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(run(new KvCommand.Get("a"))).isEqualTo(new KvResult.Value(Optional.empty()));
        assertThat(sm.view()).isEmpty();
    }

    @Test
    void casSwapsOnlyWhenTheCurrentValueMatches() {
        assertThat(run(new KvCommand.Cas("a", Optional.empty(), "1")))
                .isEqualTo(new KvResult.CasOutcome(true, Optional.empty()));
        assertThat(run(new KvCommand.Cas("a", Optional.of("x"), "2")))
                .isEqualTo(new KvResult.CasOutcome(false, Optional.of("1")));
        assertThat(run(new KvCommand.Cas("a", Optional.of("1"), "2")))
                .isEqualTo(new KvResult.CasOutcome(true, Optional.of("1")));
        assertThat(sm.view()).isEqualTo(Map.of("a", "2"));
    }

    /** E1a rule 4: apply must be deterministic even for garbage; it answers Invalid and changes nothing. */
    @Test
    void malformedCommandIsAnsweredInvalidWithoutChangingState() {
        run(new KvCommand.Put("a", "1"));
        long before = sm.stateHash();

        Bytes reply = sm.apply(++index, Bytes.copyOf(new byte[] {9, 9, 9}));

        assertThat(KvCodec.decodeResult(reply)).isInstanceOf(KvResult.Invalid.class);
        assertThat(sm.stateHash()).isEqualTo(before);
    }

    /** Indexes must be applied in order, each exactly once (State Machine Safety, §5.4.3). */
    @Test
    void rejectsOutOfOrderApply() {
        sm.apply(1, KvCodec.encode(new KvRequest(1, 1, new KvCommand.Get("a"))));
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> sm.apply(1, KvCodec.encode(new KvRequest(1, 1, new KvCommand.Get("a")))))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Equal contents give equal hashes no matter the order the keys were written in. */
    @Test
    void stateHashDependsOnlyOnContents() {
        KvStateMachine other = new KvStateMachine();
        run(new KvCommand.Put("a", "1"));
        run(new KvCommand.Put("b", "2"));
        other.apply(1, KvCodec.encode(new KvRequest(2, 1, new KvCommand.Put("b", "2"))));
        other.apply(2, KvCodec.encode(new KvRequest(2, 2, new KvCommand.Put("a", "1"))));

        assertThat(other.stateHash()).isEqualTo(sm.stateHash());
        run(new KvCommand.Put("b", "3"));
        assertThat(other.stateHash()).isNotEqualTo(sm.stateHash());
    }

    /** Raft no-op entries are skipped by the driver but still occupy an index. */
    @Test
    void skippedIndexesAreAllowedButNeverBackwards() {
        sm.apply(3, KvCodec.encode(new KvRequest(1, 1, new KvCommand.Put("a", "1"))));
        assertThat(sm.lastAppliedIndex()).isEqualTo(3);
    }
}
