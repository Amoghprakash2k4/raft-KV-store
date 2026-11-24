package com.projects.raftkv.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;

/** Snapshots of the KV store (DESIGN.md M3-E): canonical bytes, exact restore. */
class KvSnapshotTest {

    private static void put(KvStateMachine sm, long index, String k, String v) {
        sm.apply(index, KvCodec.encode(new KvRequest(1, index, new KvCommand.Put(k, v))));
    }

    @Test
    void restoreReproducesTheStateAndTheAppliedIndex() {
        KvStateMachine a = new KvStateMachine();
        put(a, 1, "x", "1");
        put(a, 2, "y", "2");
        put(a, 5, "x", "3");

        KvStateMachine b = new KvStateMachine();
        b.restore(7, a.snapshot());     // the snapshot index may be a no-op after the last command

        assertThat(b.view()).isEqualTo(Map.of("x", "3", "y", "2"));
        assertThat(b.stateHash()).isEqualTo(a.stateHash());
        assertThat(b.lastAppliedIndex()).isEqualTo(7);
        assertThatThrownBy(() -> put(b, 7, "z", "0")).isInstanceOf(IllegalStateException.class);
        put(b, 8, "z", "0");
        assertThat(b.view()).containsEntry("z", "0");
    }

    @Test
    void restoreReplacesEverythingThatWasThere() {
        KvStateMachine sm = new KvStateMachine();
        put(sm, 1, "old", "1");
        KvStateMachine other = new KvStateMachine();
        put(other, 1, "new", "2");
        sm.restore(10, other.snapshot());
        assertThat(sm.view()).isEqualTo(Map.of("new", "2"));
    }

    @Test
    void equalStatesGiveIdenticalBytesWhateverTheHistory() {
        KvStateMachine a = new KvStateMachine();
        put(a, 1, "b", "2");
        put(a, 2, "a", "1");
        KvStateMachine b = new KvStateMachine();
        put(b, 1, "a", "0");
        put(b, 2, "a", "1");
        put(b, 3, "b", "2");
        assertThat(a.snapshot()).isEqualTo(b.snapshot());
    }

    @Test
    void malformedSnapshotsAreRejected() {
        Bytes good = KvCodec.encodeSnapshot(new TreeMap<>(Map.of("a", "1", "b", "2")));
        byte[] raw = good.toByteArray();
        assertThatThrownBy(() -> KvCodec.decodeSnapshot(Bytes.copyOf(java.util.Arrays.copyOf(raw, raw.length - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] longer = java.util.Arrays.copyOf(raw, raw.length + 1);
        assertThatThrownBy(() -> KvCodec.decodeSnapshot(Bytes.copyOf(longer))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keysOutOfOrderAreNotCanonical() {
        // Hand-build a snapshot with keys "b" then "a".
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(buf)) {
            out.writeByte(1);
            out.writeInt(2);
            for (String s : List.of("b", "2", "a", "1")) {
                out.writeInt(1);
                out.write(s.getBytes());
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        assertThatThrownBy(() -> KvCodec.decodeSnapshot(Bytes.copyOf(buf.toByteArray())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("order");
    }

    @Property(tries = 200)
    void snapshotRoundTrips(@ForAll @Size(max = 20) Map<String, String> contents) {
        TreeMap<String, String> m = new TreeMap<>(contents);
        assertThat(KvCodec.decodeSnapshot(KvCodec.encodeSnapshot(m))).isEqualTo(m);
    }
}
