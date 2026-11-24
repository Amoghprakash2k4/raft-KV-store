package com.projects.raftkv.statemachine;

import com.projects.raftkv.core.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The key-value store. A TreeMap keeps iteration order sorted, so the state hash and the snapshot
 * bytes depend only on contents, never on insertion history (DESIGN.md E1a rule 4).
 * M4 adds the client session table for exactly-once semantics.
 */
public final class KvStateMachine implements StateMachine {
    private TreeMap<String, String> data = new TreeMap<>();
    private long lastApplied;

    @Override
    public Bytes apply(long index, Bytes command) {
        Objects.requireNonNull(command);
        if (index <= lastApplied)
            throw new IllegalStateException("apply out of order: index " + index + " after " + lastApplied);
        lastApplied = index;
        KvRequest request;
        try {
            request = KvCodec.decodeRequest(command);
        } catch (IllegalArgumentException e) {
            return KvCodec.encode(new KvResult.Invalid(e.getMessage()));
        }
        return KvCodec.encode(execute(request.command()));
    }

    private KvResult execute(KvCommand c) {
        return switch (c) {
            case KvCommand.Put p -> new KvResult.Value(Optional.ofNullable(data.put(p.key(), p.value())));
            case KvCommand.Get g -> new KvResult.Value(Optional.ofNullable(data.get(g.key())));
            case KvCommand.Delete d -> new KvResult.Value(Optional.ofNullable(data.remove(d.key())));
            case KvCommand.Cas cas -> {
                Optional<String> current = Optional.ofNullable(data.get(cas.key()));
                boolean swap = current.equals(cas.expected());
                if (swap) data.put(cas.key(), cas.newValue());
                yield new KvResult.CasOutcome(swap, current);
            }
        };
    }

    @Override
    public long lastAppliedIndex() { return lastApplied; }

    @Override
    public Bytes snapshot() { return KvCodec.encodeSnapshot(data); }

    @Override
    public void restore(long index, Bytes snapshot) {
        if (index < 0) throw new IllegalArgumentException("negative snapshot index");
        data = KvCodec.decodeSnapshot(snapshot);
        lastApplied = index;
    }

    /** Read-only, sorted view of the contents (tests and the simulator). */
    public SortedMap<String, String> view() { return Collections.unmodifiableSortedMap(data); }

    /**
     * 64-bit FNV-1a over the sorted (key, value) pairs, length-prefixed so ("ab","c") and ("a","bc")
     * differ. Replicas at the same lastApplied must have the same hash.
     */
    public long stateHash() {
        long h = 0xcbf29ce484222325L;
        for (Map.Entry<String, String> e : data.entrySet()) {
            h = mix(h, e.getKey());
            h = mix(h, e.getValue());
        }
        return h;
    }

    private static long mix(long h, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        for (int shift = 24; shift >= 0; shift -= 8) h = fnv(h, (byte) (b.length >>> shift));
        for (byte x : b) h = fnv(h, x);
        return h;
    }

    private static long fnv(long h, byte x) {
        return (h ^ (x & 0xff)) * 0x100000001b3L;
    }
}
