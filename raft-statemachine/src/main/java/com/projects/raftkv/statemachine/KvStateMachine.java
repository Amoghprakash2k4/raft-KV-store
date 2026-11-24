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
 * The key-value store, plus the client session table that makes writes exactly-once (thesis §6.3).
 * TreeMaps keep iteration order sorted, so the state hash and the snapshot bytes depend only on
 * contents, never on insertion history (DESIGN.md E1a rule 4).
 *
 * <p>Dedup happens here, at apply time, so every replica makes the same decision for the same log
 * entry: the session table is replicated state like the data, and travels in snapshots.
 */
public final class KvStateMachine implements StateMachine {

    /** What the cluster remembers about one client: the last seq it executed and that request's reply. */
    public record Session(long lastSeq, Bytes reply) {
        public Session { Objects.requireNonNull(reply); }
    }

    private TreeMap<String, String> data = new TreeMap<>();
    private TreeMap<Long, Session> sessions = new TreeMap<>();
    private long lastApplied;
    private final boolean dedup;
    private long duplicatesAnswered;   // a statistic, not replicated state

    public KvStateMachine() { this(true); }

    private KvStateMachine(boolean dedup) { this.dedup = dedup; }

    /** MUTATION (test only, M4-F SKIP_DEDUP): executes duplicates again instead of replying from the session. */
    public static KvStateMachine withoutDedupForTesting() { return new KvStateMachine(false); }

    /**
     * Thesis §6.3. For a request from client c with sequence number s (c != 0):
     * <ul>
     *   <li>s == lastSeq[c]: a duplicate (a retry, or the network delivered it twice). Return the saved
     *       reply; do not execute again.</li>
     *   <li>s &lt; lastSeq[c]: an old copy the client has moved past. Reply {@link KvResult.Stale}.</li>
     *   <li>s &gt; lastSeq[c] (or no session yet): execute, and save (s, reply) as the session.</li>
     * </ul>
     * Client 0 has no session and is never deduplicated (tools and tests). Undecodable bytes are
     * answered {@link KvResult.Invalid} and change nothing.
     */
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
        long client = request.clientId();
        if (client == 0) return KvCodec.encode(execute(request.command()));
        if (request.seq() < 1) return KvCodec.encode(new KvResult.Invalid("seq must be >= 1 with a session"));

        Session s = sessions.get(client);
        if (s != null && dedup) {                                   // MUTATION (test only) when !dedup
            if (request.seq() == s.lastSeq()) {
                duplicatesAnswered++;
                return s.reply();
            }
            if (request.seq() < s.lastSeq()) return KvCodec.encode(new KvResult.Stale(s.lastSeq()));
        }
        Bytes reply = KvCodec.encode(execute(request.command()));
        if (s == null || request.seq() > s.lastSeq()) sessions.put(client, new Session(request.seq(), reply));
        return reply;
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

    /** Thesis §6.4: a read served outside the log. Only {@link KvCommand.Get} is allowed. */
    @Override
    public Bytes query(Bytes query) {
        Objects.requireNonNull(query);
        KvRequest request;
        try {
            request = KvCodec.decodeRequest(query);
        } catch (IllegalArgumentException e) {
            return KvCodec.encode(new KvResult.Invalid(e.getMessage()));
        }
        if (!(request.command() instanceof KvCommand.Get g))
            return KvCodec.encode(new KvResult.Invalid("a query must be a Get"));
        return KvCodec.encode(new KvResult.Value(Optional.ofNullable(data.get(g.key()))));
    }

    @Override
    public long lastAppliedIndex() { return lastApplied; }

    @Override
    public Bytes snapshot() { return KvCodec.encodeSnapshot(data, sessions); }

    @Override
    public void restore(long index, Bytes snapshot) {
        if (index < 0) throw new IllegalArgumentException("negative snapshot index");
        KvCodec.SnapshotState state = KvCodec.decodeSnapshotState(snapshot);
        data = state.data();
        sessions = state.sessions();
        lastApplied = index;
    }

    /** Read-only, sorted view of the contents (tests and the simulator). */
    public SortedMap<String, String> view() { return Collections.unmodifiableSortedMap(data); }

    /** How many applied entries were duplicates answered from a session (this incarnation; not replicated state). */
    public long duplicatesAnswered() { return duplicatesAnswered; }

    /** Read-only, sorted view of the session table (tests and the simulator). */
    public SortedMap<Long, Session> sessions() { return Collections.unmodifiableSortedMap(sessions); }

    /**
     * 64-bit FNV-1a over the sorted (key, value) pairs and then the sorted sessions, length-prefixed so
     * ("ab","c") and ("a","bc") differ. Replicas at the same lastApplied must have the same hash:
     * sessions included, because a replica with a different session table would dedup differently.
     */
    public long stateHash() {
        long h = 0xcbf29ce484222325L;
        for (Map.Entry<String, String> e : data.entrySet()) {
            h = mix(h, e.getKey().getBytes(StandardCharsets.UTF_8));
            h = mix(h, e.getValue().getBytes(StandardCharsets.UTF_8));
        }
        h = mixLong(h, sessions.size());
        for (Map.Entry<Long, Session> e : sessions.entrySet()) {
            h = mixLong(h, e.getKey());
            h = mixLong(h, e.getValue().lastSeq());
            h = mix(h, e.getValue().reply().toByteArray());
        }
        return h;
    }

    private static long mix(long h, byte[] b) {
        for (int shift = 24; shift >= 0; shift -= 8) h = fnv(h, (byte) (b.length >>> shift));
        for (byte x : b) h = fnv(h, x);
        return h;
    }

    private static long mixLong(long h, long v) {
        for (int shift = 56; shift >= 0; shift -= 8) h = fnv(h, (byte) (v >>> shift));
        return h;
    }

    private static long fnv(long h, byte x) {
        return (h ^ (x & 0xff)) * 0x100000001b3L;
    }
}
