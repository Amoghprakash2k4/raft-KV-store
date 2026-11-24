package com.projects.raftkv.statemachine;

import com.projects.raftkv.core.Bytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Map;

/**
 * Hand-written binary codec (DESIGN.md M2-B). Layout, big-endian:
 * <pre>
 *   request = version:u8(=1) clientId:i64 seq:i64 op
 *   op      = 1 key value            (Put)
 *           | 2 key                  (Get)
 *           | 3 key                  (Delete)
 *           | 4 key optional newValue (Cas: optional = expected)
 *   result  = version:u8(=1) ( 1 optional | 2 bool:u8 optional | 3 string | 4 lastSeq:i64 )
 *   string  = length:i32 (>= 0) utf8-bytes      optional = 0 | 1 string      bool = 0 | 1
 * </pre>
 * Canonical: each value has exactly one encoding, and the decoder rejects anything else (unknown tags,
 * non-0/1 flags, malformed UTF-8, truncation, trailing bytes) with IllegalArgumentException.
 */
public final class KvCodec {
    private static final int VERSION = 1;
    private static final int PUT = 1, GET = 2, DELETE = 3, CAS = 4;
    private static final int VALUE = 1, CAS_OUTCOME = 2, INVALID = 3, STALE = 4;
    /** Snapshot format 2 (M4) adds the client session table after the data. */
    private static final int SNAPSHOT_VERSION = 2;

    private KvCodec() {}

    public static Bytes encode(KvRequest r) {
        return write(out -> {
            out.writeByte(VERSION);
            out.writeLong(r.clientId());
            out.writeLong(r.seq());
            switch (r.command()) {
                case KvCommand.Put p -> { out.writeByte(PUT); writeString(out, p.key()); writeString(out, p.value()); }
                case KvCommand.Get g -> { out.writeByte(GET); writeString(out, g.key()); }
                case KvCommand.Delete d -> { out.writeByte(DELETE); writeString(out, d.key()); }
                case KvCommand.Cas c -> {
                    out.writeByte(CAS);
                    writeString(out, c.key());
                    writeOptional(out, c.expected());
                    writeString(out, c.newValue());
                }
            }
        });
    }

    public static Bytes encode(KvResult r) {
        return write(out -> {
            out.writeByte(VERSION);
            switch (r) {
                case KvResult.Value v -> { out.writeByte(VALUE); writeOptional(out, v.value()); }
                case KvResult.CasOutcome c -> {
                    out.writeByte(CAS_OUTCOME);
                    out.writeByte(c.swapped() ? 1 : 0);
                    writeOptional(out, c.previous());
                }
                case KvResult.Invalid i -> { out.writeByte(INVALID); writeString(out, i.reason()); }
                case KvResult.Stale st -> { out.writeByte(STALE); out.writeLong(st.lastSeq()); }
            }
        });
    }

    public static KvRequest decodeRequest(Bytes b) {
        return read(b, in -> {
            readVersion(in);
            long clientId = in.readLong();
            long seq = in.readLong();
            int tag = in.readUnsignedByte();
            KvCommand cmd = switch (tag) {
                case PUT -> new KvCommand.Put(readString(in), readString(in));
                case GET -> new KvCommand.Get(readString(in));
                case DELETE -> new KvCommand.Delete(readString(in));
                case CAS -> new KvCommand.Cas(readString(in), readOptional(in), readString(in));
                default -> throw new IllegalArgumentException("unknown command tag " + tag);
            };
            return new KvRequest(clientId, seq, cmd);
        });
    }

    public static KvResult decodeResult(Bytes b) {
        return read(b, in -> {
            readVersion(in);
            int tag = in.readUnsignedByte();
            return switch (tag) {
                case VALUE -> new KvResult.Value(readOptional(in));
                case CAS_OUTCOME -> new KvResult.CasOutcome(readFlag(in), readOptional(in));
                case INVALID -> new KvResult.Invalid(readString(in));
                case STALE -> new KvResult.Stale(in.readLong());
                default -> throw new IllegalArgumentException("unknown result tag " + tag);
            };
        });
    }

    // ---------------------------------------------------------------- snapshots

    /** Everything a snapshot holds: the key-value data and the client session table (thesis §6.3). */
    public record SnapshotState(TreeMap<String, String> data, TreeMap<Long, KvStateMachine.Session> sessions) {}

    /** A snapshot of data with no sessions. */
    public static Bytes encodeSnapshot(SortedMap<String, String> data) {
        return encodeSnapshot(data, new TreeMap<>());
    }

    /**
     * Snapshot format 2:
     * <pre>
     *   version u8(=2), count i32, (key string, value string)*        keys strictly ascending
     *   sessions i32, (clientId i64, lastSeq i64, reply length-prefixed bytes)*   clientIds strictly ascending
     * </pre>
     * Equal states encode to identical bytes (DESIGN.md E1a rule 4).
     */
    public static Bytes encodeSnapshot(SortedMap<String, String> data, SortedMap<Long, KvStateMachine.Session> sessions) {
        if (data.comparator() != null || sessions.comparator() != null)
            throw new IllegalArgumentException("natural key order required");
        return write(out -> {
            out.writeByte(SNAPSHOT_VERSION);
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                writeString(out, e.getKey());
                writeString(out, e.getValue());
            }
            out.writeInt(sessions.size());
            for (Map.Entry<Long, KvStateMachine.Session> e : sessions.entrySet()) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue().lastSeq());
                byte[] reply = e.getValue().reply().toByteArray();
                out.writeInt(reply.length);
                out.write(reply);
            }
        });
    }

    /** The data part of a snapshot (tests). */
    public static TreeMap<String, String> decodeSnapshot(Bytes b) {
        return decodeSnapshotState(b).data();
    }

    /** Strict inverse of {@link #encodeSnapshot}: rejects anything that is not exactly canonical. */
    public static SnapshotState decodeSnapshotState(Bytes b) {
        return read(b, in -> {
            int v = in.readUnsignedByte();
            if (v != SNAPSHOT_VERSION) throw new IllegalArgumentException("unsupported snapshot version " + v);
            int n = in.readInt();
            if (n < 0 || n > in.available()) throw new IllegalArgumentException("bad entry count " + n);
            TreeMap<String, String> m = new TreeMap<>();
            String prev = null;
            for (int i = 0; i < n; i++) {
                String k = readString(in);
                String val = readString(in);
                if (prev != null && prev.compareTo(k) >= 0)
                    throw new IllegalArgumentException("snapshot keys out of order: " + prev + " then " + k);
                m.put(k, val);
                prev = k;
            }
            int sn = in.readInt();
            if (sn < 0 || sn > in.available()) throw new IllegalArgumentException("bad session count " + sn);
            TreeMap<Long, KvStateMachine.Session> sessions = new TreeMap<>();
            long prevId = Long.MIN_VALUE;
            for (int i = 0; i < sn; i++) {
                long id = in.readLong();
                long lastSeq = in.readLong();
                int len = in.readInt();
                if (len < 0 || len > in.available()) throw new IllegalArgumentException("bad reply length " + len);
                byte[] reply = in.readNBytes(len);
                if (i > 0 && prevId >= id)
                    throw new IllegalArgumentException("snapshot sessions out of order: " + prevId + " then " + id);
                if (id == 0 || lastSeq < 1) throw new IllegalArgumentException("bad session " + id + "/" + lastSeq);
                sessions.put(id, new KvStateMachine.Session(lastSeq, Bytes.copyOf(reply)));
                prevId = id;
            }
            return new SnapshotState(m, sessions);
        });
    }

    // ---------------------------------------------------------------- plumbing

    private interface Writer { void write(DataOutputStream out) throws IOException; }

    private interface Reader<T> { T read(DataInputStream in) throws IOException; }

    private static Bytes write(Writer w) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buf)) {
            w.write(out);
        } catch (IOException e) {
            throw new UncheckedIOException("in-memory write failed", e);
        }
        return Bytes.copyOf(buf.toByteArray());
    }

    private static <T> T read(Bytes b, Reader<T> r) {
        byte[] raw = b.toByteArray();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
        try {
            T value = r.read(in);
            if (in.available() > 0) throw new IllegalArgumentException(in.available() + " trailing bytes");
            return value;
        } catch (EOFException e) {
            throw new IllegalArgumentException("truncated input", e);
        } catch (IOException e) {
            throw new UncheckedIOException("in-memory read failed", e);
        }
    }

    private static void readVersion(DataInputStream in) throws IOException {
        int v = in.readUnsignedByte();
        if (v != VERSION) throw new IllegalArgumentException("unsupported version " + v);
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(utf8.length);
        out.write(utf8);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > in.available()) throw new IllegalArgumentException("bad string length " + len);
        byte[] utf8 = in.readNBytes(len);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(utf8)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("malformed UTF-8", e);
        }
    }

    private static void writeOptional(DataOutputStream out, Optional<String> o) throws IOException {
        out.writeByte(o.isPresent() ? 1 : 0);
        if (o.isPresent()) writeString(out, o.get());
    }

    private static Optional<String> readOptional(DataInputStream in) throws IOException {
        return readFlag(in) ? Optional.of(readString(in)) : Optional.empty();
    }

    private static boolean readFlag(DataInputStream in) throws IOException {
        int f = in.readUnsignedByte();
        if (f > 1) throw new IllegalArgumentException("bad flag byte " + f);
        return f == 1;
    }
}
