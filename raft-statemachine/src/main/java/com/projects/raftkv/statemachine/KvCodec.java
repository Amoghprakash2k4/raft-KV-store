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

/**
 * Hand-written binary codec (DESIGN.md M2-B). Layout, big-endian:
 * <pre>
 *   request = version:u8(=1) clientId:i64 seq:i64 op
 *   op      = 1 key value            (Put)
 *           | 2 key                  (Get)
 *           | 3 key                  (Delete)
 *           | 4 key optional newValue (Cas: optional = expected)
 *   result  = version:u8(=1) ( 1 optional | 2 bool:u8 optional | 3 string )
 *   string  = length:i32 (>= 0) utf8-bytes      optional = 0 | 1 string      bool = 0 | 1
 * </pre>
 * Canonical: each value has exactly one encoding, and the decoder rejects anything else (unknown tags,
 * non-0/1 flags, malformed UTF-8, truncation, trailing bytes) with IllegalArgumentException.
 */
public final class KvCodec {
    private static final int VERSION = 1;
    private static final int PUT = 1, GET = 2, DELETE = 3, CAS = 4;
    private static final int VALUE = 1, CAS_OUTCOME = 2, INVALID = 3;

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
                default -> throw new IllegalArgumentException("unknown result tag " + tag);
            };
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
