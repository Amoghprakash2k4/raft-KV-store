package com.projects.raftkv.storage.wal;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.storage.StorageException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Binary framing of WAL records:
 * <pre>
 *   +-------------+-------------+--------+-------------------+
 *   | length  u32 | crc32c  u32 | type u8| payload           |
 *   +-------------+-------------+--------+-------------------+
 *                  \______ crc covers type + payload ______/
 *   length = 1 + payload size (type byte included), big-endian
 * </pre>
 * {@link #scan} reads records until the bytes end cleanly or a frame is incomplete or fails its CRC.
 * A frame with a good CRC that does not decode is not a torn write; it is reported as corruption.
 */
final class WalCodec {
    static final int HEADER = 8;
    static final byte HARD_STATE = 1;
    static final byte ENTRIES = 2;
    static final byte TRUNCATE = 3;
    static final byte SNAPSHOT_MARK = 4;
    static final byte RESET = 5;

    private static final byte NOOP = 0;
    private static final byte COMMAND = 1;

    private WalCodec() {}

    static byte[] encode(WalRecord record) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(body)) {
            switch (record) {
                case WalRecord.SaveHardState(HardState hs) -> {
                    out.writeByte(HARD_STATE);
                    out.writeLong(hs.currentTerm());
                    out.writeInt(hs.votedFor().map(NodeId::value).orElse(-1));
                }
                case WalRecord.Entries(List<LogEntry> entries) -> {
                    out.writeByte(ENTRIES);
                    out.writeInt(entries.size());
                    for (LogEntry e : entries) writeEntry(out, e);
                }
                case WalRecord.Truncate(long fromIndex) -> {
                    out.writeByte(TRUNCATE);
                    out.writeLong(fromIndex);
                }
                case WalRecord.SnapshotMark(long index, long term) -> {
                    out.writeByte(SNAPSHOT_MARK);
                    out.writeLong(index);
                    out.writeLong(term);
                }
                case WalRecord.Reset r -> out.writeByte(RESET);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        byte[] typeAndPayload = body.toByteArray();
        CRC32C crc = new CRC32C();
        crc.update(typeAndPayload);
        return ByteBuffer.allocate(HEADER + typeAndPayload.length)
                .putInt(typeAndPayload.length)
                .putInt((int) crc.getValue())
                .put(typeAndPayload)
                .array();
    }

    /** Records decoded from {@code bytes}, and the offset just after the last good one. */
    record Scan(List<WalRecord> records, List<Long> ends, long validBytes, boolean clean) {}

    static Scan scan(byte[] bytes, String fileName) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        List<WalRecord> records = new ArrayList<>();
        List<Long> ends = new ArrayList<>();
        while (buf.remaining() > 0) {
            int start = buf.position();
            if (buf.remaining() < HEADER) return new Scan(records, ends, start, false);
            int length = buf.getInt();
            int storedCrc = buf.getInt();
            if (length < 1 || length > buf.remaining()) return new Scan(records, ends, start, false);
            CRC32C crc = new CRC32C();
            crc.update(bytes, buf.position(), length);
            if ((int) crc.getValue() != storedCrc) return new Scan(records, ends, start, false);
            byte[] body = new byte[length];
            buf.get(body);
            records.add(decode(body, fileName, start));
            ends.add((long) buf.position());
        }
        return new Scan(records, ends, buf.position(), true);
    }

    private static WalRecord decode(byte[] body, String fileName, int offset) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(body))) {
            WalRecord r = switch (in.readByte()) {
                case HARD_STATE -> {
                    long term = in.readLong();
                    int voted = in.readInt();
                    yield new WalRecord.SaveHardState(
                            new HardState(term, voted < 0 ? Optional.empty() : Optional.of(new NodeId(voted))));
                }
                case ENTRIES -> {
                    int n = in.readInt();
                    if (n < 1 || n > body.length) throw new IOException("bad entry count " + n);
                    List<LogEntry> entries = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) entries.add(readEntry(in));
                    yield new WalRecord.Entries(entries);
                }
                case TRUNCATE -> new WalRecord.Truncate(in.readLong());
                case SNAPSHOT_MARK -> new WalRecord.SnapshotMark(in.readLong(), in.readLong());
                case RESET -> new WalRecord.Reset();
                default -> throw new IOException("unknown record type");
            };
            if (in.available() != 0) throw new IOException("trailing bytes in record");
            return r;
        } catch (IOException | IllegalArgumentException e) {
            throw new StorageException("corrupt WAL record in " + fileName + " at offset " + offset
                    + " (CRC is valid, contents are not): " + e.getMessage(), e);
        }
    }

    private static void writeEntry(DataOutputStream out, LogEntry e) throws IOException {
        out.writeLong(e.index());
        out.writeLong(e.term());
        switch (e.payload()) {
            case EntryPayload.Noop n -> out.writeByte(NOOP);
            case EntryPayload.Command c -> {
                byte[] data = c.data().toByteArray();
                out.writeByte(COMMAND);
                out.writeInt(data.length);
                out.write(data);
            }
        }
    }

    private static LogEntry readEntry(DataInputStream in) throws IOException {
        long index = in.readLong();
        long term = in.readLong();
        EntryPayload payload = switch (in.readByte()) {
            case NOOP -> new EntryPayload.Noop();
            case COMMAND -> {
                int len = in.readInt();
                if (len < 0 || len > in.available()) throw new IOException("bad command length " + len);
                yield new EntryPayload.Command(Bytes.copyOf(in.readNBytes(len)));
            }
            default -> throw new IOException("unknown payload type");
        };
        return new LogEntry(index, term, payload);
    }
}
