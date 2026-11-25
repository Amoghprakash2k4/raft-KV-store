package com.projects.raftkv.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.proto.RaftMessage;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Every message survives Message → protobuf → bytes → protobuf → Message unchanged (DESIGN.md M5-B). */
class ProtoMappingProperties {

    @Property(tries = 2000)
    void everyMessageRoundTripsThroughTheWireFormat(@ForAll("messages") Message m) throws Exception {
        byte[] wire = ProtoMapping.toProto(m).toByteArray();
        assertThat(ProtoMapping.fromProto(RaftMessage.parseFrom(wire))).isEqualTo(m);
    }

    @Example
    void extremeValuesRoundTrip() {
        long max = Long.MAX_VALUE;
        Message ae = new Message.AppendEntries(new NodeId(1), new NodeId(2), max, max, max,
                List.of(new LogEntry(max, max, new EntryPayload.Command(Bytes.EMPTY)), new LogEntry(1, 1, new EntryPayload.Noop())),
                max, max);
        assertThat(ProtoMapping.fromProto(ProtoMapping.toProto(ae))).isEqualTo(ae);
    }

    @Example
    void aMessageWithoutABodyIsRejected() {
        RaftMessage empty = RaftMessage.newBuilder().setFrom(1).setTo(2).setTerm(3).build();
        assertThatThrownBy(() -> ProtoMapping.fromProto(empty)).isInstanceOf(IllegalArgumentException.class);
    }

    @Example
    void invalidFieldsAreRejectedByTheRecordConstructors() {
        RaftMessage badEntry = ProtoMapping.toProto(new Message.AppendEntries(new NodeId(1), new NodeId(2), 1, 0, 0,
                List.of(new LogEntry(1, 1, new EntryPayload.Noop())), 0)).toBuilder()
                .setAppendEntries(com.projects.raftkv.proto.AppendEntries.newBuilder()
                        .addEntries(com.projects.raftkv.proto.LogEntry.newBuilder().setIndex(0).setTerm(1)
                                .setNoop(com.projects.raftkv.proto.Noop.getDefaultInstance())))
                .build();
        assertThatThrownBy(() -> ProtoMapping.fromProto(badEntry)).isInstanceOf(IllegalArgumentException.class);
        RaftMessage negativeId = RaftMessage.newBuilder().setFrom(-1).setTo(2)
                .setRequestVoteResponse(com.projects.raftkv.proto.RequestVoteResponse.getDefaultInstance()).build();
        assertThatThrownBy(() -> ProtoMapping.fromProto(negativeId)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- generators

    private record Header(NodeId from, NodeId to, long term) {}

    private static Arbitrary<Long> nat() {
        return Arbitraries.oneOf(Arbitraries.longs().between(0, 1000), Arbitraries.longs().between(0, Long.MAX_VALUE));
    }

    private static Arbitrary<Long> positive() {
        return Arbitraries.oneOf(Arbitraries.longs().between(1, 1000), Arbitraries.longs().between(1, Long.MAX_VALUE));
    }

    private static Arbitrary<Header> headers() {
        Arbitrary<NodeId> ids = Arbitraries.integers().between(0, 10).map(NodeId::new);
        return Combinators.combine(ids, ids, nat()).as(Header::new);
    }

    private static Arbitrary<Bytes> bytes() {
        return Arbitraries.bytes().array(byte[].class).ofMaxSize(100).map(Bytes::copyOf);
    }

    private static Arbitrary<LogEntry> entries() {
        Arbitrary<EntryPayload> payload = Arbitraries.oneOf(
                Arbitraries.just(new EntryPayload.Noop()), bytes().map(EntryPayload.Command::new));
        return Combinators.combine(positive(), positive(), payload).as(LogEntry::new);
    }

    @Provide
    Arbitrary<Message> messages() {
        Arbitrary<Boolean> b = Arbitraries.of(true, false);
        Arbitrary<Message> rv = Combinators.combine(headers(), nat(), nat())
                .as((h, i, t) -> new Message.RequestVote(h.from(), h.to(), h.term(), i, t));
        Arbitrary<Message> rvr = Combinators.combine(headers(), b)
                .as((h, g) -> new Message.RequestVoteResponse(h.from(), h.to(), h.term(), g));
        Arbitrary<Message> pv = Combinators.combine(headers(), nat(), nat())
                .as((h, i, t) -> new Message.PreVote(h.from(), h.to(), h.term(), i, t));
        Arbitrary<Message> pvr = Combinators.combine(headers(), b)
                .as((h, g) -> new Message.PreVoteResponse(h.from(), h.to(), h.term(), g));
        Arbitrary<Message> ae = Combinators.combine(headers(), nat(), nat(), entries().list().ofMaxSize(5), nat(), nat())
                .as((h, pi, pt, es, c, r) -> new Message.AppendEntries(h.from(), h.to(), h.term(), pi, pt, es, c, r));
        Arbitrary<Message> aer = Combinators.combine(headers(), b, nat(), nat(), nat(), nat(), nat())
                .as((h, ok, m, ri, ct, ci, r) -> new Message.AppendEntriesResponse(h.from(), h.to(), h.term(), ok, m, ri, ct, ci, r));
        Arbitrary<Message> is = Combinators.combine(headers(), positive(), positive(), nat(), bytes(), b, nat())
                .as((h, i, t, o, d, done, r) -> new Message.InstallSnapshot(h.from(), h.to(), h.term(), i, t, o, d, done, r));
        Arbitrary<Message> isr = Combinators.combine(headers(), nat(), nat(), b, nat())
                .as((h, i, o, done, r) -> new Message.InstallSnapshotResponse(h.from(), h.to(), h.term(), i, o, done, r));
        return Arbitraries.oneOf(rv, rvr, pv, pvr, ae, aer, is, isr);
    }
}
