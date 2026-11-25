package com.projects.raftkv.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.proto.RaftMessage;
import com.projects.raftkv.proto.RaftPeerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** GrpcTransport + GrpcPeerServer over in-process gRPC (same code path as TCP, minus the sockets). */
class GrpcTransportTest {
    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);

    private final String name = "peer-n2-" + UUID.randomUUID();
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (AutoCloseable c : toClose.reversed()) c.close();
    }

    private GrpcPeerServer server() {
        GrpcPeerServer s = GrpcPeerServer.start(InProcessServerBuilder.forName(name), inbox::add);
        toClose.add(s);
        return s;
    }

    private GrpcTransport transport(int capacity) {
        GrpcTransport t = new GrpcTransport(List.of(N2), id -> InProcessChannelBuilder.forName(name).build(), capacity);
        toClose.add(t);
        return t;
    }

    private static Message heartbeat(long term) {
        return new Message.AppendEntries(N1, N2, term, 0, 0, List.of(), 0, term);
    }

    @Test
    void messagesArriveInOrderAndUnchanged() throws Exception {
        server();
        GrpcTransport t = transport(1000);
        for (int i = 1; i <= 200; i++) t.send(heartbeat(i));

        for (int i = 1; i <= 200; i++) {
            assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo(heartbeat(i));
        }
        assertThat(t.sent(N2)).isEqualTo(200);
        assertThat(t.dropped(N2)).isZero();
    }

    @Test
    void aRestartedPeerIsReachedAgain() throws Exception {
        GrpcPeerServer first = server();
        GrpcTransport t = transport(1000);
        t.send(heartbeat(1));
        assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo(heartbeat(1));

        first.close();                                  // the peer goes down...
        for (int i = 0; i < 20; i++) t.send(heartbeat(2));
        server();                                       // ...and comes back under the same address

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        Message got = null;
        long term = 3;
        while (got == null && System.nanoTime() < deadline) {
            t.send(heartbeat(term++));                  // like the leader's heartbeats: keep sending
            got = inbox.poll(50, TimeUnit.MILLISECONDS);
        }
        assertThat(got).isNotNull();
    }

    @Test
    void anUnreachablePeerCostsBoundedMemoryAndNeverBlocksTheSender() {
        GrpcTransport t = transport(10);                // no server at this address
        long start = System.nanoTime();
        for (int i = 0; i < 10_000; i++) t.send(heartbeat(i + 1));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(2_000);
        assertThat(t.queued(N2)).isLessThanOrEqualTo(10);
        assertThat(t.dropped(N2)).isGreaterThan(0);
    }

    @Test
    void aMalformedMessageIsDroppedBeforeItReachesTheNode() throws Exception {
        GrpcPeerServer s = server();
        ManagedChannel ch = InProcessChannelBuilder.forName(name).build();
        StreamObserver<RaftMessage> out = RaftPeerGrpc.newStub(ch).stream(new StreamObserver<>() {
            @Override public void onNext(com.projects.raftkv.proto.Ack a) {}
            @Override public void onError(Throwable e) {}
            @Override public void onCompleted() {}
        });
        out.onNext(RaftMessage.newBuilder().setFrom(1).setTo(2).setTerm(1).build());   // no body
        out.onNext(ProtoMapping.toProto(heartbeat(7)));
        assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo(heartbeat(7));
        assertThat(s.malformed()).isEqualTo(1);
        out.onCompleted();
        ch.shutdownNow();
    }

    @Test
    void sendingToANonPeerIsAProgrammingError() {
        GrpcTransport t = transport(10);
        Message toSelf = new Message.RequestVoteResponse(N2, N1, 1, true);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> t.send(toSelf)).isInstanceOf(IllegalArgumentException.class);
    }
}
