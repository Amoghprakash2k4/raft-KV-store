package com.projects.raftkv.transport;

import com.projects.raftkv.core.Message;
import com.projects.raftkv.proto.Ack;
import com.projects.raftkv.proto.RaftMessage;
import com.projects.raftkv.proto.RaftPeerGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inbound peer messages over gRPC (DESIGN.md M5-B). Each decoded message goes to {@code inbound}, which
 * must be thread-safe and must not block (the node puts it in its mailbox). A message that does not decode
 * ({@link ProtoMapping#fromProto} rejects it) is counted and dropped: it never reaches the core.
 */
public final class GrpcPeerServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GrpcPeerServer.class);

    private final Server server;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();

    private GrpcPeerServer(ServerBuilder<?> builder, Consumer<Message> inbound) {
        Objects.requireNonNull(inbound);
        this.server = builder.addService(new RaftPeerGrpc.RaftPeerImplBase() {
            @Override
            public StreamObserver<RaftMessage> stream(StreamObserver<Ack> reply) {
                return new StreamObserver<>() {
                    @Override
                    public void onNext(RaftMessage m) {
                        Message msg;
                        try {
                            msg = ProtoMapping.fromProto(m);
                        } catch (IllegalArgumentException e) {
                            malformed.incrementAndGet();
                            log.warn("dropped a malformed peer message: {}", e.getMessage());
                            return;
                        }
                        received.incrementAndGet();
                        inbound.accept(msg);
                    }

                    @Override public void onError(Throwable t) {}   // the sender reconnects

                    @Override
                    public void onCompleted() {
                        reply.onNext(Ack.getDefaultInstance());
                        reply.onCompleted();
                    }
                };
            }
        }).build();
    }

    /** Plaintext gRPC on {@code port} (0 = any free port; see {@link #port()}). */
    public static GrpcPeerServer start(int port, Consumer<Message> inbound) {
        return start(Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .maxInboundMessageSize(GrpcTransport.MAX_MESSAGE_BYTES), inbound);
    }

    /** With a server builder from the caller (tests use an in-process server). */
    public static GrpcPeerServer start(ServerBuilder<?> builder, Consumer<Message> inbound) {
        GrpcPeerServer s = new GrpcPeerServer(builder, inbound);
        try {
            s.server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot start the peer server", e);
        }
        return s;
    }

    public int port() { return server.getPort(); }

    public long received() { return received.get(); }

    public long malformed() { return malformed.get(); }

    @Override
    public void close() {
        server.shutdownNow();
        try {
            server.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
