package com.projects.raftkv.transport;

import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.proto.Ack;
import com.projects.raftkv.proto.RaftMessage;
import com.projects.raftkv.proto.RaftPeerGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outbound peer messages over gRPC (DESIGN.md M5-B): per peer, one channel, one long-lived client stream
 * ({@code RaftPeer.Stream}), one bounded queue and one sender thread. {@link #send} never blocks the
 * caller (the node's event loop): when a peer's queue is full the message is dropped. Raft is built for
 * lost messages, and heartbeats resend whatever is missing (the simulator drops 5% of messages in every run).
 *
 * <p>The sender thread respects gRPC flow control: it waits for the stream to be ready, and if the peer
 * does not read for {@link #READY_TIMEOUT_MS} the stream is cancelled and reopened. A broken stream (peer
 * restarted or down) is reopened on the next message, at most once per {@link #RECONNECT_BACKOFF_MS};
 * messages in between are dropped.
 */
public final class GrpcTransport implements Transport, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GrpcTransport.class);

    static final long READY_TIMEOUT_MS = 200;
    static final long RECONNECT_BACKOFF_MS = 100;
    /** Snapshot chunks are 64 KiB and AppendEntries batches at most 64 entries; this leaves plenty of room. */
    public static final int MAX_MESSAGE_BYTES = 16 * 1024 * 1024;

    private final Map<NodeId, PeerLink> links = new LinkedHashMap<>();

    /** Plaintext gRPC over TCP to each peer's address (TLS is out of scope for v1). */
    public static GrpcTransport connect(Map<NodeId, InetSocketAddress> peers, int queueCapacity) {
        return new GrpcTransport(peers.keySet(), id -> {
            InetSocketAddress a = peers.get(id);
            return Grpc.newChannelBuilderForAddress(a.getHostString(), a.getPort(), InsecureChannelCredentials.create())
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .build();
        }, queueCapacity);
    }

    /** With channels from the caller (tests use in-process channels). */
    public GrpcTransport(Iterable<NodeId> peers, Function<NodeId, ManagedChannel> channels, int queueCapacity) {
        if (queueCapacity < 1) throw new IllegalArgumentException("queueCapacity < 1");
        for (NodeId p : peers) links.put(p, new PeerLink(p, channels.apply(p), queueCapacity));
        for (PeerLink l : links.values()) l.start();
    }

    @Override
    public void send(Message message) {
        PeerLink l = links.get(message.to());
        if (l == null) throw new IllegalArgumentException("not a peer: " + message.to());
        l.enqueue(message);
    }

    public long sent(NodeId peer) { return link(peer).sent.get(); }

    public long dropped(NodeId peer) { return link(peer).dropped.get(); }

    public int queued(NodeId peer) { return link(peer).queue.size(); }

    private PeerLink link(NodeId peer) {
        return Objects.requireNonNull(links.get(peer), () -> "not a peer: " + peer);
    }

    @Override
    public void close() {
        for (PeerLink l : links.values()) l.close();
    }

    /** One peer: a bounded queue drained by its own thread into one gRPC stream. */
    private static final class PeerLink {
        private final NodeId peer;
        private final ManagedChannel channel;
        private final RaftPeerGrpc.RaftPeerStub stub;
        final BlockingQueue<Message> queue;
        final AtomicLong sent = new AtomicLong();
        final AtomicLong dropped = new AtomicLong();
        private final Object readyLock = new Object();
        private final Thread thread;
        private volatile boolean closed;
        /** The open stream, or null. Written by the sender thread and by gRPC callbacks (on failure). */
        private volatile ClientCallStreamObserver<RaftMessage> stream;
        private volatile long reconnectNotBeforeNanos;

        PeerLink(NodeId peer, ManagedChannel channel, int capacity) {
            this.peer = peer;
            this.channel = channel;
            this.stub = RaftPeerGrpc.newStub(channel);
            this.queue = new ArrayBlockingQueue<>(capacity);
            this.thread = new Thread(this::run, "raft-send-" + peer);
            this.thread.setDaemon(true);
        }

        void start() { thread.start(); }

        void enqueue(Message m) {
            if (!queue.offer(m)) dropped.incrementAndGet();
        }

        private void run() {
            while (!closed) {
                Message m;
                try {
                    m = queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    deliver(ProtoMapping.toProto(m));
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    dropped.incrementAndGet();
                    resetStream("send failed: " + e);
                }
            }
        }

        private void deliver(RaftMessage m) throws InterruptedException {
            ClientCallStreamObserver<RaftMessage> s = stream;
            if (s == null) {
                if (System.nanoTime() < reconnectNotBeforeNanos) {
                    dropped.incrementAndGet();
                    return;
                }
                s = open();
            }
            if (!awaitReady(s)) {
                dropped.incrementAndGet();
                if (stream == s) {
                    s.cancel("peer not reading", null);
                    resetStream("not ready within " + READY_TIMEOUT_MS + " ms");
                }
                return;
            }
            s.onNext(m);
            sent.incrementAndGet();
        }

        private ClientCallStreamObserver<RaftMessage> open() {
            AtomicReference<ClientCallStreamObserver<RaftMessage>> opened = new AtomicReference<>();
            stub.stream(new ClientResponseObserver<RaftMessage, Ack>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<RaftMessage> requestStream) {
                    opened.set(requestStream);
                    requestStream.setOnReadyHandler(() -> {
                        synchronized (readyLock) { readyLock.notifyAll(); }
                    });
                }

                @Override public void onNext(Ack ack) {}

                @Override
                public void onError(Throwable t) {
                    if (stream == opened.get()) resetStream("stream to " + peer + " failed: " + t.getMessage());
                    synchronized (readyLock) { readyLock.notifyAll(); }
                }

                @Override
                public void onCompleted() {
                    if (stream == opened.get()) resetStream("stream to " + peer + " closed by the peer");
                }
            });
            stream = opened.get();
            return opened.get();
        }

        /** Waits until gRPC can take another message on this stream; false on timeout or if it broke. */
        private boolean awaitReady(ClientCallStreamObserver<RaftMessage> s) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(READY_TIMEOUT_MS);
            synchronized (readyLock) {
                while (!s.isReady()) {
                    if (stream != s || closed) return false;
                    long left = deadline - System.nanoTime();
                    if (left <= 0) return false;
                    TimeUnit.NANOSECONDS.timedWait(readyLock, left);
                }
            }
            return stream == s;
        }

        private void resetStream(String why) {
            stream = null;
            reconnectNotBeforeNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RECONNECT_BACKOFF_MS);
            if (!closed) log.debug("link to {}: {}", peer, why);
        }

        void close() {
            closed = true;
            thread.interrupt();
            ClientCallStreamObserver<RaftMessage> s = stream;
            if (s != null) {
                try {
                    s.onCompleted();
                } catch (RuntimeException ignored) {
                    // already broken: nothing to finish
                }
            }
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
