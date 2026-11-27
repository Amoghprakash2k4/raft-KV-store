package com.projects.raftkv.server;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftNodeCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.node.NodeDriver;
import com.projects.raftkv.node.NodeHaltedException;
import com.projects.raftkv.node.ProposalOutcome;
import com.projects.raftkv.node.ReadOutcome;
import com.projects.raftkv.statemachine.KvStateMachine;
import com.projects.raftkv.storage.disk.RealDisk;
import com.projects.raftkv.storage.wal.FileWalStorage;
import com.projects.raftkv.storage.wal.WalOptions;
import com.projects.raftkv.transport.GrpcPeerServer;
import com.projects.raftkv.transport.GrpcTransport;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One real Raft node (DESIGN.md M5, "node runtime"): the same {@link NodeDriver} the simulator runs, now
 * fed by real threads. Everything that can happen to the node (a peer message from gRPC, a client request
 * from HTTP, a timer tick) is put into ONE bounded mailbox, and ONE event-loop thread takes events out and
 * hands them to the driver (decision E1a). With group commit (M6-A, on by default) it takes everything
 * waiting, up to {@code maxBatch}, and the driver handles it as one batch with one fsync; otherwise one
 * event at a time. So the core, the driver, the log and
 * the state machine are only ever touched by that thread: no locks, the same order of effects as in the
 * simulator (persist + fsync → send → apply → notify).
 *
 * <p>Other threads only read {@link #view()}, an immutable snapshot the loop publishes after every event.
 * A failed fsync or a broken invariant halts the node (fail-stop): pending clients get an error and
 * {@code onHalt} runs (the process exits with status 1).
 */
public final class RaftServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftServer.class);
    static final int MAILBOX_CAPACITY = 50_000;
    static final int PEER_QUEUE_CAPACITY = 10_000;

    /** What the rest of the process may see of the node. Published by the loop thread after each event. */
    public record View(RaftStatus status, long lastLogIndex, Map<NodeId, Long> matchIndexes, long snapshotsTaken,
                       boolean halted) {
        public Role role() { return status.soft().role(); }
    }

    private final ServerConfig cfg;
    /** An input that becomes an event on the loop thread (where the driver and {@code waiting} live). */
    @FunctionalInterface
    private interface Input {
        Event toEvent();
    }

    private final BlockingQueue<Input> mailbox = new ArrayBlockingQueue<>(MAILBOX_CAPACITY);
    private final AtomicBoolean tickPending = new AtomicBoolean();
    private final AtomicLong nextRequestId = new AtomicLong();
    private final AtomicLong droppedPeerMessages = new AtomicLong();
    private final AtomicLong rejectedPeerMessages = new AtomicLong();
    private final Runnable onHalt;
    private final CountDownLatch stopped = new CountDownLatch(1);

    private final RealDisk disk;
    private final FileWalStorage storage;
    private final GrpcTransport transport;
    private final NodeDriver driver;                                   // loop thread only
    private final Map<Long, CompletableFuture<ClientReply>> waiting = new HashMap<>();   // loop thread only
    private final Thread loop;
    private final ScheduledExecutorService ticker;
    private final GrpcPeerServer peerServer;
    private final NodeMetrics metrics;
    private volatile View view;
    private volatile boolean running = true;

    private RaftServer(ServerConfig cfg, Runnable onHalt) {
        this.cfg = cfg;
        this.onHalt = Objects.requireNonNull(onHalt);
        this.metrics = new NodeMetrics(cfg.self(), cfg.peers().keySet(), this::view, mailbox::size);

        this.disk = new RealDisk(cfg.dataDir());
        this.storage = FileWalStorage.open(new TimedDisk(disk, metrics::recordFsync), WalOptions.DEFAULTS);
        if (storage.tornBytesCut() > 0) log.warn("recovery cut a torn WAL tail of {} bytes", storage.tornBytesCut());
        Map<NodeId, InetSocketAddress> peers = cfg.peers();
        this.transport = GrpcTransport.connect(peers, PEER_QUEUE_CAPACITY);
        metrics.bindTransport(transport, peers.keySet());

        RaftConfig rc = new RaftConfig(cfg.self(), cfg.memberIds(), cfg.electionTicks(), cfg.heartbeatTicks(), 64,
                RaftConfig.DEFAULT_SNAPSHOT_CHUNK_BYTES, cfg.preVote(), cfg.checkQuorum());
        KvStateMachine sm = new KvStateMachine();
        PersistentState recovered = storage.loadAll();
        NodeDriver.restoreFrom(recovered, sm);
        this.driver = NodeDriver.create(new RaftNodeCore(rc, recovered, new SplittableRandom()), storage, transport, sm,
                this::onProposalOutcome, this::onReadOutcome, cfg.snapshotEvery());
        log.info("{} recovered: {} (log up to {}, snapshot at {})", cfg.self(), recovered.hardState(),
                driver.log().lastIndex(), driver.log().snapshotIndex());
        publishView();

        this.loop = new Thread(this::runLoop, "raft-loop-" + cfg.self());
        this.ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-ticker-" + cfg.self());
            t.setDaemon(true);
            return t;
        });
        this.peerServer = GrpcPeerServer.start(cfg.peerPort(), this::onPeerMessage);
    }

    /** Recovers from {@code cfg.dataDir()}, then starts the loop, the ticker and the peer server. */
    public static RaftServer start(ServerConfig cfg, Runnable onHalt) {
        RaftServer s = new RaftServer(cfg, onHalt);
        s.loop.start();
        s.ticker.scheduleAtFixedRate(s::tick, cfg.tickMs(), cfg.tickMs(), TimeUnit.MILLISECONDS);
        return s;
    }

    public ServerConfig config() { return cfg; }

    public View view() { return view; }

    public NodeMetrics metrics() { return metrics; }

    public int peerPort() { return peerServer.port(); }

    // ---------------------------------------------------------------- inputs (any thread)

    /** A write: {@code request} is a KvCodec-encoded {@code KvRequest}. Completes when applied, or rejected. */
    public CompletableFuture<ClientReply> write(Bytes request) {
        CompletableFuture<ClientReply> f = new CompletableFuture<>();
        long id = nextRequestId.incrementAndGet();
        submit(f, () -> {
            waiting.put(id, f);
            return new Event.ProposeCommand(id, request);
        });
        return f;
    }

    /** A linearizable read (ReadIndex): {@code query} is a KvCodec-encoded Get. */
    public CompletableFuture<ClientReply> read(Bytes query) {
        CompletableFuture<ClientReply> f = new CompletableFuture<>();
        long id = nextRequestId.incrementAndGet();
        submit(f, () -> {
            waiting.put(id, f);
            return driver.prepareRead(id, query);
        });
        return f;
    }

    private void submit(CompletableFuture<ClientReply> f, Input task) {
        if (!running || view.halted()) {
            f.complete(new ClientReply.Unavailable("node is not running"));
        } else if (!mailbox.offer(task)) {
            f.complete(new ClientReply.Unavailable("overloaded"));
        }
    }

    /** From gRPC threads. Only messages for this node from another member reach the core. */
    private void onPeerMessage(Message m) {
        if (!m.to().equals(cfg.self()) || m.from().equals(cfg.self()) || !cfg.members().containsKey(m.from())) {
            rejectedPeerMessages.incrementAndGet();
            return;
        }
        if (!mailbox.offer(() -> new Event.MessageReceived(m))) {
            droppedPeerMessages.incrementAndGet();   // like a lost message: Raft resends what matters
        }
    }

    /** At most one tick waits in the mailbox: if the loop falls behind, ticks are skipped, not queued up. */
    private void tick() {
        if (tickPending.compareAndSet(false, true)) {
            if (!mailbox.offer(() -> {
                tickPending.set(false);
                return new Event.Tick();
            })) {
                tickPending.set(false);
            }
        }
    }

    // ---------------------------------------------------------------- the loop thread

    private void runLoop() {
        try {
            List<Input> inputs = new ArrayList<>();
            List<Event> events = new ArrayList<>();
            while (running) {
                Input first;
                try {
                    first = mailbox.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }
                if (first == null) continue;
                inputs.clear();
                events.clear();
                inputs.add(first);
                if (cfg.fsyncBatching()) mailbox.drainTo(inputs, cfg.maxBatch() - 1);
                try {
                    for (Input in : inputs) events.add(in.toEvent());
                    if (cfg.fsyncBatching()) {
                        driver.handleBatch(events);
                    } else {
                        for (Event e : events) driver.handle(e);
                    }
                    metrics.recordBatch(events.size());
                } catch (NodeHaltedException e) {
                    halt("storage failure: " + e.getMessage(), e);
                    return;
                } catch (RuntimeException e) {
                    halt("unexpected error in the event loop", e);
                    return;
                }
                publishView();
            }
        } finally {
            failAllWaiting("node stopped");
            stopped.countDown();
        }
    }

    private void onProposalOutcome(ProposalOutcome o) {
        CompletableFuture<ClientReply> f = waiting.remove(o.proposalId());
        if (f == null) return;
        f.complete(switch (o) {
            case ProposalOutcome.Committed c -> new ClientReply.Ok(c.result());
            case ProposalOutcome.Rejected r -> new ClientReply.NotLeader(r.leaderHint());
            case ProposalOutcome.Lost l -> new ClientReply.Retry("entry " + l.index() + " was overwritten by a new leader");
            case ProposalOutcome.Unknown u -> new ClientReply.Retry("entry " + u.index() + " was covered by a snapshot");
        });
    }

    private void onReadOutcome(ReadOutcome o) {
        CompletableFuture<ClientReply> f = waiting.remove(o.readId());
        if (f == null) return;
        f.complete(switch (o) {
            case ReadOutcome.Ok ok -> new ClientReply.Ok(ok.result());
            case ReadOutcome.Rejected r -> new ClientReply.NotLeader(r.leaderHint());
        });
    }

    private void publishView() {
        View next = new View(driver.status(), driver.log().lastIndex(), driver.matchIndexes(), driver.snapshotsTaken(),
                driver.isHalted());
        metrics.observe(view, next);
        view = next;
    }

    private void halt(String why, Throwable cause) {
        log.error("HALT ({}): fail-stop, this node stops participating", why, cause);
        running = false;
        view = new View(view.status(), view.lastLogIndex(), view.matchIndexes(), view.snapshotsTaken(), true);
        failAllWaiting("node halted");
        onHalt.run();
    }

    private void failAllWaiting(String why) {
        for (CompletableFuture<ClientReply> f : waiting.values()) f.complete(new ClientReply.Unavailable(why));
        waiting.clear();
    }

    public long droppedPeerMessages() { return droppedPeerMessages.get(); }

    public long rejectedPeerMessages() { return rejectedPeerMessages.get(); }

    /** Graceful stop (SIGTERM, tests). A kill -9 skips all of this, which is exactly what recovery is for. */
    @Override
    public void close() {
        running = false;
        ticker.shutdownNow();
        peerServer.close();
        try {
            stopped.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transport.close();
        disk.close();   // closes file handles only; everything acknowledged was already fsynced
    }
}
