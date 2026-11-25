package com.projects.raftkv.node;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.ClientNotice;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.PersistOp;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftNodeCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.core.Snapshot;
import com.projects.raftkv.statemachine.StateMachine;
import com.projects.raftkv.storage.Storage;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.transport.Transport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

/**
 * Executes the core's effects in the order required by DESIGN.md E1:
 * persist all ops, fsync, send messages, apply committed entries (or restore an installed snapshot),
 * then report client outcomes, serving ReadIndex reads whose read index has been applied (M4). Between two events it may take a snapshot (DESIGN.md M3-E).
 *
 * <p>Synchronous and not thread-safe: exactly one thread calls {@link #handle}. The simulator calls it
 * directly; the real node (M5) wraps it in a single-threaded event loop with an inbound queue.
 */
public final class NodeDriver {

    /** A proposal this node accepted as leader, waiting for its index to be applied (DESIGN.md M2-E). */
    private record Pending(long proposalId, long term) {}

    private final RaftCore core;
    private final Storage storage;
    private final Transport transport;
    private final StateMachine stateMachine;
    private final Consumer<ProposalOutcome> outcomes;
    private final Consumer<ReadOutcome> readOutcomes;
    /** Production default for M3-E: snapshot once this many entries were applied since the last one. */
    public static final long DEFAULT_SNAPSHOT_EVERY = 10_000;

    private final Map<Long, Pending> pendingByIndex = new HashMap<>();
    /** Reads handed to the core, by readId: the query to run once released. */
    private final Map<Long, Bytes> readQueries = new HashMap<>();
    /** Released reads waiting for lastApplied >= readIndex (thesis §6.4 step 4), by readIndex. */
    private final TreeMap<Long, List<Long>> readsWaitingForApply = new TreeMap<>();
    /** Index of the last entry applied here, no-ops included (the state machine skips those). */
    private long appliedIndex;
    private final long snapshotEvery;
    private boolean halted;
    private long snapshotsTaken;
    private long snapshotsInstalled;

    private NodeDriver(RaftCore core, Storage storage, Transport transport, StateMachine stateMachine,
                       Consumer<ProposalOutcome> outcomes, Consumer<ReadOutcome> readOutcomes, long snapshotEvery) {
        this.core = Objects.requireNonNull(core);
        this.storage = Objects.requireNonNull(storage);
        this.transport = Objects.requireNonNull(transport);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.outcomes = Objects.requireNonNull(outcomes);
        this.readOutcomes = Objects.requireNonNull(readOutcomes);
        this.appliedIndex = stateMachine.lastAppliedIndex();   // the restored snapshot's index, or 0
        if (snapshotEvery < 0) throw new IllegalArgumentException("snapshotEvery < 0");
        this.snapshotEvery = snapshotEvery;
    }

    /**
     * Builds a node from whatever is durable in {@code storage} (fresh start or crash recovery).
     * {@code stateMachine} must be empty: it is restored from the snapshot, if any, and the committed
     * entries after it are re-applied as the node learns its commit index again.
     */
    public static NodeDriver recover(RaftConfig config, Storage storage, Transport transport,
                                     StateMachine stateMachine, Consumer<ProposalOutcome> outcomes,
                                     RandomGenerator rng, long snapshotEvery) {
        PersistentState state = storage.loadAll();
        restoreFrom(state, stateMachine);
        RaftCore core = new RaftNodeCore(config, state, rng);
        return new NodeDriver(core, storage, transport, stateMachine, outcomes, NO_READS, snapshotEvery);
    }

    public static NodeDriver recover(RaftConfig config, Storage storage, Transport transport,
                                     StateMachine stateMachine, Consumer<ProposalOutcome> outcomes,
                                     RandomGenerator rng) {
        return recover(config, storage, transport, stateMachine, outcomes, rng, DEFAULT_SNAPSHOT_EVERY);
    }

    /** Puts the recovered snapshot (if any) into an empty state machine, before the core is built from the same state. */
    public static void restoreFrom(PersistentState state, StateMachine stateMachine) {
        if (stateMachine.lastAppliedIndex() != 0) throw new IllegalArgumentException("state machine is not empty");
        state.snapshot().ifPresent(s -> stateMachine.restore(s.index(), s.data()));
    }

    /**
     * Wraps an already-built core (the simulator, and driver tests). The caller has restored
     * {@code stateMachine} from the same recovered state ({@link #restoreFrom}). {@code snapshotEvery}
     * 0 never snapshots.
     */
    public static NodeDriver create(RaftCore core, Storage storage, Transport transport,
                                    StateMachine stateMachine, Consumer<ProposalOutcome> outcomes,
                                    long snapshotEvery) {
        return new NodeDriver(core, storage, transport, stateMachine, outcomes, NO_READS, snapshotEvery);
    }

    /** As above, with ReadIndex reads ({@link #read}) reported to {@code readOutcomes}. */
    public static NodeDriver create(RaftCore core, Storage storage, Transport transport,
                                    StateMachine stateMachine, Consumer<ProposalOutcome> outcomes,
                                    Consumer<ReadOutcome> readOutcomes, long snapshotEvery) {
        return new NodeDriver(core, storage, transport, stateMachine, outcomes, readOutcomes, snapshotEvery);
    }

    private static final Consumer<ReadOutcome> NO_READS = r -> {
        throw new IllegalStateException("this driver was built without a read listener: " + r);
    };

    /**
     * Starts a linearizable read (thesis §6.4): the core confirms leadership and picks a read index; this
     * driver then runs {@code query} against the state machine once it has applied that far. The answer
     * goes to the read listener as {@link ReadOutcome}. {@code readId} must not be in flight already.
     */
    public void read(long readId, Bytes query) {
        Objects.requireNonNull(query);
        if (halted) throw new NodeHaltedException("node halted after a storage failure", null);
        if (readQueries.putIfAbsent(readId, query) != null)
            throw new IllegalArgumentException("read " + readId + " is already in flight");
        handle(new Event.ReadRequested(readId));
    }

    /** As above, without snapshots. */
    public static NodeDriver create(RaftCore core, Storage storage, Transport transport,
                                    StateMachine stateMachine, Consumer<ProposalOutcome> outcomes) {
        return create(core, storage, transport, stateMachine, outcomes, 0);
    }

    public void handle(Event event) {
        if (halted) throw new NodeHaltedException("node halted after a storage failure", null);
        Ready ready = core.step(event);

        // 1. Persist + one fsync. Nothing below happens if this fails (E1a rule 2).
        if (ready.mustSync()) {
            try {
                for (PersistOp op : ready.persist()) persist(op);
                storage.sync();
            } catch (StorageException e) {
                halted = true;
                throw new NodeHaltedException("fsync failed; halting before sending anything", e);
            }
        }
        // 2. Send.
        for (Message m : ready.messages()) {
            transport.send(m);
        }
        // Bookkeeping for proposals accepted in this step; must precede apply so the match is seen.
        for (ClientNotice n : ready.notices()) {
            if (n instanceof ClientNotice.ProposalAccepted a) track(a);
        }
        // 3. Apply: an installed snapshot first (it replaces the state), then entries in index order.
        ready.snapshotToRestore().ifPresent(this::restore);
        for (LogEntry e : ready.toApply()) {
            apply(e);
        }
        // 4. Notify.
        for (ClientNotice n : ready.notices()) {
            switch (n) {
                case ClientNotice.ProposalAccepted a -> { }   // tracked above; completes at apply time
                case ClientNotice.ProposalRejected r -> outcomes.accept(new ProposalOutcome.Rejected(r.proposalId(), r.leaderHint()));
                case ClientNotice.ReadReady r -> readsWaitingForApply.computeIfAbsent(r.readIndex(), k -> new ArrayList<>()).add(r.readId());
                case ClientNotice.ReadRejected r -> {
                    if (readQueries.remove(r.readId()) == null) throw new IllegalStateException("unknown read " + r);
                    readOutcomes.accept(new ReadOutcome.Rejected(r.readId(), r.leaderHint()));
                }
            }
        }
        serveReads();
        // Between two events: maybe take a snapshot.
        maybeSnapshot();
    }

    /** Thesis §6.4 step 4: answer every released read whose readIndex has been applied. */
    private void serveReads() {
        while (!readsWaitingForApply.isEmpty() && readsWaitingForApply.firstKey() <= appliedIndex) {
            Map.Entry<Long, List<Long>> e = readsWaitingForApply.pollFirstEntry();
            for (long readId : e.getValue()) {
                Bytes query = readQueries.remove(readId);
                if (query == null) throw new IllegalStateException("unknown read " + readId);
                readOutcomes.accept(new ReadOutcome.Ok(readId, e.getKey(), stateMachine.query(query)));
            }
        }
    }

    /**
     * DESIGN.md M3-E: once snapshotEvery entries were applied since the last snapshot, serialize the state
     * machine, which is exactly at lastApplied now (E1a rule 5), save it durably, and only then tell the
     * core, whose compaction record must never reach the disk before the snapshot it depends on.
     */
    private void maybeSnapshot() {
        if (snapshotEvery == 0) return;
        long applied = core.status().lastApplied();
        if (applied - core.log().snapshotIndex() < snapshotEvery) return;
        long term = core.log().termAt(applied).orElseThrow();
        Snapshot s = new Snapshot(applied, term, stateMachine.snapshot());
        try {
            storage.saveSnapshot(s);
        } catch (StorageException e) {
            halted = true;
            throw new NodeHaltedException("saving a snapshot failed; halting", e);
        }
        snapshotsTaken++;
        handle(new Event.SnapshotCreated(s.index(), s.term(), s.data()));
    }

    /**
     * Figure 13 step 8: reset the state machine from the leader's snapshot. Proposals waiting on an index
     * the snapshot covers can no longer be matched to an applied entry, so their outcome is unknown.
     */
    private void restore(Snapshot s) {
        stateMachine.restore(s.index(), s.data());
        appliedIndex = Math.max(appliedIndex, s.index());
        snapshotsInstalled++;
        for (Iterator<Map.Entry<Long, Pending>> it = pendingByIndex.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Pending> p = it.next();
            if (p.getKey() <= s.index()) {
                it.remove();
                outcomes.accept(new ProposalOutcome.Unknown(p.getValue().proposalId(), p.getKey()));
            }
        }
    }

    public boolean isHalted() { return halted; }

    public long snapshotsTaken() { return snapshotsTaken; }

    public long snapshotsInstalled() { return snapshotsInstalled; }

    public RaftStatus status() { return core.status(); }

    /** Leader only: each peer's matchIndex (metrics). */
    public java.util.Map<com.projects.raftkv.core.NodeId, Long> matchIndexes() { return core.matchIndexes(); }

    public LogView log() { return core.log(); }

    /** Reads started here and not yet answered (for tests and metrics). */
    public int pendingReads() { return readQueries.size(); }

    /** Proposals accepted here and not yet resolved (for tests and metrics). */
    public int pendingProposals() { return pendingByIndex.size(); }

    /**
     * A leader accepting a proposal at an index that already has a pending one means the older entry
     * was truncated away (this node lost leadership and later regained it), so the older one is lost.
     */
    private void track(ClientNotice.ProposalAccepted a) {
        Pending previous = pendingByIndex.put(a.index(), new Pending(a.proposalId(), a.term()));
        if (previous != null) outcomes.accept(new ProposalOutcome.Lost(previous.proposalId(), a.index()));
    }

    /**
     * Applies one committed entry. If a proposal is waiting on this index, the entry's term decides:
     * the same term means it is our entry (Log Matching: same index and term = same entry), any other
     * term means a different leader's entry was committed there instead (thesis §6.3).
     */
    private void apply(LogEntry e) {
        appliedIndex = e.index();
        Bytes result = switch (e.payload()) {
            case EntryPayload.Noop n -> null;
            case EntryPayload.Command c -> stateMachine.apply(e.index(), c.data());
        };
        Pending p = pendingByIndex.remove(e.index());
        if (p == null) return;
        if (p.term() == e.term() && result != null) {
            outcomes.accept(new ProposalOutcome.Committed(p.proposalId(), e.index(), result));
        } else {
            outcomes.accept(new ProposalOutcome.Lost(p.proposalId(), e.index()));
        }
    }

    private void persist(PersistOp op) {
        switch (op) {
            case PersistOp.SaveHardState s -> storage.saveHardState(s.state());
            case PersistOp.TruncateSuffix t -> storage.truncateFrom(t.fromIndex());
            case PersistOp.AppendEntries a -> storage.append(a.entries());
            case PersistOp.CompactLog c -> storage.compactLog(c.index(), c.term());
            case PersistOp.InstallSnapshot is -> {
                storage.saveSnapshot(is.snapshot());              // durable first ...
                storage.compactLog(is.snapshot().index(), is.snapshot().term());   // ... then the log moves onto it
            }
        }
    }
}
