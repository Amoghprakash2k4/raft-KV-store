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
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.statemachine.StateMachine;
import com.projects.raftkv.storage.Storage;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.transport.Transport;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

/**
 * Executes the core's effects in the order required by DESIGN.md E1:
 * persist all ops, fsync, send messages, apply committed entries, then report client outcomes.
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
    private final Map<Long, Pending> pendingByIndex = new HashMap<>();
    private boolean halted;

    private NodeDriver(RaftCore core, Storage storage, Transport transport, StateMachine stateMachine,
                       Consumer<ProposalOutcome> outcomes) {
        this.core = Objects.requireNonNull(core);
        this.storage = Objects.requireNonNull(storage);
        this.transport = Objects.requireNonNull(transport);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.outcomes = Objects.requireNonNull(outcomes);
    }

    /**
     * Builds a node from whatever is durable in {@code storage} (fresh start or crash recovery).
     * {@code stateMachine} must be empty: it is rebuilt by re-applying the committed log (M3 adds
     * snapshots).
     */
    public static NodeDriver recover(RaftConfig config, Storage storage, Transport transport,
                                     StateMachine stateMachine, Consumer<ProposalOutcome> outcomes,
                                     RandomGenerator rng) {
        RaftCore core = new RaftNodeCore(config, storage.loadAll(), rng);
        return new NodeDriver(core, storage, transport, stateMachine, outcomes);
    }

    /** Wraps an already-built core (the simulator's mutation runs, and driver tests). */
    public static NodeDriver create(RaftCore core, Storage storage, Transport transport,
                                    StateMachine stateMachine, Consumer<ProposalOutcome> outcomes) {
        return new NodeDriver(core, storage, transport, stateMachine, outcomes);
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
        // 3. Apply, in index order.
        for (LogEntry e : ready.toApply()) {
            apply(e);
        }
        // 4. Notify.
        for (ClientNotice n : ready.notices()) {
            switch (n) {
                case ClientNotice.ProposalAccepted a -> { }   // tracked above; completes at apply time
                case ClientNotice.ProposalRejected r -> outcomes.accept(new ProposalOutcome.Rejected(r.proposalId(), r.leaderHint()));
                case ClientNotice.ReadReady r -> throw new IllegalStateException("ReadIndex arrives in M4: " + r);
                case ClientNotice.ReadRejected r -> throw new IllegalStateException("ReadIndex arrives in M4: " + r);
            }
        }
    }

    public boolean isHalted() { return halted; }

    public RaftStatus status() { return core.status(); }

    public LogView log() { return core.log(); }

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
        }
    }
}
