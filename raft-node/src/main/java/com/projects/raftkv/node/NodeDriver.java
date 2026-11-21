package com.projects.raftkv.node;

import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.PersistOp;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftNodeCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Ready;
import com.projects.raftkv.storage.Storage;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.transport.Transport;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Executes the core's effects in the order required by DESIGN.md E1:
 * persist all ops, fsync, then send messages (M2: then apply; M4: then notify clients).
 *
 * <p>Synchronous and not thread-safe: exactly one thread calls {@link #handle}. The simulator calls it
 * directly; the real node (M5) wraps it in a single-threaded event loop with an inbound queue.
 */
public final class NodeDriver {

    private final RaftCore core;
    private final Storage storage;
    private final Transport transport;
    private boolean halted;

    private NodeDriver(RaftCore core, Storage storage, Transport transport) {
        this.core = core;
        this.storage = storage;
        this.transport = transport;
    }

    /** Builds a node from whatever is durable in {@code storage} (fresh start or crash recovery). */
    public static NodeDriver recover(RaftConfig config, Storage storage, Transport transport, RandomGenerator rng) {
        Objects.requireNonNull(storage);
        Objects.requireNonNull(transport);
        RaftCore core = new RaftNodeCore(config, storage.loadAll(), rng);
        return new NodeDriver(core, storage, transport);
    }

    public void handle(Event event) {
        if (halted) throw new NodeHaltedException("node halted after a storage failure", null);
        Ready ready = core.step(event);

        if (ready.mustSync()) {
            try {
                for (PersistOp op : ready.persist()) persist(op);
                storage.sync();
            } catch (StorageException e) {
                halted = true;
                throw new NodeHaltedException("fsync failed; halting before sending anything", e);
            }
        }
        for (Message m : ready.messages()) {
            transport.send(m);
        }
        if (!ready.toApply().isEmpty() || !ready.notices().isEmpty()) {
            throw new IllegalStateException("apply arrives in M2 and client notices in M4: " + ready);
        }
    }

    public boolean isHalted() { return halted; }

    public RaftStatus status() { return core.status(); }

    public LogView log() { return core.log(); }

    private void persist(PersistOp op) {
        switch (op) {
            case PersistOp.SaveHardState s -> storage.saveHardState(s.state());
            case PersistOp.TruncateSuffix t -> storage.truncateFrom(t.fromIndex());
            case PersistOp.AppendEntries a -> storage.append(a.entries());
        }
    }
}
