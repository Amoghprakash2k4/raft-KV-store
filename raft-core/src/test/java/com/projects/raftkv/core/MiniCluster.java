package com.projects.raftkv.core;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A synchronous, lossless message pump between real cores, for hand-built paper scenarios.
 * Messages to or from an isolated node are dropped. No clock: callers tick explicitly.
 */
final class MiniCluster {
    private final Map<NodeId, RaftNodeCore> nodes = new LinkedHashMap<>();
    private final Set<NodeId> isolated = new HashSet<>();
    private final Deque<Message> inFlight = new ArrayDeque<>();
    private int appendEntriesDelivered;

    MiniCluster add(RaftNodeCore core) {
        nodes.put(core.status().id(), core);
        return this;
    }

    MiniCluster isolate(NodeId id) {
        isolated.add(id);
        return this;
    }

    RaftNodeCore node(NodeId id) { return nodes.get(id); }

    /** Number of AppendEntries messages delivered so far: a cost measure for log backup. */
    int appendEntriesDelivered() { return appendEntriesDelivered; }

    void enqueue(Ready r) { inFlight.addAll(r.messages()); }

    void step(NodeId id, Event e) { enqueue(nodes.get(id).step(e)); }

    /** Delivers messages until none are left. Fails if that takes absurdly long (a livelock). */
    void deliverAll() {
        int budget = 100_000;
        while (!inFlight.isEmpty()) {
            if (--budget == 0) throw new AssertionError("message storm: messages never stopped");
            Message m = inFlight.removeFirst();
            if (isolated.contains(m.from()) || isolated.contains(m.to()) || !nodes.containsKey(m.to())) continue;
            if (m instanceof Message.AppendEntries) appendEntriesDelivered++;
            enqueue(nodes.get(m.to()).step(new Event.MessageReceived(m)));
        }
    }

    /** Ticks every node once (in insertion order) and delivers everything that results. */
    void tickAll() {
        for (NodeId id : nodes.keySet()) {
            if (!isolated.contains(id)) step(id, new Event.Tick());
        }
        deliverAll();
    }
}
