package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

/** Shared builders for core unit tests. T = 10 ticks, heartbeat = 2 ticks. */
final class CoreFixtures {
    static final NodeId N1 = new NodeId(1);
    static final NodeId N2 = new NodeId(2);
    static final NodeId N3 = new NodeId(3);
    static final NodeId N4 = new NodeId(4);
    static final NodeId N5 = new NodeId(5);
    static final List<NodeId> THREE = List.of(N1, N2, N3);
    static final List<NodeId> FIVE = List.of(N1, N2, N3, N4, N5);
    static final int T = 10;
    static final int HEARTBEAT = 2;

    private CoreFixtures() {}

    static RaftConfig config(NodeId self, List<NodeId> members) {
        return new RaftConfig(self, members, T, HEARTBEAT, 64);
    }

    static RaftNodeCore core(NodeId self, List<NodeId> members, PersistentState state, long seed) {
        return new RaftNodeCore(config(self, members), state, new SplittableRandom(seed));
    }

    static RaftNodeCore fresh(NodeId self) {
        return core(self, THREE, PersistentState.EMPTY, 42);
    }

    static PersistentState state(long term, Optional<NodeId> votedFor, long... entryTerms) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryTerms.length; i++) {
            entries.add(new LogEntry(i + 1, entryTerms[i], new EntryPayload.Noop()));
        }
        return new PersistentState(new HardState(term, votedFor), entries);
    }

    static Ready tick(RaftCore core) {
        return core.step(new Event.Tick());
    }

    /** Ticks n times and returns all Readys concatenated into one list of messages. */
    static List<Message> tickN(RaftCore core, int n) {
        List<Message> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.addAll(tick(core).messages());
        return out;
    }

    /** Ticks until the node becomes a candidate; returns the Ready of the tick that started the election. */
    static Ready tickUntilElection(RaftCore core) {
        for (int i = 0; i < 2 * T; i++) {
            Ready r = tick(core);
            if (core.status().soft().role() == Role.CANDIDATE) return r;
        }
        throw new AssertionError("no election within 2T ticks");
    }

    static Ready receive(RaftCore core, Message m) {
        return core.step(new Event.MessageReceived(m));
    }

    static RaftNodeCore leaderOf3(NodeId self) {
        RaftNodeCore c = fresh(self);
        tickUntilElection(c);
        long term = c.status().hard().currentTerm();
        NodeId peer = self.equals(N2) ? N3 : N2;
        receive(c, new Message.RequestVoteResponse(peer, self, term, true));
        if (c.status().soft().role() != Role.LEADER) throw new AssertionError("expected leader");
        return c;
    }

    static Message.AppendEntries heartbeat(NodeId from, NodeId to, long term) {
        return new Message.AppendEntries(from, to, term, 0, 0, List.of(), 0);
    }

    static <M extends Message> List<M> ofType(List<Message> ms, Class<M> type) {
        return ms.stream().filter(type::isInstance).map(type::cast).toList();
    }
}
