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

    /** Entries with the given terms starting at startIndex. Payload: a command naming its (index, term). */
    static List<LogEntry> entries(long startIndex, long... terms) {
        List<LogEntry> out = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            long index = startIndex + i;
            out.add(new LogEntry(index, terms[i], cmd(index + "@" + terms[i])));
        }
        return out;
    }

    static EntryPayload.Command cmd(String s) {
        return new EntryPayload.Command(Bytes.copyOf(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** A recovered state whose log entries carry commands (unlike {@link #state}, which uses no-ops). */
    static PersistentState stateWithLog(long term, long... entryTerms) {
        return new PersistentState(new HardState(term, Optional.empty()), entries(1, entryTerms));
    }

    static Message.AppendEntries append(NodeId from, NodeId to, long term, long prevIndex, long prevTerm,
                                        List<LogEntry> entries, long leaderCommit) {
        return new Message.AppendEntries(from, to, term, prevIndex, prevTerm, entries, leaderCommit);
    }

    static Message.AppendEntriesResponse ok(NodeId from, NodeId to, long term, long matchIndex) {
        return new Message.AppendEntriesResponse(from, to, term, true, matchIndex, 0, 0, 0);
    }

    /** A rejection without a useful hint: the leader just backs up by one (conflictIndex = rejectedIndex). */
    static Message.AppendEntriesResponse reject(NodeId from, NodeId to, long term, long rejectedIndex) {
        return new Message.AppendEntriesResponse(from, to, term, false, 0, rejectedIndex, 0, rejectedIndex);
    }

    /** A rejection carrying the fast-backup hint (conflictTerm, conflictIndex). */
    static Message.AppendEntriesResponse reject(NodeId from, NodeId to, long term, long rejectedIndex,
                                                long conflictTerm, long conflictIndex) {
        return new Message.AppendEntriesResponse(from, to, term, false, 0, rejectedIndex, conflictTerm, conflictIndex);
    }

    /** The terms of every entry in the log, in order: compact for assertions. */
    static List<Long> terms(LogView log) {
        List<Long> out = new ArrayList<>();
        for (long i = 1; i <= log.lastIndex(); i++) out.add(log.termAt(i).getAsLong());
        return out;
    }

    static List<Long> termList(long... terms) {
        return java.util.Arrays.stream(terms).boxed().toList();
    }

    /**
     * Makes {@code self} leader of exactly {@code term}: recovers with currentTerm = term - 1 and the
     * given log, times out, and receives votes from the given voters. Election messages are discarded.
     */
    static RaftNodeCore leaderAt(NodeId self, List<NodeId> members, long term, PersistentState log,
                                 List<NodeId> voters) {
        PersistentState s = new PersistentState(new HardState(term - 1, Optional.empty()), log.entries());
        RaftNodeCore c = core(self, members, s, 42);
        tickUntilElection(c);
        for (NodeId v : voters) receive(c, new Message.RequestVoteResponse(v, self, term, true));
        if (c.status().soft().role() != Role.LEADER || c.status().hard().currentTerm() != term)
            throw new AssertionError("expected leader of term " + term + ", got " + c.status());
        return c;
    }

    static List<Message.AppendEntries> appendsTo(Ready r, NodeId to) {
        return ofType(r.messages(), Message.AppendEntries.class).stream().filter(m -> m.to().equals(to)).toList();
    }

    static <M extends Message> List<M> ofType(List<Message> ms, Class<M> type) {
        return ms.stream().filter(type::isInstance).map(type::cast).toList();
    }
}
