package com.projects.raftkv.sim;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.LogView;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Snapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Invariant 6, Durability: after a crash and restart with only fsynced data, no acknowledged state is
 * lost. The checker keeps a <b>shadow model</b> of what each node has made durable, fed by every
 * storage write, every saved snapshot and every successful sync. It is written independently of the
 * WAL code under test, so a bug in one is not silently repeated by the other.
 * <ul>
 *   <li><b>(a) Node level, at every restart:</b> the state the WAL recovers must equal the shadow model
 *       with some prefix of the writes that were still unsynced when the node died. (A crash during
 *       fsync can persist the first records of a batch, never a later one without an earlier one.)</li>
 *   <li><b>(b) Cluster level, when an index first becomes committed:</b> a majority of nodes must hold
 *       that entry in their durable state, in the log or inside a snapshot. A leader that counts an
 *       acknowledgement sent before the follower's fsync, or a lying disk, breaks this.</li>
 *   <li><b>(c) Node level, at every sync:</b> a node whose term has reached the term in which an entry
 *       was committed never durably deletes that entry, except by covering it with a snapshot.
 *       Such nodes only follow leaders that hold the entry (§5.4, Leader Completeness), so for them a
 *       deletion is always a bug, and it can shrink the majority behind a commit. A node still in an
 *       older term may legitimately lose it: a stale leader can overwrite an entry that a later leader
 *       committed through a majority this node was not part of (Figure 8).</li>
 * </ul>
 */
public final class DurabilityChecker {

    /** Hard state, the latest snapshot, and the log after a base: like storage, but written separately. */
    private static final class Model {
        HardState hardState = HardState.INITIAL;
        Optional<Snapshot> snapshot = Optional.empty();
        long baseIndex;
        long baseTerm;
        final List<LogEntry> log = new ArrayList<>();
        final List<Consumer<Model>> pending = new ArrayList<>();
        // Set by the checker for (c) while applying synced writes; null otherwise.
        java.util.function.LongPredicate committedHere;
        String violation;

        Model copy() {
            Model m = new Model();
            m.hardState = hardState;
            m.snapshot = snapshot;
            m.baseIndex = baseIndex;
            m.baseTerm = baseTerm;
            m.log.addAll(log);
            return m;
        }

        long lastIndex() { return baseIndex + log.size(); }

        long termAt(long index) {
            if (index == baseIndex) return baseTerm;
            if (index < baseIndex || index > lastIndex()) return -1;
            return log.get((int) (index - baseIndex - 1)).term();
        }

        void truncateFrom(long from) {
            if (committedHere != null && violation == null && from > baseIndex && from <= lastIndex() && committedHere.test(from))
                violation = "truncated committed entry " + from + "";
            log.subList((int) Math.max(0, Math.min(from - baseIndex - 1, log.size())), log.size()).clear();
        }

        /** Figure 13 / D1a: keep the suffix after (index, term) if held, else drop the whole log. */
        void rebase(long index, long term) {
            if (committedHere != null && violation == null) {
                if (index < baseIndex) violation = "moved its snapshot base back from " + baseIndex + " to " + index;
                else if (termAt(index) != term && index + 1 <= lastIndex() && committedHere.test(index + 1))
                    violation = "discarded its log, including committed entry " + (index + 1) + ", for a snapshot through " + index;
            }
            if (index < baseIndex) return;
            if (termAt(index) == term) log.subList(0, (int) (index - baseIndex)).clear();
            else log.clear();
            baseIndex = index;
            baseTerm = term;
        }

        /** What recovery must return: a snapshot newer than the base wins. */
        boolean recoversAs(PersistentState p) {
            Model m = copy();
            snapshot.filter(s -> s.index() > m.baseIndex).ifPresent(s -> m.rebase(s.index(), s.term()));
            return m.hardState.equals(p.hardState()) && m.snapshot.equals(p.snapshot()) && m.log.equals(p.entries());
        }

        boolean holds(long index, long term) {
            if (index <= baseIndex || index <= snapshot.map(Snapshot::index).orElse(0L)) return true;   // committed, in a snapshot
            return termAt(index) == term;
        }
    }

    private final List<NodeId> members;
    private final boolean enabled;
    private final Map<NodeId, Model> models = new HashMap<>();
    private final Map<Long, Long> committedTerm = new HashMap<>();
    /** Term of the node that first reported each index committed: at least the term that committed it. */
    private final Map<Long, Long> committedInTerm = new HashMap<>();
    private long commitCheckedThrough;
    private long restartsChecked;

    /** @param enabled false keeps the shadow models but never reports (mutation runs without durability checkers) */
    public DurabilityChecker(List<NodeId> members, boolean enabled) {
        this.members = List.copyOf(members);
        this.enabled = enabled;
        for (NodeId n : members) models.put(n, new Model());
    }

    // ---- fed by the node's storage ----

    public void onSaveHardState(NodeId n, HardState hs) { models.get(n).pending.add(m -> m.hardState = hs); }

    public void onAppend(NodeId n, List<LogEntry> entries) {
        List<LogEntry> copy = List.copyOf(entries);
        models.get(n).pending.add(m -> m.log.addAll(copy));
    }

    public void onTruncate(NodeId n, long fromIndex) { models.get(n).pending.add(m -> m.truncateFrom(fromIndex)); }

    public void onCompactLog(NodeId n, long index, long term) { models.get(n).pending.add(m -> m.rebase(index, term)); }

    /** Storage.saveSnapshot returned: the snapshot is durable now, not at the next sync. */
    public void onSnapshotSaved(NodeId n, Snapshot s) { models.get(n).snapshot = Optional.of(s); }

    /** The fsync returned successfully: everything written so far is durable. Checks (c) on the way. */
    public void onSync(NodeId n) {
        Model m = models.get(n);
        m.committedHere = i -> m.termAt(i) == committedTerm.getOrDefault(i, -2L)
                && m.hardState.currentTerm() >= committedInTerm.getOrDefault(i, Long.MAX_VALUE);
        for (Consumer<Model> op : m.pending) op.accept(m);
        m.committedHere = null;
        m.pending.clear();
        if (m.violation != null) {
            String v = m.violation;
            m.violation = null;
            if (!enabled) return;
            throw new InvariantViolation("Durability violated: " + n + " " + v
                    + "; a committed entry must stay on the majority that committed it");
        }
    }

    // ---- checks ----

    /** (a) Called with what the WAL recovered when node {@code n} restarts. */
    public void onRecover(long now, NodeId n, PersistentState recovered) {
        Model m = models.get(n);
        restartsChecked++;
        Model candidate = m.copy();
        boolean ok = candidate.recoversAs(recovered);
        for (int k = 0; k < m.pending.size() && !ok; k++) {
            m.pending.get(k).accept(candidate);
            ok = candidate.recoversAs(recovered);
        }
        if (!ok && enabled) {
            throw new InvariantViolation("Durability violated at t=" + now + "ms: " + n + " recovered "
                    + describe(recovered) + " but had synced " + describe(m)
                    + " (+" + m.pending.size() + " unsynced writes, any prefix of which may survive)");
        }
        Model fresh = new Model();
        fresh.hardState = recovered.hardState();
        fresh.snapshot = recovered.snapshot();
        fresh.baseIndex = recovered.snapshot().map(Snapshot::index).orElse(0L);
        fresh.baseTerm = recovered.snapshot().map(Snapshot::term).orElse(0L);
        fresh.log.addAll(recovered.entries());
        models.put(n, fresh);
    }

    /** (b) Every index this node reports as committed, the first time any node does, is durable on a majority. */
    public void observeCommit(long now, RaftStatus s, LogView log) {
        for (long i = Math.max(commitCheckedThrough + 1, log.snapshotIndex() + 1); i <= s.commitIndex(); i++) {
            long term = log.termAt(i).orElseThrow();
            int holders = 0;
            for (NodeId n : members) if (models.get(n).holds(i, term)) holders++;
            committedTerm.put(i, term);
            committedInTerm.put(i, s.soft().term());
            if (holders < members.size() / 2 + 1 && enabled) {
                throw new InvariantViolation("Durability violated at t=" + now + "ms: " + s.id()
                        + " reports index " + i + " (term " + term + ") committed, but only " + holders
                        + " of " + members.size() + " nodes hold it durably; a majority is required");
            }
            commitCheckedThrough = i;
        }
        commitCheckedThrough = Math.max(commitCheckedThrough, s.commitIndex());
    }

    public long restartsChecked() { return restartsChecked; }

    private static String describe(PersistentState p) {
        long base = p.snapshot().map(Snapshot::index).orElse(0L);
        return "{term=" + p.hardState().currentTerm() + " vote=" + p.hardState().votedFor().map(NodeId::toString).orElse("-")
                + " snapshot=" + base + " lastIndex=" + (base + p.entries().size()) + "}";
    }

    private static String describe(Model m) {
        return "{term=" + m.hardState.currentTerm() + " vote=" + m.hardState.votedFor().map(NodeId::toString).orElse("-")
                + " snapshot=" + m.snapshot.map(Snapshot::index).orElse(0L) + " base=" + m.baseIndex
                + " lastIndex=" + m.lastIndex() + "}";
    }
}
