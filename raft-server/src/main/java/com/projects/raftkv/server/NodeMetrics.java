package com.projects.raftkv.server;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.transport.GrpcTransport;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * The brief's M5 metrics, served at {@code GET /metrics} in Prometheus text format (DESIGN.md M5-D):
 * term, role, commitIndex, lastApplied, election count, replication lag and fsync latency, plus leader
 * changes, pre-vote rounds, CheckQuorum step-downs, transport and mailbox counters. Gauges read the
 * loop's published {@link RaftServer.View}; counters are derived from role changes between views.
 */
public final class NodeMetrics {
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final Supplier<RaftServer.View> view;
    private final Counter elections;
    private final Counter preVoteRounds;
    private final Counter leaderChanges;
    private final Counter checkQuorumStepDowns;
    private final Timer fsync;

    NodeMetrics(NodeId self, Set<NodeId> peers, Supplier<RaftServer.View> view, IntSupplier mailboxSize) {
        this.view = view;
        registry.config().commonTags("node", self.toString());
        Gauge.builder("raft.term", this, m -> m.current() == null ? 0 : m.current().status().hard().currentTerm())
                .description("current term").register(registry);
        Gauge.builder("raft.role", this, m -> m.current() == null ? 0 : m.current().role().ordinal())
                .description("0 follower, 1 pre-candidate, 2 candidate, 3 leader").register(registry);
        Gauge.builder("raft.is.leader", this, m -> m.current() != null && m.current().role() == Role.LEADER ? 1 : 0)
                .register(registry);
        Gauge.builder("raft.commit.index", this, m -> m.current() == null ? 0 : m.current().status().commitIndex())
                .register(registry);
        Gauge.builder("raft.last.applied", this, m -> m.current() == null ? 0 : m.current().status().lastApplied())
                .register(registry);
        Gauge.builder("raft.last.log.index", this, m -> m.current() == null ? 0 : m.current().lastLogIndex())
                .register(registry);
        Gauge.builder("raft.snapshots.taken", this, m -> m.current() == null ? 0 : m.current().snapshotsTaken())
                .register(registry);
        Gauge.builder("raft.halted", this, m -> m.current() != null && m.current().halted() ? 1 : 0).register(registry);
        Gauge.builder("raft.mailbox.size", mailboxSize, IntSupplier::getAsInt).register(registry);
        for (NodeId p : peers) {
            Gauge.builder("raft.replication.lag", this, m -> m.lagOf(p))
                    .description("leader only: entries the peer is behind the leader's log (lastLogIndex - matchIndex)")
                    .tags(Tags.of("peer", p.toString())).register(registry);
        }
        elections = Counter.builder("raft.elections").description("elections started by this node").register(registry);
        preVoteRounds = Counter.builder("raft.prevote.rounds").description("pre-vote rounds started").register(registry);
        leaderChanges = Counter.builder("raft.leader.changes").description("times this node saw a new leader").register(registry);
        checkQuorumStepDowns = Counter.builder("raft.checkquorum.stepdowns")
                .description("times this node stepped down as leader without a higher term").register(registry);
        fsync = Timer.builder("raft.fsync").description("fsync latency (files and directory)")
                .publishPercentiles(0.5, 0.99).publishPercentileHistogram().register(registry);
    }

    void bindTransport(GrpcTransport t, Set<NodeId> peers) {
        for (NodeId p : peers) {
            Tags tags = Tags.of("peer", p.toString());
            FunctionCounter.builder("raft.transport.sent", t, x -> x.sent(p)).tags(tags).register(registry);
            FunctionCounter.builder("raft.transport.dropped", t, x -> x.dropped(p)).tags(tags).register(registry);
            Gauge.builder("raft.transport.queued", t, x -> x.queued(p)).tags(tags).register(registry);
        }
    }

    void recordFsync(long nanos) {
        fsync.record(nanos, TimeUnit.NANOSECONDS);
    }

    /** Counts the transitions between two consecutive views (the loop calls this after every event). */
    void observe(RaftServer.View prev, RaftServer.View next) {
        if (prev == null) return;
        Role was = prev.role();
        Role now = next.role();
        long prevTerm = prev.status().hard().currentTerm();
        long term = next.status().hard().currentTerm();
        if (now == Role.CANDIDATE && (was != Role.CANDIDATE || term > prevTerm)) elections.increment();
        if (now == Role.PRE_CANDIDATE && was != Role.PRE_CANDIDATE) preVoteRounds.increment();
        if (next.status().soft().leader().isPresent() && !next.status().soft().leader().equals(prev.status().soft().leader()))
            leaderChanges.increment();
        if (was == Role.LEADER && now == Role.FOLLOWER && term == prevTerm) checkQuorumStepDowns.increment();
    }

    private RaftServer.View current() { return view.get(); }

    private double lagOf(NodeId peer) {
        RaftServer.View v = current();
        if (v == null || v.role() != Role.LEADER) return 0;
        Long match = v.matchIndexes().get(peer);
        return match == null ? 0 : Math.max(0, v.lastLogIndex() - match);
    }

    public double counter(String name) {
        Counter c = registry.find(name).counter();
        return c == null ? Double.NaN : c.count();
    }

    public long fsyncCount() { return fsync.count(); }

    /** Prometheus text exposition format. */
    public String scrape() { return registry.scrape(); }
}
