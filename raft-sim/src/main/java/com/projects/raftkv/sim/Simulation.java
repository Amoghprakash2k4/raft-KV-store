package com.projects.raftkv.sim;

import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.SoftState;
import com.projects.raftkv.node.NodeDriver;
import com.projects.raftkv.node.NodeHaltedException;
import com.projects.raftkv.storage.InMemoryStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

/**
 * Deterministic discrete-event simulation of a cluster: virtual time, a seeded network, a seeded fault
 * schedule, and invariant checks after every event. Each node runs the real NodeDriver over an
 * InMemoryStorage that loses unsynced data on crash. Same (seed, config) gives the same run.
 */
public final class Simulation {

    /** Liveness: after faults heal, a current-term leader must be seen within the last window. */
    private static final long LIVENESS_WINDOW_MS = 1_000;

    private record Scheduled(long time, long seq, Runnable action) {}

    private static final class SimNode {
        final NodeId id;
        final InMemoryStorage storage = new InMemoryStorage();
        NodeDriver driver;          // null while crashed
        int incarnation;
        long pausedUntil;
        SoftState lastSoft;

        SimNode(NodeId id) { this.id = id; }
        boolean up() { return driver != null; }
        String name() { return id.toString(); }
    }

    private final long seed;
    private final SimConfig cfg;
    private final SplittableRandom netRng;
    private final SplittableRandom faultRng;
    private final SplittableRandom nodeRng;
    private final List<NodeId> members;
    private final List<SimNode> nodes = new ArrayList<>();
    private final boolean[][] blocked;      // blocked[from][to], 1-based ids
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>(
            (a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Long.compare(a.seq, b.seq));
    private final Trace trace = new Trace(300);
    private final ElectionSafetyChecker electionSafety = new ElectionSafetyChecker();
    private final VoteDurabilityChecker voteDurability = new VoteDurabilityChecker();

    private long now;
    private long seq;
    private long events;
    private long firstLeaderAt = -1;
    private long lastNewLeaderAt = -1;
    private long lastCurrentLeaderSeenAt = -1;
    private int crashes;
    private int partitions;
    private int pauses;

    private Simulation(long seed, SimConfig cfg) {
        this.seed = seed;
        this.cfg = cfg;
        SplittableRandom root = new SplittableRandom(seed);
        this.netRng = root.split();
        this.faultRng = root.split();
        this.nodeRng = root.split();
        this.members = IntStream.rangeClosed(1, cfg.clusterSize()).mapToObj(NodeId::new).toList();
        this.blocked = new boolean[cfg.clusterSize() + 1][cfg.clusterSize() + 1];
        for (NodeId id : members) nodes.add(new SimNode(id));
    }

    public static SimResult run(long seed, SimConfig cfg) {
        Simulation sim = new Simulation(seed, cfg);
        try {
            return sim.execute();
        } catch (RuntimeException e) {
            throw new SimulationFailure(seed, cfg, sim.trace.format(), e);
        }
    }

    private SimResult execute() {
        for (SimNode n : nodes) {
            n.storage.setFsyncIsNoop(cfg.lyingDisks());
            start(n);
        }
        if (cfg.faults().enabled()) scheduleNextFault();
        if (cfg.chaosMs() > 0) at(cfg.chaosMs(), this::endChaos);

        long end = cfg.chaosMs() + cfg.quietMs();
        while (!queue.isEmpty() && queue.peek().time <= end) {
            Scheduled s = queue.poll();
            now = s.time;
            s.action.run();
            events++;
        }
        now = end;
        if (lastCurrentLeaderSeenAt < end - LIVENESS_WINDOW_MS) {
            throw new InvariantViolation("Liveness: no current-term leader in the last " + LIVENESS_WINDOW_MS
                    + "ms of the quiet period (last seen at " + lastCurrentLeaderSeenAt + "ms)");
        }
        long maxTerm = nodes.stream().filter(SimNode::up)
                .mapToLong(n -> n.driver.status().hard().currentTerm()).max().orElse(0);
        return new SimResult(seed, events, trace.hash(), electionSafety.leadersElected(), maxTerm,
                firstLeaderAt, lastNewLeaderAt, crashes, partitions, pauses);
    }

    // ---------------------------------------------------------------- scheduling

    private void at(long time, Runnable action) {
        queue.add(new Scheduled(time, seq++, action));
    }

    private static int between(SplittableRandom rng, int lo, int hi) {
        return lo + rng.nextInt(hi - lo + 1);
    }

    // ---------------------------------------------------------------- node lifecycle

    private void start(SimNode n) {
        n.incarnation++;
        RaftConfig rc = new RaftConfig(n.id, members, cfg.electionTimeoutTicks(), cfg.heartbeatTicks(), 64);
        n.driver = NodeDriver.recover(rc, n.storage, this::onSend, nodeRng.split());
        n.lastSoft = null;
        trace.add(now, n.name(), "start #" + n.incarnation + " " + n.driver.status().hard());
        observe(n);
        int incarnation = n.incarnation;
        at(now + 1 + nodeRng.nextInt(cfg.tickMs()), () -> tick(n, incarnation));
    }

    private void crash(SimNode n, String why) {
        n.storage.crash();
        n.driver = null;
        crashes++;
        trace.add(now, n.name(), "CRASH (" + why + ")");
    }

    private void tick(SimNode n, int incarnation) {
        if (!n.up() || n.incarnation != incarnation) return;     // tick chain of a dead incarnation
        if (now >= n.pausedUntil) deliver(n, new Event.Tick());   // a paused process sees no ticks
        at(now + cfg.tickMs(), () -> tick(n, incarnation));
    }

    private void deliver(SimNode n, Event event) {
        if (!(event instanceof Event.Tick)) trace.add(now, n.name(), event);
        try {
            n.driver.handle(event);
        } catch (NodeHaltedException e) {
            crash(n, "halted: " + e.getMessage());
            return;
        }
        observe(n);
    }

    private void observe(SimNode n) {
        RaftStatus s = n.driver.status();
        electionSafety.observe(now, s);
        if (cfg.checkVoteDurability()) voteDurability.observe(now, s);

        if (!s.soft().equals(n.lastSoft)) {
            trace.add(now, n.name(), "-> " + s.soft().role().name() + " term=" + s.soft().term()
                    + " leader=" + s.soft().leader().map(NodeId::toString).orElse("?"));
            if (s.soft().role() == Role.LEADER) {
                if (firstLeaderAt < 0) firstLeaderAt = now;
                lastNewLeaderAt = now;
            }
            n.lastSoft = s.soft();
        }
        if (s.soft().role() == Role.LEADER && s.hard().currentTerm() >= maxLiveTerm()) {
            lastCurrentLeaderSeenAt = now;
        }
    }

    private long maxLiveTerm() {
        long max = 0;
        for (SimNode n : nodes) if (n.up()) max = Math.max(max, n.driver.status().hard().currentTerm());
        return max;
    }

    // ---------------------------------------------------------------- network

    private void onSend(Message m) {
        if (blocked[m.from().value()][m.to().value()]) {
            trace.add(now, m.from().toString(), "blocked " + m);
            return;
        }
        NetworkConfig net = cfg.network();
        if (netRng.nextDouble() < net.dropProbability()) {
            trace.add(now, m.from().toString(), "dropped " + m);
            return;
        }
        scheduleDelivery(m);
        if (netRng.nextDouble() < net.duplicateProbability()) scheduleDelivery(m);
    }

    private void scheduleDelivery(Message m) {
        NetworkConfig net = cfg.network();
        long delay = between(netRng, net.minDelayMs(), net.maxDelayMs());
        if (netRng.nextDouble() < net.longDelayProbability()) {
            delay *= between(netRng, net.longDelayMinFactor(), net.longDelayMaxFactor());
        }
        at(now + delay, () -> arrive(m));
    }

    private void arrive(Message m) {
        SimNode to = nodes.get(m.to().value() - 1);
        if (!to.up()) return;                                  // lost: destination is down
        if (now < to.pausedUntil) {                            // queued in the paused process's socket
            at(to.pausedUntil, () -> arrive(m));
            return;
        }
        deliver(to, new Event.MessageReceived(m));
    }

    // ---------------------------------------------------------------- faults

    private void scheduleNextFault() {
        long when = now + between(faultRng, cfg.faults().minGapMs(), cfg.faults().maxGapMs());
        if (when < cfg.chaosMs()) {
            at(when, () -> {
                injectFault();
                scheduleNextFault();
            });
        }
    }

    private void injectFault() {
        FaultConfig f = cfg.faults();
        switch (faultRng.nextInt(5)) {
            case 0 -> partitionSymmetric();
            case 1 -> partitionOneWay();
            case 2 -> {
                healLinks();
                trace.add(now, "sim", "heal links");
            }
            case 3 -> randomUpNode().ifPresent(n -> {
                crash(n, "injected");
                long restartAt = now + between(faultRng, f.minDownMs(), f.maxDownMs());
                at(restartAt, () -> restart(n));
            });
            case 4 -> randomUpNode().filter(n -> now >= n.pausedUntil).ifPresent(n -> {
                n.pausedUntil = now + between(faultRng, f.minPauseMs(), f.maxPauseMs());
                pauses++;
                trace.add(now, n.name(), "PAUSE until " + n.pausedUntil + "ms");
            });
            default -> throw new IllegalStateException("unreachable");
        }
    }

    private void partitionSymmetric() {
        healLinks();
        List<NodeId> shuffled = new ArrayList<>(members);
        for (int i = shuffled.size() - 1; i > 0; i--) {
            int j = faultRng.nextInt(i + 1);
            NodeId tmp = shuffled.get(i);
            shuffled.set(i, shuffled.get(j));
            shuffled.set(j, tmp);
        }
        int k = 1 + faultRng.nextInt(members.size() - 1);
        List<NodeId> side = shuffled.subList(0, k);
        for (NodeId a : members) {
            for (NodeId b : members) {
                if (side.contains(a) != side.contains(b)) blocked[a.value()][b.value()] = true;
            }
        }
        partitions++;
        trace.add(now, "sim", "PARTITION " + side + " | rest");
    }

    private void partitionOneWay() {
        int links = 1 + faultRng.nextInt(2);
        StringBuilder desc = new StringBuilder("ONE-WAY");
        for (int i = 0; i < links; i++) {
            int from = 1 + faultRng.nextInt(members.size());
            int to = 1 + faultRng.nextInt(members.size() - 1);
            if (to >= from) to++;
            blocked[from][to] = true;
            desc.append(" n").append(from).append("-x->n").append(to);
        }
        partitions++;
        trace.add(now, "sim", desc.toString());
    }

    private void healLinks() {
        for (boolean[] row : blocked) java.util.Arrays.fill(row, false);
    }

    private java.util.Optional<SimNode> randomUpNode() {
        List<SimNode> up = nodes.stream().filter(SimNode::up).toList();
        return up.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(up.get(faultRng.nextInt(up.size())));
    }

    private void restart(SimNode n) {
        if (!n.up()) start(n);
    }

    private void endChaos() {
        healLinks();
        for (SimNode n : nodes) {
            n.pausedUntil = Math.min(n.pausedUntil, now);
            restart(n);
        }
        trace.add(now, "sim", "END CHAOS: links healed, all nodes up and running");
    }
}
