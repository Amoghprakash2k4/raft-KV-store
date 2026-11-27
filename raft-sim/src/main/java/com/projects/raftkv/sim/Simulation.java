package com.projects.raftkv.sim;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.ClientSession.Reply;
import com.projects.raftkv.core.Event;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftConfig;
import com.projects.raftkv.core.RaftCore;
import com.projects.raftkv.core.RaftNodeCore;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.SoftState;
import com.projects.raftkv.node.NodeDriver;
import com.projects.raftkv.node.NodeHaltedException;
import com.projects.raftkv.node.ProposalOutcome;
import com.projects.raftkv.node.ReadOutcome;
import com.projects.raftkv.statemachine.KvStateMachine;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.storage.StorageException;
import com.projects.raftkv.storage.disk.SimDisk;
import com.projects.raftkv.storage.wal.FileWalStorage;
import com.projects.raftkv.storage.wal.WalOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * Deterministic discrete-event simulation of a cluster: virtual time, a seeded network, a seeded fault
 * schedule, closed-loop simulated clients over lossy client links, invariant checks after every event,
 * and a linearizability check of everything the clients saw at the end (invariant 7). Each node runs the real
 * NodeDriver and KvStateMachine over the real write-ahead log (FileWalStorage) on a SimDisk, which loses or
 * tears unsynced data on a crash; every restart runs the real WAL recovery.
 * Same (seed, config) gives the same run.
 */
public final class Simulation {

    /**
     * Liveness, checked over the last window of the quiet period: a current-term leader exists, client
     * commands still commit, and every node has applied what was committed one window before the end.
     */
    private static final long LIVENESS_WINDOW_MS = 1_000;
    /**
     * Every client must complete an operation within this window at the end (DEVIATIONS.md H6). Client
     * links keep dropping 5% of messages in the quiet period, so a client can be unlucky for a while:
     * over 4 × 5,000 seeds its idle time at the end peaked at 1,089 ms (p99.9 ≈ 790 ms) whether or not
     * PreVote/CheckQuorum were on. 1 s (M4) sat at that tail; 2 s does not.
     */
    private static final long CLIENT_LIVENESS_WINDOW_MS = Long.getLong("sim.clientWindow", 2_000);

    /** Search budget for the end-of-run linearizability check (exceeding it fails the run). */
    private static final long LINEARIZABILITY_STEP_BUDGET = 20_000_000;

    private record Scheduled(long time, long seq, Runnable action) {}

    /** A client read waiting in a node's inbox; registered with the driver only when the batch runs. */
    private record PendingRead(long delivery, Bytes payload) {}

    /** Largest batch one drain hands to the driver (the real server uses the same bound). */
    private static final int MAX_BATCH = 256;

    private static final class SimNode {
        final NodeId id;
        final SimDisk disk;
        final NodeLogTracker tracker;
        final long snapshotEvery;
        TrackedStorage trackedStorage;   // wraps this incarnation's WAL
        boolean midWriteCrashArmed;
        NodeDriver driver;          // null while crashed
        KvStateMachine stateMachine;
        int incarnation;
        long pausedUntil;
        SoftState lastSoft;
        long lastAppliedSeen = -1;
        /** Group commit (M6-A): events waiting for the next batch; an Event or a PendingRead. */
        final List<Object> inbox = new ArrayList<>();
        boolean drainScheduled;

        SimNode(NodeId id, SplittableRandom diskRng, NodeLogTracker tracker, long snapshotEvery) {
            this.id = id;
            this.disk = new SimDisk(diskRng);
            this.tracker = tracker;
            this.snapshotEvery = snapshotEvery;
        }
        boolean up() { return driver != null; }
        String name() { return id.toString(); }
    }

    private final long seed;
    private final SimConfig cfg;
    private final SplittableRandom netRng;
    private final SplittableRandom faultRng;
    private final SplittableRandom nodeRng;
    private final SplittableRandom clientRng;
    private final SplittableRandom linkRng;
    private final List<NodeId> members;
    private final List<SimNode> nodes = new ArrayList<>();
    private final boolean[][] blocked;      // blocked[from][to], 1-based ids
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>(
            (a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Long.compare(a.seq, b.seq));
    /** Lines kept for a failure report: -Psim.trace=N keeps more (debugging a replayed seed). */
    private final Trace trace = new Trace(Integer.getInteger("sim.trace", 300));
    private final ElectionSafetyChecker electionSafety = new ElectionSafetyChecker();
    private final VoteDurabilityChecker voteDurability = new VoteDurabilityChecker();
    private final LogMatchingChecker logMatching = new LogMatchingChecker();
    private final LeaderAppendOnlyChecker leaderAppendOnly = new LeaderAppendOnlyChecker();
    private final StateMachineSafetyChecker stateMachineSafety = new StateMachineSafetyChecker();
    private final ReplicaStateChecker replicaState = new ReplicaStateChecker();
    private final LeaderCompletenessChecker leaderCompleteness = new LeaderCompletenessChecker();
    private final DurabilityChecker durability;
    private final ClientWorkload workload;
    private final HistoryRecorder history = new HistoryRecorder();
    /** Each copy of a client request delivered to a node gets its own id there; this maps it to the attempt. */
    private final Map<Long, Long> attemptOfDelivery = new HashMap<>();
    private long nextDeliveryId = 1;

    private long now;
    private long seq;
    private long events;
    private long batches, batchedEvents, maxBatch;   // group commit (M6-A)
    private long firstLeaderAt = -1;
    private long lastNewLeaderAt = -1;
    private long lastCurrentLeaderSeenAt = -1;
    private int crashes;
    private int partitions;
    private int pauses;
    private int midWriteCrashes;
    private int tornTailsCut;
    private long snapshotsTaken;
    private long snapshotsInstalled;
    private long maxLogEntries;
    private long commitTargetForLiveness = -1;
    private long lostProposals;
    private long unknownProposals;
    private long duplicatesAnswered;
    private long linkDuplicates;
    private long linearizabilitySteps;
    // The scripted scenario (SimConfig.Scenario, DESIGN.md M5-A).
    private SimNode scenarioNode;
    private long scenarioStartedAt = -1;
    private long termAtScenarioStart = -1;
    private int newLeadersSinceScenario;
    private long scenarioStepDownMs = -1;

    private Simulation(long seed, SimConfig cfg) {
        this.seed = seed;
        this.cfg = cfg;
        SplittableRandom root = new SplittableRandom(seed);
        this.netRng = root.split();
        this.faultRng = root.split();
        this.nodeRng = root.split();
        this.clientRng = root.split();
        SplittableRandom diskRng = root.split();
        this.linkRng = root.split();
        this.members = IntStream.rangeClosed(1, cfg.clusterSize()).mapToObj(NodeId::new).toList();
        this.blocked = new boolean[cfg.clusterSize() + 1][cfg.clusterSize() + 1];
        int every = cfg.storage().snapshotEvery();
        for (NodeId id : members) {
            SplittableRandom r = diskRng.split();
            long snapshotEvery = every == 0 ? 0 : every + r.nextInt(every);
            nodes.add(new SimNode(id, r, new NodeLogTracker(leaderCompleteness::committedHashAt), snapshotEvery));
        }
        this.durability = new DurabilityChecker(members, cfg.checkDurability());
        this.workload = new ClientWorkload(cfg.workload(), members, clientRng, this::at, this::sendToNode, () -> now,
                history);
    }

    public static SimResult run(long seed, SimConfig cfg) {
        Simulation sim = new Simulation(seed, cfg);
        try {
            return sim.execute();
        } catch (RuntimeException e) {
            SimResult partial;
            try {
                partial = sim.result(sim.now);
            } catch (RuntimeException ignored) {
                partial = null;   // statistics are a courtesy; the failure is what matters
            }
            throw new SimulationFailure(seed, cfg, sim.trace.format(), e, partial, sim.now);
        }
    }

    private SimResult execute() {
        for (SimNode n : nodes) {
            n.disk.setFsyncIsNoop(cfg.lyingDisks());
            start(n);
        }
        if (cfg.faults().enabled()) scheduleNextFault();
        if (cfg.chaosMs() > 0) at(cfg.chaosMs(), this::endChaos);
        if (cfg.scenario() != SimConfig.Scenario.NONE) {
            at(SimConfig.Scenario.START_MS, this::startScenario);
            at(SimConfig.Scenario.HEAL_MS, this::endScenario);
        }
        workload.start();

        long end = cfg.chaosMs() + cfg.quietMs();
        at(end - LIVENESS_WINDOW_MS, () -> commitTargetForLiveness = maxCommitIndex());
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
        if (cfg.workload().clients() > 0) {
            checkLinearizability();
            checkClientLiveness(end);
        }
        for (SimNode n : nodes) if (n.up()) countSnapshots(n);
        return result(end);
    }

    /** The run's counters as of {@code end} (the end of the run, or the moment it failed). */
    private SimResult result(long end) {
        long maxTerm = nodes.stream().filter(SimNode::up)
                .mapToLong(n -> n.driver.status().hard().currentTerm()).max().orElse(0);
        return new SimResult(seed, events, trace.hash(), electionSafety.leadersElected(), maxTerm,
                firstLeaderAt, lastNewLeaderAt, crashes, partitions, pauses,
                workload.opsStarted, workload.writesCompleted, workload.readsCompleted, workload.attempts,
                workload.timeouts, workload.redirects, workload.retryReplies, history.unknown(),
                lostProposals, unknownProposals, duplicatesAnswered, linkDuplicates, linearizabilitySteps,
                maxCommitIndex(), midWriteCrashes, tornTailsCut, durability.restartsChecked(),
                snapshotsTaken, snapshotsInstalled, maxLogEntries,
                termAtScenarioStart, newLeadersSinceScenario, scenarioStepDownMs,
                cfg.workload().clients() > 0 ? end - workload.slowestClientLastCompletedAt() : 0,
                batches, batchedEvents, maxBatch);
    }

    /**
     * Invariant 7: what the clients saw is linearizable. Operations still unanswered at the end may or may
     * not have taken effect. Safety, so it runs before the liveness checks: a violation is reported as such.
     */
    private void checkLinearizability() {
        LinearizabilityChecker.Result r = LinearizabilityChecker.check(history.history(), LINEARIZABILITY_STEP_BUDGET);
        linearizabilitySteps = r.steps();
        if (!r.linearizable()) {
            throw new InvariantViolation((r.budgetExceeded() ? "Linearizability undecided: " : "Linearizability violated: ")
                    + r.describe());
        }
    }

    /**
     * After the faults heal: every client keeps completing operations, writes keep committing, and every
     * node catches up (restarted ones included).
     */
    private void checkClientLiveness(long end) {
        if (workload.lastWriteCompletedAt < end - LIVENESS_WINDOW_MS) {
            throw new InvariantViolation("Liveness: no client write completed in the last " + LIVENESS_WINDOW_MS
                    + "ms of the quiet period (last at " + workload.lastWriteCompletedAt + "ms)");
        }
        if (workload.slowestClientLastCompletedAt() < end - CLIENT_LIVENESS_WINDOW_MS) {
            throw new InvariantViolation("Liveness: a client completed nothing in the last " + CLIENT_LIVENESS_WINDOW_MS
                    + "ms of the quiet period (its last completion at " + workload.slowestClientLastCompletedAt() + "ms)");
        }
        for (SimNode n : nodes) {
            long applied = n.driver.status().lastApplied();
            if (applied < commitTargetForLiveness) {
                throw new InvariantViolation("Liveness: " + n.name() + " applied only up to " + applied
                        + " but index " + commitTargetForLiveness + " was committed " + LIVENESS_WINDOW_MS
                        + "ms before the end");
            }
        }
    }

    private long maxCommitIndex() {
        long max = 0;
        for (SimNode n : nodes) if (n.up()) max = Math.max(max, n.driver.status().commitIndex());
        return max;
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
        RaftConfig rc = new RaftConfig(n.id, members, cfg.electionTimeoutTicks(), cfg.heartbeatTicks(),
                cfg.maxEntriesPerAppend(), cfg.storage().snapshotChunkBytes(), cfg.preVote(), cfg.checkQuorum());
        FileWalStorage wal;
        try {
            wal = FileWalStorage.open(n.disk, new WalOptions(cfg.storage().walSegmentBytes(), cfg.storage().mutations()));
        } catch (StorageException e) {
            throw new InvariantViolation("Durability violated at t=" + now + "ms: " + n.name()
                    + " cannot recover its WAL: " + e.getMessage());
        }
        if (wal.tornBytesCut() > 0) {
            tornTailsCut++;
            trace.add(now, n.name(), "WAL recovery cut a torn tail of " + wal.tornBytesCut() + " bytes");
        }
        PersistentState recovered = wal.loadAll();
        durability.onRecover(now, n.id, recovered);
        n.trackedStorage = new TrackedStorage(wal, n.tracker, durability, n.id);
        RaftCore core = cfg.mutations().isEmpty()
                ? new RaftNodeCore(rc, recovered, nodeRng.split())
                : RaftNodeCore.withMutationsForTesting(rc, recovered, nodeRng.split(), cfg.mutations());
        n.tracker.restart(recovered);
        stateMachineSafety.onRestart(n.id);
        n.stateMachine = cfg.skipDedup() ? KvStateMachine.withoutDedupForTesting() : new KvStateMachine();
        NodeDriver.restoreFrom(recovered, n.stateMachine);   // the snapshot; the rest is re-applied from the log
        n.driver = NodeDriver.create(core, n.trackedStorage, this::onSend, n.stateMachine, this::onOutcome,
                this::onReadOutcome, n.snapshotEvery).withMutationsForTesting(cfg.mutations());
        n.lastSoft = null;
        n.lastAppliedSeen = -1;
        trace.add(now, n.name(), "start #" + n.incarnation + " " + n.driver.status().hard());
        observe(n);
        int incarnation = n.incarnation;
        at(now + 1 + nodeRng.nextInt(cfg.tickMs()), () -> tick(n, incarnation));
    }

    private void crash(SimNode n, String why) {
        countSnapshots(n);
        n.disk.crash();
        n.midWriteCrashArmed = false;
        n.driver = null;
        n.inbox.clear();          // the process's mailbox dies with it
        n.drainScheduled = false;
        crashes++;
        trace.add(now, n.name(), "CRASH (" + why + ")");
    }

    private void countSnapshots(SimNode n) {
        snapshotsTaken += n.driver.snapshotsTaken();
        snapshotsInstalled += n.driver.snapshotsInstalled();
        duplicatesAnswered += n.stateMachine.duplicatesAnswered();
    }

    private void tick(SimNode n, int incarnation) {
        if (!n.up() || n.incarnation != incarnation) return;     // tick chain of a dead incarnation
        if (now >= n.pausedUntil) deliver(n, new Event.Tick());   // a paused process sees no ticks
        at(now + cfg.tickMs(), () -> tick(n, incarnation));
    }

    private void deliver(SimNode n, Event event) {
        if (!(event instanceof Event.Tick)) trace.add(now, n.name(), event);
        if (cfg.batchWindowMs() == 0) {
            run(n, d -> d.handle(event));
        } else {
            enqueue(n, event);
        }
    }

    /** Group commit: the event waits in the inbox; the first one schedules the drain. */
    private void enqueue(SimNode n, Object item) {
        n.inbox.add(item);
        if (!n.drainScheduled) {
            n.drainScheduled = true;
            int incarnation = n.incarnation;
            at(now + cfg.batchWindowMs(), () -> drain(n, incarnation));
        }
    }

    /** Hands everything waiting (up to MAX_BATCH) to the driver as one batch: one fsync for all of it. */
    private void drain(SimNode n, int incarnation) {
        if (!n.up() || n.incarnation != incarnation) return;      // a drain of a dead incarnation
        if (now < n.pausedUntil) {                                 // a paused process handles nothing
            at(n.pausedUntil, () -> drain(n, incarnation));
            return;
        }
        n.drainScheduled = false;
        List<Object> items = new ArrayList<>(n.inbox.subList(0, Math.min(MAX_BATCH, n.inbox.size())));
        n.inbox.subList(0, items.size()).clear();
        if (!n.inbox.isEmpty()) {
            n.drainScheduled = true;
            at(now, () -> drain(n, incarnation));
        }
        batches++;
        batchedEvents += items.size();
        maxBatch = Math.max(maxBatch, items.size());
        run(n, d -> {
            List<Event> events = new ArrayList<>(items.size());
            for (Object item : items) {
                events.add(item instanceof PendingRead r ? d.prepareRead(r.delivery(), r.payload()) : (Event) item);
            }
            d.handleBatch(events);
        });
    }

    /** Runs one driver call (an event, or a read) and then every invariant checker; a halted node crashes. */
    private void run(SimNode n, Consumer<NodeDriver> call) {
        try {
            call.accept(n.driver);
        } catch (NodeHaltedException e) {
            if (n.midWriteCrashArmed) midWriteCrashes++;
            crash(n, "halted: " + e.getMessage());
            FaultConfig f = cfg.faults();
            at(now + between(faultRng, f.minDownMs(), f.maxDownMs()), () -> restart(n));
            return;
        }
        observe(n);
    }

    /** Runs every invariant checker against this node's state right after it handled an event. */
    private void observe(SimNode n) {
        RaftStatus s = n.driver.status();
        NodeLogTracker.Change change = n.tracker.refresh(n.driver.log());
        electionSafety.observe(now, s);
        if (cfg.checkDurability()) voteDurability.observe(now, s);
        maxLogEntries = Math.max(maxLogEntries, n.driver.log().lastIndex() - n.driver.log().snapshotIndex());
        leaderAppendOnly.observe(now, s, n.tracker.lastIndex(), change.truncated());
        logMatching.observe(now, s.id(), n.driver.log(), n.tracker, change.fromIndex());
        stateMachineSafety.observe(now, s, n.driver.log());
        leaderCompleteness.observe(now, s, n.tracker);
        durability.observeCommit(now, s, n.driver.log());
        if (s.lastApplied() != n.lastAppliedSeen) {
            replicaState.observe(now, s.id(), s.lastApplied(), n.stateMachine.stateHash());
            n.lastAppliedSeen = s.lastApplied();
        }

        if (!s.soft().equals(n.lastSoft)) {
            trace.add(now, n.name(), "-> " + s.soft().role().name() + " term=" + s.soft().term()
                    + " leader=" + s.soft().leader().map(NodeId::toString).orElse("?"));
            if (s.soft().role() == Role.LEADER) {
                if (firstLeaderAt < 0) firstLeaderAt = now;
                lastNewLeaderAt = now;
                if (scenarioStartedAt >= 0) newLeadersSinceScenario++;
            }
            if (n == scenarioNode && cfg.scenario() == SimConfig.Scenario.ISOLATE_LEADER
                    && scenarioStepDownMs < 0 && s.soft().role() != Role.LEADER) {
                scenarioStepDownMs = now - scenarioStartedAt;
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

    // ---------------------------------------------------------------- clients

    /**
     * A client attempt travels to its target over a lossy link (DESIGN.md M4-A): it may be dropped,
     * duplicated or delayed (long delays included) like peer messages, so retries and late duplicates
     * really happen. Each delivered copy gets its own proposal/read id at the node.
     */
    private void sendToNode(String client, ClientSession.Attempt a) {
        trace.add(now, client, (a.read() ? "read #" : "write #") + Long.toHexString(a.attemptId()) + " -> " + a.target());
        if (linkRng.nextDouble() < cfg.network().dropProbability()) return;
        at(now + linkDelay(), () -> arriveAtNode(a));
        if (linkRng.nextDouble() < cfg.network().duplicateProbability()) {
            linkDuplicates++;
            at(now + linkDelay(), () -> arriveAtNode(a));
        }
    }

    private void arriveAtNode(ClientSession.Attempt a) {
        SimNode n = nodes.get(a.target().value() - 1);
        if (!n.up()) return;                                  // lost: the node is down
        if (now < n.pausedUntil) {
            at(n.pausedUntil, () -> arriveAtNode(a));
            return;
        }
        long delivery = nextDeliveryId++;
        attemptOfDelivery.put(delivery, a.attemptId());
        if (a.read()) {
            trace.add(now, n.name(), "read " + delivery + " (attempt " + Long.toHexString(a.attemptId()) + ")");
            if (cfg.batchWindowMs() == 0) run(n, d -> d.read(delivery, a.payload()));
            else enqueue(n, new PendingRead(delivery, a.payload()));
        } else {
            deliver(n, new Event.ProposeCommand(delivery, a.payload()));
        }
    }

    private void onOutcome(ProposalOutcome o) {
        trace.add(now, "cli", o);
        Reply reply = switch (o) {
            case ProposalOutcome.Committed c -> new Reply.Ok(c.result());
            case ProposalOutcome.Rejected r -> new Reply.NotLeader(r.leaderHint());
            case ProposalOutcome.Lost l -> { lostProposals++; yield new Reply.Retry(); }
            case ProposalOutcome.Unknown u -> { unknownProposals++; yield new Reply.Retry(); }
        };
        replyToClient(o.proposalId(), reply);
    }

    private void onReadOutcome(ReadOutcome o) {
        trace.add(now, "cli", o);
        Reply reply = switch (o) {
            case ReadOutcome.Ok ok -> new Reply.Ok(ok.result());
            case ReadOutcome.Rejected r -> new Reply.NotLeader(r.leaderHint());
        };
        replyToClient(o.readId(), reply);
    }

    /** The answer travels back over the same kind of lossy link. */
    private void replyToClient(long delivery, Reply reply) {
        Long attempt = attemptOfDelivery.remove(delivery);
        if (attempt == null) throw new IllegalStateException("second outcome for delivery " + delivery);
        if (linkRng.nextDouble() < cfg.network().dropProbability()) return;
        at(now + linkDelay(), () -> workload.onReply(attempt, reply));
        if (linkRng.nextDouble() < cfg.network().duplicateProbability()) {
            linkDuplicates++;
            at(now + linkDelay(), () -> workload.onReply(attempt, reply));
        }
    }

    private long linkDelay() {
        NetworkConfig net = cfg.network();
        long delay = between(linkRng, net.minDelayMs(), net.maxDelayMs());
        if (linkRng.nextDouble() < net.longDelayProbability()) {
            delay *= between(linkRng, net.longDelayMinFactor(), net.longDelayMaxFactor());
        }
        return delay;
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
        if (f.targetLeader() && faultRng.nextInt(10) < 7) {
            java.util.Optional<SimNode> leader = currentLeader();
            if (leader.isPresent()) {
                injectLeaderFault(leader.get());
                return;
            }
        }
        int kind = f.crashLoop() && faultRng.nextInt(10) < 7 ? (faultRng.nextBoolean() ? 3 : 5) : faultRng.nextInt(6);
        switch (kind) {
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
            case 5 -> randomUpNode().filter(n -> !n.midWriteCrashArmed).ifPresent(n -> {
                // Crash during the node's next fsync: it dies with that batch written but not synced,
                // so the disk keeps none, all or a torn prefix of it (SimDisk crash model).
                n.disk.failNextFsync();
                n.midWriteCrashArmed = true;
                trace.add(now, n.name(), "ARM crash during next fsync");
            });
            default -> throw new IllegalStateException("unreachable");
        }
    }

    /** The up node that is leader in the highest term, if any. */
    private java.util.Optional<SimNode> currentLeader() {
        return nodes.stream().filter(SimNode::up)
                .filter(n -> n.driver.status().soft().role() == Role.LEADER)
                .max(java.util.Comparator.comparingLong(n -> n.driver.status().hard().currentTerm()));
    }

    /**
     * DESIGN.md M4-F: the faults under which a leader's reads can go stale. Isolated: the others elect a
     * new leader while clients can still reach this one. Paused: it resumes believing it still leads and
     * finds client requests queued. Crashed: a new leader takes over with a commitIndex that may lag.
     */
    private void injectLeaderFault(SimNode leader) {
        FaultConfig f = cfg.faults();
        switch (faultRng.nextInt(3)) {
            case 0 -> {
                healLinks();
                for (NodeId other : members) {
                    if (other.equals(leader.id)) continue;
                    blocked[leader.id.value()][other.value()] = true;
                    blocked[other.value()][leader.id.value()] = true;
                }
                partitions++;
                trace.add(now, "sim", "ISOLATE LEADER " + leader.name());
            }
            case 1 -> {
                if (now < leader.pausedUntil) return;
                leader.pausedUntil = now + between(faultRng, f.minPauseMs(), f.maxPauseMs());
                pauses++;
                trace.add(now, leader.name(), "PAUSE LEADER until " + leader.pausedUntil + "ms");
            }
            default -> {
                crash(leader, "injected (leader)");
                at(now + between(faultRng, f.minDownMs(), f.maxDownMs()), () -> restart(leader));
            }
        }
    }

    /**
     * Cut one node off from every other node (client links stay up). Waits for a leader first: both
     * scenarios are about a healthy cluster with a leader.
     */
    private void startScenario() {
        java.util.Optional<SimNode> leader = currentLeader();
        if (leader.isEmpty()) {
            at(now + 50, this::startScenario);
            return;
        }
        SimNode target = cfg.scenario() == SimConfig.Scenario.ISOLATE_LEADER ? leader.get()
                : nodes.stream().filter(n -> n != leader.get() && n.up()).findFirst().orElseThrow();
        for (NodeId other : members) {
            if (other.equals(target.id)) continue;
            blocked[target.id.value()][other.value()] = true;
            blocked[other.value()][target.id.value()] = true;
        }
        scenarioNode = target;
        scenarioStartedAt = now;
        termAtScenarioStart = maxLiveTerm();
        partitions++;
        trace.add(now, "sim", "SCENARIO " + cfg.scenario() + ": isolate " + target.name());
    }

    private void endScenario() {
        healLinks();
        trace.add(now, "sim", "SCENARIO heal");
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
            if (n.midWriteCrashArmed) {
                n.disk.clearFailNextFsync();
                n.midWriteCrashArmed = false;
            }
        }
        trace.add(now, "sim", "END CHAOS: links healed, all nodes up and running");
    }
}
