package com.projects.raftkv.sim;

import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.storage.StorageMutation;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * One simulation run: chaos for chaosMs, then quietMs with link/node faults healed (network loss and
 * delay continue). Times are virtual milliseconds; one core tick = tickMs.
 * {@code mutations} deliberately break protocol rules (mutation tests only, DESIGN.md M2-C).
 * {@code checkDurability} turns the Vote durability and Durability checkers on.
 * {@code skipDedup} runs every replica's state machine without session dedup (mutation SKIP_DEDUP, M4-F).
 * {@code preVote} / {@code checkQuorum} switch the M5-A liveness features (on by default here);
 * {@code scenario} adds one scripted fault on top of everything else (DESIGN.md M5-A measurements).
 * {@code batchWindowMs} > 0 turns on group commit (DESIGN.md M6-A): a node's events wait in an inbox for
 * up to that long and are then handled as one batch with one fsync, like the real server's mailbox.
 */
public record SimConfig(int clusterSize, int tickMs, int electionTimeoutTicks, int heartbeatTicks,
                        int maxEntriesPerAppend, long chaosMs, long quietMs, NetworkConfig network,
                        FaultConfig faults, WorkloadConfig workload, boolean lyingDisks,
                        boolean checkDurability, Set<ProtocolMutation> mutations, StorageConfig storage,
                        boolean skipDedup, boolean preVote, boolean checkQuorum, Scenario scenario,
                        int batchWindowMs) {

    /**
     * A scripted fault for measuring PreVote and CheckQuorum. At {@link #START_MS} one node is cut off
     * from every other node (client links stay up) and at {@link #HEAL_MS} it is reconnected.
     */
    public enum Scenario {
        NONE,
        /** Cut off one follower of the current leader: the disruptive-server case (thesis §9.6). */
        ISOLATE_FOLLOWER,
        /** Cut off the current leader: the minority-leader case (thesis §6.2). */
        ISOLATE_LEADER;

        public static final long START_MS = 1_000;
        public static final long HEAL_MS = 4_000;
    }

    public SimConfig {
        Objects.requireNonNull(network);
        Objects.requireNonNull(faults);
        Objects.requireNonNull(workload);
        Objects.requireNonNull(storage);
        Objects.requireNonNull(scenario);
        mutations = Set.copyOf(mutations);
        if (batchWindowMs < 0) throw new IllegalArgumentException("batchWindowMs < 0");
    }

    public static SimConfig quiet(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 0, 3_000, NetworkConfig.reliable(), FaultConfig.none(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim(), false, true, true,
                Scenario.NONE, 0);
    }

    public static SimConfig chaos(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 10_000, 3_000, NetworkConfig.chaotic(), FaultConfig.chaos(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim(), false, true, true,
                Scenario.NONE, 0);
    }

    /** The chaos mix with a crash loop: frequent crashes, most of them in the middle of an fsync. */
    public static SimConfig crashLoop(int nodes) {
        return chaos(nodes).withFaults(FaultConfig.crashLoops());
    }

    /**
     * A healthy cluster (reliable network, no random faults, clients running) with one scripted fault,
     * run until 3 s after the heal. Used to measure PreVote and CheckQuorum with the flags on and off.
     */
    public static SimConfig scenario(int nodes, Scenario s) {
        return new SimConfig(nodes, 10, 10, 2, 64, 0, Scenario.HEAL_MS + 3_000, NetworkConfig.reliable(),
                FaultConfig.none(), WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim(), false,
                true, true, s, 0);
    }

    /** Mutation: fsync acknowledges without persisting. */
    public SimConfig withLyingDisks() {
        return copy(faults, workload, true, checkDurability, mutations, storage, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    /** Turns off the Vote durability and Durability checkers, to see what the protocol checkers catch alone. */
    public SimConfig withoutDurabilityCheckers() {
        return copy(faults, workload, lyingDisks, false, mutations, storage, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    public SimConfig withMutation(ProtocolMutation m) {
        Set<ProtocolMutation> next = mutations.isEmpty() ? EnumSet.noneOf(ProtocolMutation.class) : EnumSet.copyOf(mutations);
        next.add(m);
        return copy(faults, workload, lyingDisks, checkDurability, next, storage, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    /** Mutation SKIP_DEDUP (M4-F): state machines execute duplicate requests again. */
    public SimConfig withoutDedup() {
        return copy(faults, workload, lyingDisks, checkDurability, mutations, storage, true, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    public SimConfig withMutation(StorageMutation m) { return withStorage(storage.withMutation(m)); }

    public SimConfig withMaxEntriesPerAppend(int max) {
        return copy(faults, workload, lyingDisks, checkDurability, mutations, storage, skipDedup, max,
                preVote, checkQuorum);
    }

    public SimConfig withWorkload(WorkloadConfig w) {
        return copy(faults, w, lyingDisks, checkDurability, mutations, storage, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    public SimConfig withFaults(FaultConfig f) {
        return copy(f, workload, lyingDisks, checkDurability, mutations, storage, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    public SimConfig withStorage(StorageConfig s) {
        return copy(faults, workload, lyingDisks, checkDurability, mutations, s, skipDedup, maxEntriesPerAppend,
                preVote, checkQuorum);
    }

    /** The M5-A features: both on by default; off reproduces the M1–M4 protocol exactly. */
    public SimConfig withHardening(boolean preVoteOn, boolean checkQuorumOn) {
        return copy(faults, workload, lyingDisks, checkDurability, mutations, storage, skipDedup, maxEntriesPerAppend,
                preVoteOn, checkQuorumOn);
    }

    /** Group commit (M6-A): events wait up to {@code windowMs} in the node's inbox, then one batch, one fsync. */
    public SimConfig withBatching(int windowMs) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend, chaosMs,
                quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, storage, skipDedup, preVote,
                checkQuorum, scenario, windowMs);
    }

    private SimConfig copy(FaultConfig f, WorkloadConfig w, boolean lying, boolean durability,
                           Set<ProtocolMutation> m, StorageConfig s, boolean noDedup, int maxEntries,
                           boolean pv, boolean cq) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntries, chaosMs, quietMs,
                network, f, w, lying, durability, m, s, noDedup, pv, cq, scenario, batchWindowMs);
    }
}
