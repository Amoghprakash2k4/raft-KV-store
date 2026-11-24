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
 */
public record SimConfig(int clusterSize, int tickMs, int electionTimeoutTicks, int heartbeatTicks,
                        int maxEntriesPerAppend, long chaosMs, long quietMs, NetworkConfig network,
                        FaultConfig faults, WorkloadConfig workload, boolean lyingDisks,
                        boolean checkDurability, Set<ProtocolMutation> mutations, StorageConfig storage,
                        boolean skipDedup) {

    public SimConfig {
        Objects.requireNonNull(network);
        Objects.requireNonNull(faults);
        Objects.requireNonNull(workload);
        Objects.requireNonNull(storage);
        mutations = Set.copyOf(mutations);
    }

    public static SimConfig quiet(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 0, 3_000, NetworkConfig.reliable(), FaultConfig.none(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim(), false);
    }

    public static SimConfig chaos(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 10_000, 3_000, NetworkConfig.chaotic(), FaultConfig.chaos(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim(), false);
    }

    /** The chaos mix with a crash loop: frequent crashes, most of them in the middle of an fsync. */
    public static SimConfig crashLoop(int nodes) {
        return chaos(nodes).withFaults(FaultConfig.crashLoops());
    }

    /** Mutation: fsync acknowledges without persisting. */
    public SimConfig withLyingDisks() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, true, checkDurability, mutations, storage, skipDedup);
    }

    /** Turns off the Vote durability and Durability checkers, to see what the protocol checkers catch alone. */
    public SimConfig withoutDurabilityCheckers() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, false, mutations, storage, skipDedup);
    }

    public SimConfig withMutation(ProtocolMutation m) {
        Set<ProtocolMutation> next = mutations.isEmpty() ? EnumSet.noneOf(ProtocolMutation.class) : EnumSet.copyOf(mutations);
        next.add(m);
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, next, storage, skipDedup);
    }

    /** Mutation SKIP_DEDUP (M4-F): state machines execute duplicate requests again. */
    public SimConfig withoutDedup() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, storage, true);
    }

    public SimConfig withMutation(StorageMutation m) { return withStorage(storage.withMutation(m)); }

    public SimConfig withMaxEntriesPerAppend(int max) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, max,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, storage, skipDedup);
    }

    public SimConfig withWorkload(WorkloadConfig w) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, w, lyingDisks, checkDurability, mutations, storage, skipDedup);
    }

    public SimConfig withFaults(FaultConfig f) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, f, workload, lyingDisks, checkDurability, mutations, storage, skipDedup);
    }

    public SimConfig withStorage(StorageConfig s) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, s, skipDedup);
    }
}
