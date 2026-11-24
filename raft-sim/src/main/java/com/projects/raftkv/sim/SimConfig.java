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
 */
public record SimConfig(int clusterSize, int tickMs, int electionTimeoutTicks, int heartbeatTicks,
                        int maxEntriesPerAppend, long chaosMs, long quietMs, NetworkConfig network,
                        FaultConfig faults, WorkloadConfig workload, boolean lyingDisks,
                        boolean checkDurability, Set<ProtocolMutation> mutations, StorageConfig storage) {

    public SimConfig {
        Objects.requireNonNull(network);
        Objects.requireNonNull(faults);
        Objects.requireNonNull(workload);
        Objects.requireNonNull(storage);
        mutations = Set.copyOf(mutations);
    }

    public static SimConfig quiet(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 0, 3_000, NetworkConfig.reliable(), FaultConfig.none(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim());
    }

    public static SimConfig chaos(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 10_000, 3_000, NetworkConfig.chaotic(), FaultConfig.chaos(),
                WorkloadConfig.standard(), false, true, Set.of(), StorageConfig.sim());
    }

    /** The chaos mix with a crash loop: frequent crashes, most of them in the middle of an fsync. */
    public static SimConfig crashLoop(int nodes) {
        return chaos(nodes).withFaults(FaultConfig.crashLoops());
    }

    /** Mutation: fsync acknowledges without persisting. */
    public SimConfig withLyingDisks() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, true, checkDurability, mutations, storage);
    }

    /** Turns off the Vote durability and Durability checkers, to see what the protocol checkers catch alone. */
    public SimConfig withoutDurabilityCheckers() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, false, mutations, storage);
    }

    public SimConfig withMutation(ProtocolMutation m) {
        Set<ProtocolMutation> next = mutations.isEmpty() ? EnumSet.noneOf(ProtocolMutation.class) : EnumSet.copyOf(mutations);
        next.add(m);
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, next, storage);
    }

    public SimConfig withMutation(StorageMutation m) { return withStorage(storage.withMutation(m)); }

    public SimConfig withMaxEntriesPerAppend(int max) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, max,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, storage);
    }

    public SimConfig withWorkload(WorkloadConfig w) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, w, lyingDisks, checkDurability, mutations, storage);
    }

    public SimConfig withFaults(FaultConfig f) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, f, workload, lyingDisks, checkDurability, mutations, storage);
    }

    public SimConfig withStorage(StorageConfig s) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkDurability, mutations, s);
    }
}
