package com.projects.raftkv.sim;

import com.projects.raftkv.core.ProtocolMutation;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * One simulation run: chaos for chaosMs, then quietMs with link/node faults healed (network loss and
 * delay continue). Times are virtual milliseconds; one core tick = tickMs.
 * {@code mutations} deliberately break protocol rules (mutation tests only, DESIGN.md M2-C).
 */
public record SimConfig(int clusterSize, int tickMs, int electionTimeoutTicks, int heartbeatTicks,
                        int maxEntriesPerAppend, long chaosMs, long quietMs, NetworkConfig network,
                        FaultConfig faults, WorkloadConfig workload, boolean lyingDisks,
                        boolean checkVoteDurability, Set<ProtocolMutation> mutations) {

    public SimConfig {
        Objects.requireNonNull(network);
        Objects.requireNonNull(faults);
        Objects.requireNonNull(workload);
        mutations = Set.copyOf(mutations);
    }

    public static SimConfig quiet(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 0, 3_000, NetworkConfig.reliable(), FaultConfig.none(),
                WorkloadConfig.standard(), false, true, Set.of());
    }

    public static SimConfig chaos(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 64, 10_000, 3_000, NetworkConfig.chaotic(), FaultConfig.chaos(),
                WorkloadConfig.standard(), false, true, Set.of());
    }

    /** Mutation: fsync acknowledges without persisting. */
    public SimConfig withLyingDisks() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, true, checkVoteDurability, mutations);
    }

    public SimConfig withoutVoteDurabilityCheck() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, false, mutations);
    }

    public SimConfig withMutation(ProtocolMutation m) {
        Set<ProtocolMutation> next = mutations.isEmpty() ? EnumSet.noneOf(ProtocolMutation.class) : EnumSet.copyOf(mutations);
        next.add(m);
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkVoteDurability, next);
    }

    public SimConfig withMaxEntriesPerAppend(int max) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, max,
                chaosMs, quietMs, network, faults, workload, lyingDisks, checkVoteDurability, mutations);
    }

    public SimConfig withWorkload(WorkloadConfig w) {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, maxEntriesPerAppend,
                chaosMs, quietMs, network, faults, w, lyingDisks, checkVoteDurability, mutations);
    }
}
