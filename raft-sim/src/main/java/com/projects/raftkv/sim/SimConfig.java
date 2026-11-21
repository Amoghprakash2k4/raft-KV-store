package com.projects.raftkv.sim;

/**
 * One simulation run: chaos for chaosMs, then quietMs with link/node faults healed (network loss and
 * delay continue). Times are virtual milliseconds; one core tick = tickMs.
 */
public record SimConfig(int clusterSize, int tickMs, int electionTimeoutTicks, int heartbeatTicks,
                        long chaosMs, long quietMs, NetworkConfig network, FaultConfig faults,
                        boolean lyingDisks, boolean checkVoteDurability) {

    public static SimConfig quiet(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 0, 3_000, NetworkConfig.reliable(), FaultConfig.none(), false, true);
    }

    public static SimConfig chaos(int nodes) {
        return new SimConfig(nodes, 10, 10, 2, 10_000, 3_000, NetworkConfig.chaotic(), FaultConfig.chaos(), false, true);
    }

    /** Mutation: fsync acknowledges without persisting. */
    public SimConfig withLyingDisks() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, chaosMs, quietMs,
                network, faults, true, checkVoteDurability);
    }

    public SimConfig withoutVoteDurabilityCheck() {
        return new SimConfig(clusterSize, tickMs, electionTimeoutTicks, heartbeatTicks, chaosMs, quietMs,
                network, faults, lyingDisks, false);
    }
}
