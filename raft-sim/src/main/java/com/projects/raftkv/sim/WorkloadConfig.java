package com.projects.raftkv.sim;

/**
 * Simulated clients (DESIGN.md M2-D): each sends one random command every [minIntervalMs,
 * maxIntervalMs] over a key space of {@code keys} keys, open loop (it never waits and never retries).
 */
public record WorkloadConfig(int clients, int minIntervalMs, int maxIntervalMs, int keys) {
    public WorkloadConfig {
        if (clients < 0 || minIntervalMs < 1 || maxIntervalMs < minIntervalMs || keys < 1)
            throw new IllegalArgumentException("bad workload config");
    }

    public static WorkloadConfig none() { return new WorkloadConfig(0, 1, 1, 1); }

    public static WorkloadConfig standard() { return new WorkloadConfig(3, 20, 100, 5); }
}
