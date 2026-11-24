package com.projects.raftkv.sim;

/**
 * Simulated clients (DESIGN.md M4-A): {@code clients} closed-loop clients, each with one operation in
 * flight, pausing [minThinkMs, maxThinkMs] between operations, over {@code keys} keys. A client that hears
 * nothing for {@code timeoutMs} retries at another node; one rejected without a leader hint waits
 * {@code backoffMs}. {@code readPercent} of the operations are ReadIndex reads, the rest writes
 * (PUT, DELETE, CAS) with exactly-once sessions.
 */
public record WorkloadConfig(int clients, int minThinkMs, int maxThinkMs, int keys, int timeoutMs, int backoffMs,
                             int readPercent) {
    public WorkloadConfig {
        if (clients < 0 || minThinkMs < 0 || maxThinkMs < minThinkMs || keys < 1 || timeoutMs < 1 || backoffMs < 0
                || readPercent < 0 || readPercent > 100)
            throw new IllegalArgumentException("bad workload config");
    }

    public static WorkloadConfig none() { return new WorkloadConfig(0, 0, 0, 1, 1, 0, 0); }

    public static WorkloadConfig standard() { return new WorkloadConfig(5, 5, 30, 5, 150, 10, 30); }
}
