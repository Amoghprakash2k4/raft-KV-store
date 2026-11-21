package com.projects.raftkv.sim;

/** Summary of a successful run. Two runs with the same seed and config must be equal (incl. traceHash). */
public record SimResult(long seed, long eventsProcessed, long traceHash,
                        int leadersElected, long maxTerm, long firstLeaderAtMs, long lastNewLeaderAtMs,
                        int crashes, int partitions, int pauses) {}
