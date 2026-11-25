package com.projects.raftkv.bench;

/**
 * One throughput/latency run (DESIGN.md M6-B).
 *
 * @param clients      closed-loop clients: each waits for its answer before sending the next request
 * @param writePercent 100 = writes only; the rest are linearizable reads (ReadIndex)
 * @param keys         keys the clients pick from uniformly
 * @param valueBytes   size of each written value
 */
public record LoadSpec(int nodes, boolean fsyncBatching, int clients, int writePercent, int keys, int valueBytes,
                       int warmupSec, int durationSec) {
    public LoadSpec {
        if (nodes != 3 && nodes != 5) throw new IllegalArgumentException("nodes must be 3 or 5");
        if (clients < 1 || clients > 512) throw new IllegalArgumentException("clients must be 1..512");
        if (writePercent < 0 || writePercent > 100) throw new IllegalArgumentException("writePercent must be 0..100");
        if (keys < 1 || valueBytes < 1 || valueBytes > 65_536) throw new IllegalArgumentException("bad keys/valueBytes");
        if (warmupSec < 0 || durationSec < 1 || durationSec > 600) throw new IllegalArgumentException("bad duration");
    }

    public static LoadSpec defaults() {
        return new LoadSpec(3, true, 16, 100, 1_000, 100, 3, 15);
    }

    public String label() {
        return nodes + " nodes, " + clients + " clients, " + (writePercent == 100 ? "writes only" : writePercent == 0 ? "reads only" : writePercent + "% writes")
                + ", batching " + (fsyncBatching ? "on" : "off");
    }
}
