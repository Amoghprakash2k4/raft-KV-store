package com.projects.raftkv.sim;

/**
 * Seeded network behaviour. Delay is uniform in [minDelayMs, maxDelayMs]; with probability
 * longDelayProbability it is multiplied by a factor in [longDelayMinFactor, longDelayMaxFactor],
 * which delivers very stale messages (from old terms) and exercises stale-term handling.
 */
public record NetworkConfig(int minDelayMs, int maxDelayMs,
                            double longDelayProbability, int longDelayMinFactor, int longDelayMaxFactor,
                            double dropProbability, double duplicateProbability) {
    public NetworkConfig {
        if (minDelayMs < 1 || maxDelayMs < minDelayMs) throw new IllegalArgumentException("bad delay range");
        if (longDelayMinFactor < 1 || longDelayMaxFactor < longDelayMinFactor)
            throw new IllegalArgumentException("bad long-delay factors");
        for (double p : new double[] {longDelayProbability, dropProbability, duplicateProbability})
            if (p < 0 || p > 1) throw new IllegalArgumentException("probability out of range: " + p);
    }

    public static NetworkConfig reliable() { return new NetworkConfig(1, 5, 0, 1, 1, 0, 0); }

    public static NetworkConfig chaotic() { return new NetworkConfig(1, 10, 0.01, 10, 50, 0.05, 0.02); }
}
