package com.projects.raftkv.sim;

/**
 * Node/link fault schedule: every [minGapMs, maxGapMs] one random fault is injected while chaos lasts.
 * {@code crashLoop} biases the choice towards crashes and crashes during an fsync (DESIGN.md M3-B).
 */
public record FaultConfig(boolean enabled, int minGapMs, int maxGapMs,
                          int minDownMs, int maxDownMs, int minPauseMs, int maxPauseMs, boolean crashLoop) {
    public FaultConfig {
        if (minGapMs < 1 || maxGapMs < minGapMs || minDownMs < 1 || maxDownMs < minDownMs
                || minPauseMs < 1 || maxPauseMs < minPauseMs)
            throw new IllegalArgumentException("bad fault ranges");
    }

    public static FaultConfig none() { return new FaultConfig(false, 1, 1, 1, 1, 1, 1, false); }

    public static FaultConfig chaos() { return new FaultConfig(true, 100, 600, 100, 2000, 100, 800, false); }

    /** Frequent short crashes, most of them in the middle of a write. */
    public static FaultConfig crashLoops() { return new FaultConfig(true, 40, 200, 20, 400, 100, 800, true); }
}
