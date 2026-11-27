package com.projects.raftkv.sim;

/**
 * A failed run, carrying everything needed to replay it, plus the run's statistics up to the failure
 * ({@link #partial()}, null when they could not be collected) and the simulated time it failed at.
 */
public final class SimulationFailure extends AssertionError {
    private static final long serialVersionUID = 1L;
    private final transient SimResult partial;
    private final long failedAtMs;

    public SimulationFailure(long seed, SimConfig config, String trace, Throwable cause) {
        this(seed, config, trace, cause, null, -1);
    }

    public SimulationFailure(long seed, SimConfig config, String trace, Throwable cause, SimResult partial, long failedAtMs) {
        super("Simulation failed for seed=" + seed + ": " + cause.getMessage()
                + "\n  replay: ./gradlew :raft-sim:test -Psim.seed=" + seed
                + "\n  config: " + config
                + "\n  last events:\n" + trace, cause);
        this.partial = partial;
        this.failedAtMs = failedAtMs;
    }

    /** Counters (elections, crashes, client operations, ...) up to the failure; null if unavailable. */
    public SimResult partial() { return partial; }

    /** Simulated time of the failure in ms, or -1. */
    public long failedAtMs() { return failedAtMs; }
}
