package com.projects.raftkv.sim;

/** A failed run, carrying everything needed to replay it. */
public final class SimulationFailure extends AssertionError {
    public SimulationFailure(long seed, SimConfig config, String trace, Throwable cause) {
        super("Simulation failed for seed=" + seed + ": " + cause.getMessage()
                + "\n  replay: ./gradlew :raft-sim:test -Psim.seed=" + seed
                + "\n  config: " + config
                + "\n  last events:\n" + trace, cause);
    }
}
