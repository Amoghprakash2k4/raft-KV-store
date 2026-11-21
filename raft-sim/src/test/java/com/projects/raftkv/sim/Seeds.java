package com.projects.raftkv.sim;

import java.util.stream.LongStream;

/** Seeds for simulation tests: -Dsim.seed=N replays one seed, -Dsim.seeds=N sets the count. */
final class Seeds {
    private Seeds() {}

    static LongStream seeds(int defaultCount) {
        String one = System.getProperty("sim.seed");
        if (one != null) return LongStream.of(Long.parseLong(one));
        return LongStream.rangeClosed(1, Integer.getInteger("sim.seeds", defaultCount));
    }
}
