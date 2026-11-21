package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Liveness baseline: on a reliable network with no faults, a leader emerges quickly and stays. */
class QuietClusterElectionTest {

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void electsALeaderQuicklyAndKeepsIt(int nodes) {
        Seeds.seeds(200).forEach(seed -> {
            SimResult r = Simulation.run(seed, SimConfig.quiet(nodes));
            assertThat(r.firstLeaderAtMs()).as("seed %d: first leader time", seed).isBetween(0L, 1000L);
            assertThat(r.lastNewLeaderAtMs()).as("seed %d: leadership changed after settling", seed)
                    .isLessThanOrEqualTo(1000L);
        });
    }
}
