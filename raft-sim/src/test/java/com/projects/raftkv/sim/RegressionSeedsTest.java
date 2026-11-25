package com.projects.raftkv.sim;

import org.junit.jupiter.api.Test;

/**
 * Seeds that once found a bug, replayed on every build (PROJECT_BRIEF: a regression test for every bug,
 * keyed by seed). Each also has a focused unit test; this one keeps the end-to-end scenario.
 */
class RegressionSeedsTest {

    /**
     * M4 gate, chaos 5 nodes: a follower in term 10 rejected a long-delayed AppendEntries from the leader's
     * old term 6 but echoed its read round with term 10, so the deposed term-10 leader counted it and
     * served a stale read. Fixed: a rejection of an older term's message echoes no round
     * (ReadIndexTest.aRejectionOfAnOlderTermEchoesNoRound).
     */
    @Test
    void seed4811StaleTermReadRoundEcho() {
        Simulation.run(4811, SimConfig.chaos(5));
    }

    /**
     * M5 gate, chaos 5 nodes: not a protocol bug. One client lost five round trips in a row to the stable
     * leader on its 5%-lossy link and completed nothing in the last 1,089 ms, failing a 1 s client-liveness
     * window that measurements showed was too tight (DEVIATIONS.md H6).
     */
    @Test
    void seed3064UnluckyClientLink() {
        SimResult r = Simulation.run(3064, SimConfig.chaos(5));
        org.assertj.core.api.Assertions.assertThat(r.slowestClientIdleMs()).isBetween(1_000L, 2_000L);
    }
}
