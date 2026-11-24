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
}
