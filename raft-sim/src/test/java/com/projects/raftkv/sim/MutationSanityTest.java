package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/**
 * Break a rule on purpose and check the simulator notices. The break: disks that acknowledge fsync
 * without persisting, so votes and terms are lost on crash (a node can vote twice in one term).
 */
class MutationSanityTest {

    @Test
    void lostVotesAreCaughtByVoteDurabilityChecker() {
        List<String> failures = failuresOver(LongStream.rangeClosed(1, 50), SimConfig.chaos(3).withLyingDisks());
        assertThat(failures).isNotEmpty();
        assertThat(failures).allMatch(m -> m.contains("Vote durability") || m.contains("Election Safety"));
    }

    /** Without the vote checker, the consequence itself, two leaders in one term, must be caught. */
    @Test
    void lostVotesEventuallyCauseTwoLeadersInOneTerm() {
        SimConfig cfg = SimConfig.chaos(3).withLyingDisks().withoutVoteDurabilityCheck();
        List<String> failures = failuresOver(LongStream.rangeClosed(1, 500), cfg);
        System.out.printf("[mutation lying-disk] two-leader violations in %d/500 seeds%n", failures.size());
        assertThat(failures).isNotEmpty();
        assertThat(failures).allMatch(m -> m.contains("Election Safety"));
    }

    private static List<String> failuresOver(LongStream seeds, SimConfig cfg) {
        return seeds.mapToObj(seed -> {
            try {
                Simulation.run(seed, cfg);
                return null;
            } catch (SimulationFailure f) {
                return f.getMessage();
            }
        }).filter(Objects::nonNull).toList();
    }
}
