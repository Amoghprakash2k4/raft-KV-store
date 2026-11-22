package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Milestone gate. Invariants 1-5 (Election Safety, Leader Append-Only, Log Matching, Leader
 * Completeness, State Machine Safety), replica state agreement, durable votes and liveness, under
 * drops, duplicates, reordering, long delays, symmetric and one-way partitions, crash/restart and
 * process pauses, with clients proposing commands throughout. Run with -Psim.seeds=5000 for the gate.
 */
class ChaosTest {

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void invariantsHoldUnderChaos(int nodes) {
        List<SimResult> results = Seeds.seeds(300).mapToObj(seed -> Simulation.run(seed, SimConfig.chaos(nodes))).toList();

        // Guard against a harness that silently stopped injecting faults or load.
        if (results.size() > 1) {
            assertThat(results.stream().mapToInt(SimResult::crashes).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::partitions).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::pauses).sum()).isPositive();
            assertThat(results.stream().mapToLong(SimResult::lost).sum()).as("overwritten proposals").isPositive();
        }
        assertThat(results).allSatisfy(r -> assertThat(r.committed()).isPositive());
        System.out.printf("[chaos %d nodes] seeds=%d elections=%d maxTerm=%d crashes=%d partitions=%d pauses=%d"
                        + " | proposals=%d committed=%d lost=%d rejected=%d maxCommitIndex=%d%n",
                nodes, results.size(),
                results.stream().mapToInt(SimResult::leadersElected).sum(),
                results.stream().mapToLong(SimResult::maxTerm).max().orElse(0),
                results.stream().mapToInt(SimResult::crashes).sum(),
                results.stream().mapToInt(SimResult::partitions).sum(),
                results.stream().mapToInt(SimResult::pauses).sum(),
                results.stream().mapToLong(SimResult::proposals).sum(),
                results.stream().mapToLong(SimResult::committed).sum(),
                results.stream().mapToLong(SimResult::lost).sum(),
                results.stream().mapToLong(SimResult::rejected).sum(),
                results.stream().mapToLong(SimResult::maxCommitIndex).max().orElse(0));
    }
}
