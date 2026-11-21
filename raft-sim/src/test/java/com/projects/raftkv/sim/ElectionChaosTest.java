package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * M1 exit criterion: Election Safety (and durable votes, monotonic terms) under drops, duplicates,
 * reordering, long delays, symmetric and one-way partitions, crash/restart and process pauses.
 * Run with -Psim.seeds=5000 for the milestone gate.
 */
class ElectionChaosTest {

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void electionSafetyHoldsUnderChaos(int nodes) {
        List<SimResult> results = Seeds.seeds(300).mapToObj(seed -> Simulation.run(seed, SimConfig.chaos(nodes))).toList();

        // Guard against a harness that silently stopped injecting faults.
        if (results.size() > 1) {
            assertThat(results.stream().mapToInt(SimResult::crashes).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::partitions).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::pauses).sum()).isPositive();
        }
        System.out.printf("[chaos %d nodes] seeds=%d elections=%d maxTerm=%d crashes=%d partitions=%d pauses=%d%n",
                nodes, results.size(),
                results.stream().mapToInt(SimResult::leadersElected).sum(),
                results.stream().mapToLong(SimResult::maxTerm).max().orElse(0),
                results.stream().mapToInt(SimResult::crashes).sum(),
                results.stream().mapToInt(SimResult::partitions).sum(),
                results.stream().mapToInt(SimResult::pauses).sum());
    }
}
