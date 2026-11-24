package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Milestone gate. Invariants 1-6 (Election Safety, Leader Append-Only, Log Matching, Leader
 * Completeness, State Machine Safety, Durability), replica state agreement, durable votes and liveness,
 * under drops, duplicates, reordering, long delays, symmetric and one-way partitions, crash/restart,
 * crashes in the middle of an fsync and process pauses, with clients proposing commands throughout.
 * Every node runs the real WAL on a simulated disk. Run with -Psim.seeds=5000 for the gate.
 */
class ChaosTest {

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void invariantsHoldUnderChaos(int nodes) {
        check("chaos", nodes, SimConfig.chaos(nodes));
    }

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void invariantsHoldUnderCrashLoops(int nodes) {
        check("crash loop", nodes, SimConfig.crashLoop(nodes));
    }

    private static void check(String name, int nodes, SimConfig cfg) {
        List<SimResult> results = Seeds.seeds(300).mapToObj(seed -> Simulation.run(seed, cfg)).toList();

        // Guard against a harness that silently stopped injecting faults or load.
        if (results.size() > 1) {
            assertThat(results.stream().mapToInt(SimResult::crashes).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::partitions).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::pauses).sum()).isPositive();
            assertThat(results.stream().mapToLong(SimResult::lost).sum()).as("overwritten proposals").isPositive();
            assertThat(results.stream().mapToInt(SimResult::midWriteCrashes).sum()).as("crashes during fsync").isPositive();
            assertThat(results.stream().mapToInt(SimResult::tornTailsCut).sum()).as("torn WAL tails recovered").isPositive();
            assertThat(results.stream().mapToLong(SimResult::snapshotsTaken).sum()).as("snapshots taken").isPositive();
            assertThat(results.stream().mapToLong(SimResult::snapshotsInstalled).sum()).as("snapshots installed").isPositive();
        }
        assertThat(results).allSatisfy(r -> assertThat(r.committed()).isPositive());
        System.out.printf("[%s %d nodes] seeds=%d elections=%d maxTerm=%d crashes=%d (mid-write %d, torn tails cut %d,"
                        + " restarts checked %d) partitions=%d pauses=%d"
                        + " | proposals=%d committed=%d lost=%d rejected=%d unknown=%d maxCommitIndex=%d"
                        + " | snapshots taken=%d installed=%d, longest in-memory log=%d entries%n",
                name, nodes, results.size(),
                results.stream().mapToInt(SimResult::leadersElected).sum(),
                results.stream().mapToLong(SimResult::maxTerm).max().orElse(0),
                results.stream().mapToInt(SimResult::crashes).sum(),
                results.stream().mapToInt(SimResult::midWriteCrashes).sum(),
                results.stream().mapToInt(SimResult::tornTailsCut).sum(),
                results.stream().mapToLong(SimResult::restartsChecked).sum(),
                results.stream().mapToInt(SimResult::partitions).sum(),
                results.stream().mapToInt(SimResult::pauses).sum(),
                results.stream().mapToLong(SimResult::proposals).sum(),
                results.stream().mapToLong(SimResult::committed).sum(),
                results.stream().mapToLong(SimResult::lost).sum(),
                results.stream().mapToLong(SimResult::rejected).sum(),
                results.stream().mapToLong(SimResult::unknown).sum(),
                results.stream().mapToLong(SimResult::maxCommitIndex).max().orElse(0),
                results.stream().mapToLong(SimResult::snapshotsTaken).sum(),
                results.stream().mapToLong(SimResult::snapshotsInstalled).sum(),
                results.stream().mapToLong(SimResult::maxLogEntries).max().orElse(0));
    }
}
