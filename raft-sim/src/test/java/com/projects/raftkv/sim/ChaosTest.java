package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Milestone gate. Invariants 1-7 (Election Safety, Leader Append-Only, Log Matching, Leader
 * Completeness, State Machine Safety, Durability, Linearizability), replica state agreement, durable
 * votes and liveness, under drops, duplicates, reordering, long delays, symmetric and one-way
 * partitions, crash/restart, crashes in the middle of an fsync and process pauses, with closed-loop
 * clients (retries, sessions, ReadIndex reads) over lossy client links throughout. Every node runs the
 * real WAL on a simulated disk. Run with -Psim.seeds=5000 for the gate.
 * The group-commit variants (M6-A) run the same mixes with events batched through a 2 ms inbox, one
 * fsync per batch, as the real server does.
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

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void invariantsHoldUnderChaosWithGroupCommit(int nodes) {
        check("chaos + group commit", nodes, SimConfig.chaos(nodes).withBatching(2));
    }

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {3, 5})
    void invariantsHoldUnderCrashLoopsWithGroupCommit(int nodes) {
        check("crash loop + group commit", nodes, SimConfig.crashLoop(nodes).withBatching(2));
    }

    private static void check(String name, int nodes, SimConfig cfg) {
        List<SimResult> results = Seeds.seeds(300).mapToObj(seed -> Simulation.run(seed, cfg)).toList();

        // Guard against a harness that silently stopped injecting faults or load.
        if (results.size() > 1) {
            assertThat(results.stream().mapToInt(SimResult::crashes).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::partitions).sum()).isPositive();
            assertThat(results.stream().mapToInt(SimResult::pauses).sum()).isPositive();
            assertThat(results.stream().mapToLong(SimResult::lost).sum()).as("overwritten proposals").isPositive();
            assertThat(results.stream().mapToLong(SimResult::timeouts).sum()).as("client timeouts").isPositive();
            assertThat(results.stream().mapToLong(SimResult::redirects).sum()).as("leader redirects").isPositive();
            assertThat(results.stream().mapToLong(SimResult::duplicatesAnswered).sum()).as("duplicates answered from sessions").isPositive();
            assertThat(results.stream().mapToLong(SimResult::unansweredAtEnd).sum()).as("unknown-outcome operations").isPositive();
            assertThat(results.stream().mapToInt(SimResult::midWriteCrashes).sum()).as("crashes during fsync").isPositive();
            assertThat(results.stream().mapToInt(SimResult::tornTailsCut).sum()).as("torn WAL tails recovered").isPositive();
            assertThat(results.stream().mapToLong(SimResult::snapshotsTaken).sum()).as("snapshots taken").isPositive();
            assertThat(results.stream().mapToLong(SimResult::snapshotsInstalled).sum()).as("snapshots installed").isPositive();
            if (cfg.batchWindowMs() > 0) {
                assertThat(results.stream().mapToLong(SimResult::maxBatch).max().orElse(0)).as("batches of more than one event").isGreaterThan(1);
            }
        }
        assertThat(results).allSatisfy(r -> {
            assertThat(r.writesCompleted()).isPositive();
            assertThat(r.readsCompleted()).isPositive();
        });
        System.out.printf("[%s %d nodes] seeds=%d elections=%d maxTerm=%d crashes=%d (mid-write %d, torn tails cut %d,"
                        + " restarts checked %d) partitions=%d pauses=%d"
                        + " | ops=%d writes=%d reads=%d attempts=%d timeouts=%d redirects=%d retries=%d unanswered=%d"
                        + " lost=%d unknown=%d dedupHits=%d linkDups=%d linSteps(max)=%d maxCommitIndex=%d"
                        + " | snapshots taken=%d installed=%d, longest in-memory log=%d entries"
                        + " | batches=%d avg batch=%.2f max batch=%d%n",
                name, nodes, results.size(),
                results.stream().mapToInt(SimResult::leadersElected).sum(),
                results.stream().mapToLong(SimResult::maxTerm).max().orElse(0),
                results.stream().mapToInt(SimResult::crashes).sum(),
                results.stream().mapToInt(SimResult::midWriteCrashes).sum(),
                results.stream().mapToInt(SimResult::tornTailsCut).sum(),
                results.stream().mapToLong(SimResult::restartsChecked).sum(),
                results.stream().mapToInt(SimResult::partitions).sum(),
                results.stream().mapToInt(SimResult::pauses).sum(),
                results.stream().mapToLong(SimResult::opsStarted).sum(),
                results.stream().mapToLong(SimResult::writesCompleted).sum(),
                results.stream().mapToLong(SimResult::readsCompleted).sum(),
                results.stream().mapToLong(SimResult::attempts).sum(),
                results.stream().mapToLong(SimResult::timeouts).sum(),
                results.stream().mapToLong(SimResult::redirects).sum(),
                results.stream().mapToLong(SimResult::retryReplies).sum(),
                results.stream().mapToLong(SimResult::unansweredAtEnd).sum(),
                results.stream().mapToLong(SimResult::lost).sum(),
                results.stream().mapToLong(SimResult::unknown).sum(),
                results.stream().mapToLong(SimResult::duplicatesAnswered).sum(),
                results.stream().mapToLong(SimResult::linkDuplicates).sum(),
                results.stream().mapToLong(SimResult::linearizabilitySteps).max().orElse(0),
                results.stream().mapToLong(SimResult::maxCommitIndex).max().orElse(0),
                results.stream().mapToLong(SimResult::snapshotsTaken).sum(),
                results.stream().mapToLong(SimResult::snapshotsInstalled).sum(),
                results.stream().mapToLong(SimResult::maxLogEntries).max().orElse(0),
                results.stream().mapToLong(SimResult::batches).sum(),
                (double) results.stream().mapToLong(SimResult::batchedEvents).sum()
                        / Math.max(1, results.stream().mapToLong(SimResult::batches).sum()),
                results.stream().mapToLong(SimResult::maxBatch).max().orElse(0));
    }
}
