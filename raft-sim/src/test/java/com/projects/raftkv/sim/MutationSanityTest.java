package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.ProtocolMutation;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/**
 * Break one rule on purpose and check that the simulator notices (DESIGN.md M2-C). A checker that never
 * fires might be checking nothing; these tests prove each one can see the bug it exists for.
 * Each test prints which checker caught the bug in how many seeds.
 */
class MutationSanityTest {

    private static final String[] SAFETY = {"Election Safety", "Vote durability", "Leader Append-Only",
            "Log Matching", "Leader Completeness", "State Machine Safety", "Replica state divergence"};

    /** Failures of one mutated configuration, grouped by what caught them. */
    private record Tally(String name, int seeds, Map<String, Integer> caughtBy) {
        int count(String key) { return caughtBy.getOrDefault(key, 0); }

        int safetyCatches() {
            return caughtBy.entrySet().stream().filter(e -> !e.getKey().equals("passed") && !e.getKey().startsWith("Liveness"))
                    .mapToInt(Map.Entry::getValue).sum();
        }

        void print() { System.out.printf("[mutation %s] %d seeds: %s%n", name, seeds, caughtBy); }
    }

    private static Tally run(String name, SimConfig cfg, int seeds) {
        Map<String, Integer> by = new TreeMap<>();
        LongStream.rangeClosed(1, seeds).forEach(seed -> {
            try {
                Simulation.run(seed, cfg);
                by.merge("passed", 1, Integer::sum);
            } catch (SimulationFailure f) {
                by.merge(category(f.getCause()), 1, Integer::sum);
            }
        });
        Tally t = new Tally(name, seeds, by);
        t.print();
        return t;
    }

    /** Checker name for InvariantViolations; "assertion: <type>" for a guard inside production code. */
    private static String category(Throwable cause) {
        String msg = String.valueOf(cause.getMessage());
        if (cause instanceof InvariantViolation) {
            if (msg.startsWith("Liveness")) return "Liveness";
            for (String s : SAFETY) if (msg.startsWith(s)) return s;
            return "unknown checker: " + msg;
        }
        return "assertion: " + cause.getClass().getSimpleName() + " " + msg.split(":")[0];
    }

    // ---------------------------------------------------------------- M1: lying disks (lost votes and logs)

    @Test
    void lostVotesAreCaughtByVoteDurabilityChecker() {
        Tally t = run("lying-disk", SimConfig.chaos(3).withLyingDisks(), 50);
        assertThat(t.count("Vote durability")).isEqualTo(50);
    }

    /** Without the vote checker, the consequences themselves (two leaders, lost committed entries) are caught. */
    @Test
    void lostDurableStateIsCaughtByTheProtocolCheckersToo() {
        Tally t = run("lying-disk, no vote checker", SimConfig.chaos(3).withLyingDisks().withoutVoteDurabilityCheck(), 300);
        assertThat(t.count("Election Safety")).isPositive();
        assertThat(t.count("Leader Completeness")).isPositive();
        assertThat(t.caughtBy().keySet()).noneMatch(k -> k.startsWith("unknown"));
    }

    // ---------------------------------------------------------------- M2 protocol mutations

    /** §5.4.1: voting for candidates with stale logs lets a leader without committed entries win. */
    @Test
    void skippingTheUpToDateCheckIsCaught() {
        Tally t = run("skip up-to-date check", SimConfig.chaos(3).withMutation(ProtocolMutation.SKIP_UP_TO_DATE_CHECK), 100);
        assertThat(t.count("Leader Completeness")).isPositive();
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(90);
    }

    /**
     * Figure 8: committing an old-term entry by counting replicas. The window is narrow, because the new
     * leader's no-op usually rides in the same AppendEntries as the old entries and commits them
     * legitimately. With one entry per AppendEntries the old entry can reach a majority alone. One-entry
     * batches also make catch-up slow, so some runs end in Liveness failures that have nothing to do
     * with the mutation; only safety catches count.
     */
    @Test
    void committingAnOldTermEntryByCountingIsCaught() {
        SimConfig cfg = SimConfig.chaos(3).withMutation(ProtocolMutation.COMMIT_OLD_TERM_BY_COUNT).withMaxEntriesPerAppend(1);
        Tally t = run("commit old term by count (batch 1)", cfg, 1000);
        assertThat(t.count("Leader Completeness") + t.count("State Machine Safety")).isPositive();
    }

    /** §5.3: truncating on a match lets a reordered old AppendEntries delete newer, committed entries. */
    @Test
    void truncatingOnMatchIsCaught() {
        Tally t = run("truncate on match", SimConfig.chaos(3).withMutation(ProtocolMutation.TRUNCATE_ON_MATCH), 100);
        assertThat(t.count("Leader Completeness") + t.count("Replica state divergence")
                + t.count("State Machine Safety")).isGreaterThanOrEqualTo(80);
    }

    /** Control: the same runs without a mutation are clean, so the catches above are caused by the mutations. */
    @Test
    void unmutatedControlRunsAreClean() {
        Tally t = run("control (batch 1)", SimConfig.chaos(3).withMaxEntriesPerAppend(1), 100);
        assertThat(t.safetyCatches()).isZero();
    }
}
