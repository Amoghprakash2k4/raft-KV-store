package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.storage.StorageMutation;
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
            "Log Matching", "Leader Completeness", "State Machine Safety", "Replica state divergence", "Durability",
            "Linearizability"};

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

    // ---------------------------------------------------------------- M1/M3: lying disks (lost votes and logs)

    /** Invariant 6: a disk whose fsync lies loses synced state, which the restart check sees directly. */
    @Test
    void lostDurableStateIsCaughtByTheDurabilityChecker() {
        Tally t = run("lying-disk", SimConfig.chaos(3).withLyingDisks(), 50);
        assertThat(t.count("Durability")).isEqualTo(50);
    }

    /** Without the durability checkers, the consequences themselves (two leaders, lost committed entries) are caught. */
    @Test
    void lostDurableStateIsCaughtByTheProtocolCheckersToo() {
        Tally t = run("lying-disk, no durability checkers", SimConfig.chaos(3).withLyingDisks().withoutDurabilityCheckers(), 300);
        assertThat(t.count("Election Safety")).isPositive();
        assertThat(t.count("Leader Completeness")).isPositive();
        assertThat(t.caughtBy().keySet()).noneMatch(k -> k.startsWith("unknown"));
    }

    // ---------------------------------------------------------------- M2 protocol mutations

    /** §5.4.1: voting for candidates with stale logs lets a leader without committed entries win. */
    /**
     * §5.4.1 without the up-to-date check. Measured on the plain protocol (PreVote off): with PreVote on,
     * the pre-vote's own log check stops a stale candidate before it ever sends RequestVote, so the
     * mutation is masked (see the next test).
     */
    @Test
    void skippingTheUpToDateCheckIsCaught() {
        Tally t = run("skip up-to-date check", SimConfig.chaos(3).withHardening(false, false)
                .withMutation(ProtocolMutation.SKIP_UP_TO_DATE_CHECK), 100);
        assertThat(t.count("Leader Completeness")).isPositive();
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(75);
    }

    /**
     * M5: PreVote checks the log too, so it hides a missing check in RequestVote. Skip both checks and
     * the bug is back: the pre-vote is a second gate, not a replacement for §5.4.1.
     */
    @Test
    void preVoteMasksTheMissingUpToDateCheckUntilItSkipsItToo() {
        Tally masked = run("skip up-to-date check (preVote on)",
                SimConfig.chaos(3).withMutation(ProtocolMutation.SKIP_UP_TO_DATE_CHECK), 100);
        assertThat(masked.safetyCatches()).isZero();
        Tally both = run("skip up-to-date check in RequestVote and PreVote",
                SimConfig.chaos(3).withMutation(ProtocolMutation.SKIP_UP_TO_DATE_CHECK)
                        .withMutation(ProtocolMutation.PREVOTE_SKIPS_LOG_CHECK), 100);
        assertThat(both.count("Leader Completeness")).isPositive();
        assertThat(both.safetyCatches()).isGreaterThanOrEqualTo(40);   // measured 56/100
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
        SimConfig cfg = SimConfig.chaos(3).withHardening(false, false)
                .withMutation(ProtocolMutation.COMMIT_OLD_TERM_BY_COUNT).withMaxEntriesPerAppend(1);
        Tally t = run("commit old term by count (batch 1)", cfg, 1000);
        assertThat(t.count("Leader Completeness") + t.count("State Machine Safety")).isPositive();
    }

    /** §5.3: truncating on a match lets a reordered old AppendEntries delete newer, committed entries. */
    @Test
    void truncatingOnMatchIsCaught() {
        Tally t = run("truncate on match", SimConfig.chaos(3).withMutation(ProtocolMutation.TRUNCATE_ON_MATCH), 100);
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(80);
    }

    // ---------------------------------------------------------------- M3 durability mutations (DESIGN.md M3-F)

    /** Recovery that keeps a torn tail: later appends land after garbage, and the next recovery loses them. */
    @Test
    void keepingATornTailIsCaught() {
        Tally t = run("skip torn-tail cut (crash loop)", SimConfig.crashLoop(3).withMutation(StorageMutation.SKIP_TORN_TAIL_CUT), 100);
        assertThat(t.count("Durability")).isGreaterThanOrEqualTo(90);     // measured 100/100
    }

    /**
     * A snapshot renamed into place before its bytes are durable: after a crash the file is empty, torn
     * or (by luck) complete. Recovery finds the corrupt file and the node cannot restart. Caught only in
     * runs where a node crashes after a snapshot and its unsynced bytes do not all survive.
     */
    @Test
    void renamingAnUnsyncedSnapshotIsCaught() {
        Tally t = run("skip snapshot data fsync (crash loop)", SimConfig.crashLoop(3).withMutation(StorageMutation.SKIP_SNAPSHOT_DATA_FSYNC), 100);
        assertThat(t.count("Durability")).isGreaterThanOrEqualTo(45);     // measured 59/100
    }

    /**
     * Figure 13 / D1a rule 5: installing a stale snapshot discards committed entries from the follower's
     * durable log and rolls its state machine back. The follower then re-replicates and converges, so
     * only the direct checks see it: Durability (c), no committed entry deleted, and no rollback.
     */
    @Test
    void installingAStaleSnapshotIsCaught() {
        Tally t = run("naive install snapshot", SimConfig.chaos(3).withMutation(ProtocolMutation.NAIVE_INSTALL_SNAPSHOT), 100);
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(90);         // measured 99/100
    }

    // ---------------------------------------------------------------- M4 client mutations (DESIGN.md M4-F)

    /**
     * Thesis §6.3: without dedup, a retried or duplicated write runs again. Invisible to the replica
     * checkers (every replica re-runs it identically); only what clients observe shows it.
     */
    @Test
    void skippingDedupIsCaughtByLinearizability() {
        Tally t = run("skip dedup", SimConfig.chaos(3).withoutDedup(), 100);
        assertThat(t.count("Linearizability")).isGreaterThanOrEqualTo(95);   // measured 100/100
    }

    /**
     * The fault mix aimed at the leader (DESIGN.md M4-F): most faults isolate, pause or crash the current
     * leader. A bad ReadIndex is stale only in the short window around a leadership change, so this puts
     * many more reads in that window (as batch size 1 does for Figure 8).
     */
    private static SimConfig leaderHunting(int readPercent, int keys) {
        return SimConfig.chaos(3).withFaults(FaultConfig.leaderHunting())
                .withWorkload(new WorkloadConfig(5, 5, 30, keys, 150, 10, readPercent));
    }

    /**
     * Thesis §6.4 step 3: a deposed leader that never confirms its leadership serves stale reads.
     * Measured with CheckQuorum off, as in M4: CheckQuorum makes a cut-off leader step down within about
     * two election timeouts, which shrinks the window (next test) but does not close it.
     */
    @Test
    void readingWithoutAQuorumCheckIsCaughtByLinearizability() {
        Tally t = run("read without quorum check (leader faults, 80% reads)",
                leaderHunting(80, 2).withHardening(true, false).withMutation(ProtocolMutation.READ_WITHOUT_QUORUM_CHECK), 100);
        assertThat(t.count("Linearizability")).isGreaterThanOrEqualTo(60);   // measured 75/100
    }

    /**
     * M5: CheckQuorum is not a substitute for the ReadIndex round. A leader that was paused wakes up still
     * believing it leads and answers before its next quorum check, so stale reads still get through.
     */
    @Test
    void checkQuorumNarrowsButDoesNotCloseTheStaleReadWindow() {
        Tally t = run("read without quorum check (leader faults, 80% reads, checkQuorum on)",
                leaderHunting(80, 2).withMutation(ProtocolMutation.READ_WITHOUT_QUORUM_CHECK), 100);
        assertThat(t.count("Linearizability")).isPositive();
    }

    /**
     * Thesis §6.4 step 1: a new leader reading before its no-op commits may miss writes the old leader
     * committed and acknowledged. Needs a write acknowledged in the last heartbeat before a leader
     * change and a read reaching the new leader within one round trip of its election: a narrow window.
     */
    @Test
    void readingBeforeTheCurrentTermCommitIsCaughtByLinearizability() {
        Tally t = run("read before current-term commit (leader faults, 50% reads, 1 key)",
                leaderHunting(50, 1).withMutation(ProtocolMutation.READ_BEFORE_CURRENT_TERM_COMMIT), 200);
        assertThat(t.count("Linearizability")).isGreaterThanOrEqualTo(30);   // measured 40/200
    }

    /** Control: the same runs without a mutation are clean, so the catches above are caused by the mutations. */
    /**
     * Teaching control (M5): PreVote without the up-to-date check grants pre-votes it should refuse, yet
     * nothing unsafe can follow, because the real RequestVote still checks the log (§5.4.1). PreVote is a
     * liveness feature only, so this "mutation" must cause no safety violation.
     */
    @Test
    void preVoteWithoutTheLogCheckIsStillSafe() {
        Tally t = run("prevote skips log check (control)", SimConfig.chaos(3).withMutation(ProtocolMutation.PREVOTE_SKIPS_LOG_CHECK), 300);
        assertThat(t.safetyCatches()).isZero();
    }

    @Test
    void unmutatedControlRunsAreClean() {
        Tally t = run("control (batch 1)", SimConfig.chaos(3).withMaxEntriesPerAppend(1), 100);
        assertThat(t.safetyCatches()).isZero();
        Tally loop = run("control (crash loop)", SimConfig.crashLoop(3), 100);
        assertThat(loop.safetyCatches()).isZero();
        Tally reads = run("control (leader faults, 80% reads)", leaderHunting(80, 2), 100);
        assertThat(reads.safetyCatches()).isZero();
        Tally mixed = run("control (leader faults, 50% reads, 1 key)", leaderHunting(50, 1), 200);
        assertThat(mixed.safetyCatches()).isZero();
    }

    // ---------------------------------------------------------------- M6: group commit

    /**
     * E1 under group commit: a batch's messages must not leave before the batch's fsync. With
     * SEND_BEFORE_SYNC, a node that crashes inside that fsync has already granted votes and acknowledged
     * entries it no longer has after restart.
     */
    @Test
    void sendingBeforeTheBatchFsyncIsCaught() {
        Tally t = run("send before sync (crash loop, group commit)",
                SimConfig.crashLoop(3).withBatching(2).withMutation(ProtocolMutation.SEND_BEFORE_SYNC), 100);
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(60);   // measured 78 (Durability 74, Election Safety 4)
    }

    @Test
    void sendingBeforeTheBatchFsyncIsCaughtByItsConsequencesAlone() {
        Tally t = run("send before sync (crash loop, group commit, no durability checkers)",
                SimConfig.crashLoop(3).withBatching(2).withMutation(ProtocolMutation.SEND_BEFORE_SYNC)
                        .withoutDurabilityCheckers(), 300);
        assertThat(t.safetyCatches()).isGreaterThanOrEqualTo(50);   // measured 97 (Leader Completeness 75, Election Safety 22)
    }

    @Test
    void groupCommitControl() {
        Tally t = run("control (crash loop, group commit)", SimConfig.crashLoop(3).withBatching(2), 100);
        assertThat(t.count("passed")).isEqualTo(100);
    }
}
