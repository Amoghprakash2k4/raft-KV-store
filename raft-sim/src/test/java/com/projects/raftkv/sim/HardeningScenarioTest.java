package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.sim.SimConfig.Scenario;
import java.util.List;
import java.util.function.Function;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * PreVote and CheckQuorum measured against the same scripted faults with the features on and off
 * (DESIGN.md M5-A). A healthy cluster with clients running; at 1 s one node is cut off from the others for
 * 3 s. These are liveness properties, so they are measured as rates; every run must still pass every
 * safety checker. Election timeout = 100-200 ms (10 ticks of 10 ms).
 */
class HardeningScenarioTest {

    private static final int SEEDS = Integer.getInteger("sim.scenarioSeeds", 100);

    private static List<SimResult> runAll(SimConfig cfg) {
        return LongStream.rangeClosed(1, SEEDS).mapToObj(seed -> Simulation.run(seed, cfg)).toList();
    }

    private static long count(List<SimResult> rs, Function<SimResult, Boolean> p) {
        return rs.stream().filter(p::apply).count();
    }

    private static void print(String what, List<SimResult> rs) {
        long disrupted = count(rs, r -> r.newLeadersSinceScenario() > 0);
        long maxGrowth = rs.stream().mapToLong(r -> r.maxTerm() - r.termAtScenarioStart()).max().orElse(0);
        long[] sd = rs.stream().mapToLong(SimResult::scenarioStepDownMs).sorted().toArray();
        System.out.printf("[scenario %s] %d seeds: runs with a new leader=%d, max term growth=%d, step-down ms min/p50/max=%d/%d/%d%n",
                what, rs.size(), disrupted, maxGrowth, sd[0], sd[sd.length / 2], sd[sd.length - 1]);
    }

    /** PreVote: a follower cut off for 3 s rejoins without deposing anyone or raising the term. */
    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void preVoteStopsAnIsolatedFollowerFromDisruptingTheLeader(int nodes) {
        List<SimResult> rs = runAll(SimConfig.scenario(nodes, Scenario.ISOLATE_FOLLOWER));
        print("isolate follower, " + nodes + " nodes, preVote on", rs);
        assertThat(rs).allSatisfy(r -> {
            assertThat(r.newLeadersSinceScenario()).isZero();
            assertThat(r.maxTerm()).isEqualTo(r.termAtScenarioStart());
        });
    }

    /** The control: without PreVote the same fault deposes the leader (measured 300 of 300 seeds). */
    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void withoutPreVoteTheIsolatedFollowerDisruptsTheLeader(int nodes) {
        List<SimResult> rs = runAll(SimConfig.scenario(nodes, Scenario.ISOLATE_FOLLOWER).withHardening(false, true));
        print("isolate follower, " + nodes + " nodes, preVote off", rs);
        assertThat(count(rs, r -> r.newLeadersSinceScenario() > 0)).isGreaterThanOrEqualTo(SEEDS * 95L / 100);
        assertThat(count(rs, r -> r.maxTerm() - r.termAtScenarioStart() > 10)).isGreaterThanOrEqualTo(SEEDS * 95L / 100);
    }

    /**
     * CheckQuorum: a leader cut off from everyone steps down within two election timeouts (the check runs
     * once per timeout; the first check can still pass on replies from just before the cut) plus a message
     * delay. Measured 97-204 ms.
     */
    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void checkQuorumMakesAnIsolatedLeaderStepDown(int nodes) {
        List<SimResult> rs = runAll(SimConfig.scenario(nodes, Scenario.ISOLATE_LEADER));
        print("isolate leader, " + nodes + " nodes, both on", rs);
        assertThat(rs).allSatisfy(r -> {
            assertThat(r.scenarioStepDownMs()).isBetween(0L, 250L);
            assertThat(r.maxTerm() - r.termAtScenarioStart()).isLessThanOrEqualTo(3);   // one or two elections
        });
    }

    /** The control: without CheckQuorum the cut-off leader keeps its role until the heal at +3 s. */
    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void withoutCheckQuorumAnIsolatedLeaderStaysLeaderUntilTheHeal(int nodes) {
        List<SimResult> rs = runAll(SimConfig.scenario(nodes, Scenario.ISOLATE_LEADER).withHardening(true, false));
        print("isolate leader, " + nodes + " nodes, checkQuorum off", rs);
        assertThat(rs).allSatisfy(r -> assertThat(r.scenarioStepDownMs()).isGreaterThanOrEqualTo(
                Scenario.HEAL_MS - Scenario.START_MS));
    }

    /**
     * Why both are needed: CheckQuorum without PreVote does make the isolated leader step down, but then
     * it times out over and over on its own and inflates the term (measured +21 on average), deposing the
     * new leader when it rejoins.
     */
    @Test
    void checkQuorumWithoutPreVoteInflatesTheTerm() {
        List<SimResult> rs = runAll(SimConfig.scenario(3, Scenario.ISOLATE_LEADER).withHardening(false, true));
        print("isolate leader, 3 nodes, preVote off", rs);
        assertThat(count(rs, r -> r.maxTerm() - r.termAtScenarioStart() > 10)).isGreaterThanOrEqualTo(SEEDS * 95L / 100);
    }

    /** Mutation PREVOTE_BUMPS_TERM: the disruption scenario catches it (the invariant checkers cannot). */
    @Test
    void mutationPreVoteThatBumpsTheTermIsCaught() {
        List<SimResult> rs = runAll(SimConfig.scenario(3, Scenario.ISOLATE_FOLLOWER)
                .withMutation(ProtocolMutation.PREVOTE_BUMPS_TERM));
        print("mutation prevote bumps term", rs);
        assertThat(count(rs, r -> r.newLeadersSinceScenario() > 0)).isGreaterThanOrEqualTo(SEEDS * 95L / 100);
    }
}
