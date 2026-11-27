package com.projects.raftkv.dashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.sim.InvariantViolation;
import com.projects.raftkv.sim.SimConfig;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** The Simulator tab's engine: settings, failure categories, and statistics of failed runs. */
class SimJobTest {

    @Test
    void failuresAreFiledUnderTheCheckerThatRaisedThem() {
        assertThat(SimJob.category(new InvariantViolation("Durability violated at t=1962ms: n1 reports..."))).isEqualTo("Durability");
        assertThat(SimJob.category(new InvariantViolation("Linearizability violated: key k0..."))).isEqualTo("Linearizability");
        assertThat(SimJob.category(new InvariantViolation("Liveness: no current-term leader"))).isEqualTo("Liveness");
        assertThat(SimJob.category(new InvariantViolation("something new"))).isEqualTo("Invariant");
        assertThat(SimJob.category(new IllegalStateException("guard"))).isEqualTo("assertion: IllegalStateException");
    }

    /** Before M6 a failed seed reported zeros everywhere, because the simulator threw its counters away. */
    @Test
    void aFailedSeedKeepsItsStatisticsUpToTheFailure() {
        SimJob.Row row = SimJob.runOne(1, SimConfig.chaos(3).withoutDedup());
        assertThat(row.outcome()).isEqualTo("Linearizability");
        assertThat(row.ops()).isPositive();
        assertThat(row.elections()).isPositive();
        assertThat(row.events()).isPositive();
        assertThat(row.failedAtMs()).isEqualTo(13_000);   // linearizability is checked once, at the end of the run
        assertThat(row.message()).contains("replay:");
    }

    @Test
    void aPassingSeedHasNoFailureTime() {
        SimJob.Row row = SimJob.runOne(1, SimConfig.chaos(3));
        assertThat(row.outcome()).isEqualTo("passed");
        assertThat(row.failedAtMs()).isEqualTo(-1);
    }

    @Test
    void totalsIncludeFailedSeeds() throws InterruptedException {
        SimJob.Result r = SimJob.run(new SimSettings(3, 5, 1, "chaos", true, true, 0, "SKIP_DEDUP"), new AtomicBoolean(), p -> { });
        assertThat(r.passed()).isZero();
        assertThat(r.totals().get("ops")).isPositive();
        assertThat(r.totals().get("elections")).isPositive();
    }

    @Test
    void everyOfferedMutationBuildsAConfig() {
        for (String m : SimSettings.mutations()) {
            for (String mix : SimSettings.MIXES) {
                assertThat(new SimSettings(5, 1, 1, mix, false, true, 2, m).toConfig()).as(mix + " " + m).isNotNull();
            }
        }
    }

    @Test
    void rejectsSettingsTheSimulatorCannotRun() {
        assertThatThrownBy(() -> new SimSettings(4, 1, 1, "chaos", true, true, 0, "none")).hasMessageContaining("3 or 5");
        assertThatThrownBy(() -> new SimSettings(3, 0, 1, "chaos", true, true, 0, "none")).hasMessageContaining("seeds");
        assertThatThrownBy(() -> new SimSettings(3, 1, 0, "chaos", true, true, 0, "none")).hasMessageContaining("firstSeed");
        assertThatThrownBy(() -> new SimSettings(3, 1, 1, "storm", true, true, 0, "none")).hasMessageContaining("unknown mix");
        assertThatThrownBy(() -> new SimSettings(3, 1, 1, "chaos", true, true, 51, "none")).hasMessageContaining("batchWindowMs");
        assertThatThrownBy(() -> new SimSettings(3, 1, 1, "chaos", true, true, 0, "RM_RF")).hasMessageContaining("unknown mutation");
    }
}
