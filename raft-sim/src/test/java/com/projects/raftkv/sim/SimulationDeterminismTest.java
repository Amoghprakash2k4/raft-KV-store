package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** A seed must fully determine a run, or failing seeds could not be replayed. */
class SimulationDeterminismTest {

    @Test
    void sameSeedProducesIdenticalRun() {
        SimResult a = Simulation.run(7, SimConfig.chaos(5));
        SimResult b = Simulation.run(7, SimConfig.chaos(5));
        assertThat(a).isEqualTo(b);
        assertThat(a.eventsProcessed()).isGreaterThan(1000);
    }

    @Test
    void differentSeedsProduceDifferentRuns() {
        assertThat(Simulation.run(7, SimConfig.chaos(5)).traceHash())
                .isNotEqualTo(Simulation.run(8, SimConfig.chaos(5)).traceHash());
    }
}
