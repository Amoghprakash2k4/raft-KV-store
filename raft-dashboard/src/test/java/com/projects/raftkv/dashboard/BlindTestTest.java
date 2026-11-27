package com.projects.raftkv.dashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** The blind bug test: the plan must hold exactly what was asked for, and the verdicts must add up. */
class BlindTestTest {

    private static BlindTest.Spec spec(int seeds, Map<String, Integer> bugs) {
        return new BlindTest.Spec(3, seeds, 1, 0, bugs, null);
    }

    @Test
    void thePlanHoldsExactlyTheRequestedCountsAndEverySeedOnce() {
        BlindTest.Spec s = spec(500, Map.of("SKIP_DEDUP", 30, "LYING_DISK", 20, "SKIP_TORN_TAIL_CUT", 7));
        List<BlindTest.Planned> plan = BlindTest.plan(s, 99);

        assertThat(plan).hasSize(500);
        assertThat(plan.stream().map(BlindTest.Planned::seed).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(java.util.stream.LongStream.rangeClosed(1, 500).boxed().toList());
        Map<String, Long> counts = plan.stream().collect(Collectors.groupingBy(BlindTest.Planned::bug, Collectors.counting()));
        assertThat(counts).containsEntry("SKIP_DEDUP", 30L).containsEntry("LYING_DISK", 20L)
                .containsEntry("SKIP_TORN_TAIL_CUT", 7L).containsEntry("none", 443L);
    }

    @Test
    void eachBugRunsUnderItsOwnProfileAndCleanSeedsOnlyUnderProfilesInPlay() {
        List<BlindTest.Planned> plan = BlindTest.plan(spec(300, Map.of("SKIP_DEDUP", 10, "SKIP_TORN_TAIL_CUT", 10)), 7);
        for (BlindTest.Planned p : plan) {
            if (!p.bug().equals("none")) assertThat(p.profile()).isEqualTo(BlindTest.bug(p.bug()).profile());
        }
        Set<BlindTest.Profile> clean = plan.stream().filter(p -> p.bug().equals("none")).map(BlindTest.Planned::profile)
                .collect(Collectors.toSet());
        // Both profiles in play show up among clean seeds, so the settings can't reveal which seeds are bugged.
        assertThat(clean).containsExactlyInAnyOrder(BlindTest.Profile.CHAOS, BlindTest.Profile.CRASH_LOOP);
    }

    @Test
    void theSamePlanSeedGivesTheSamePlanAndADifferentOneShuffles() {
        BlindTest.Spec s = spec(200, Map.of("SKIP_DEDUP", 20));
        assertThat(BlindTest.plan(s, 5)).isEqualTo(BlindTest.plan(s, 5));
        Set<Long> a = bugged(BlindTest.plan(s, 5)), b = bugged(BlindTest.plan(s, 6));
        assertThat(a).isNotEqualTo(b);
    }

    private static Set<Long> bugged(List<BlindTest.Planned> plan) {
        Set<Long> out = new HashSet<>();
        for (BlindTest.Planned p : plan) if (!p.bug().equals("none")) out.add(p.seed());
        return out;
    }

    @Test
    void rejectsPlansThatCannotBeBuilt() {
        assertThatThrownBy(() -> spec(10, Map.of("SKIP_DEDUP", 11))).hasMessageContaining("do not fit");
        assertThatThrownBy(() -> spec(10, Map.of("NOT_A_BUG", 1))).hasMessageContaining("unknown bug");
        assertThatThrownBy(() -> spec(10, Map.of("SKIP_DEDUP", -1))).hasMessageContaining("bad count");
        assertThatThrownBy(() -> spec(10, Map.of("SKIP_DEDUP", 0))).hasMessageContaining("at least one");
        assertThatThrownBy(() -> spec(10, Map.of())).hasMessageContaining("at least one");
        assertThatThrownBy(() -> new BlindTest.Spec(4, 10, 1, 0, Map.of("SKIP_DEDUP", 1), null)).hasMessageContaining("3 or 5");
        assertThatThrownBy(() -> new BlindTest.Spec(3, 10, 1, 0, Map.of("SKIP_DEDUP", 1), new BlindTest.Fixed("storm", true, true, 0)))
                .hasMessageContaining("unknown mix");
    }

    /** Controls (PREVOTE_SKIPS_LOG_CHECK) and liveness-only changes (PREVOTE_BUMPS_TERM) are not plantable bugs. */
    @Test
    void onlyRealSafetyBugsArePlantable() {
        assertThat(BlindTest.BUGS).extracting(BlindTest.Bug::name)
                .doesNotContain("PREVOTE_SKIPS_LOG_CHECK", "PREVOTE_BUMPS_TERM", "none").hasSize(11);
    }

    /** Deterministic: the same plan seed and simulation seeds always give these verdicts. */
    @Test
    void verdictsAddUpAndCleanSeedsAreNeverFlagged() throws InterruptedException {
        BlindTest.Spec s = new BlindTest.Spec(3, 10, 1, 42, Map.of("SKIP_DEDUP", 4), null);
        BlindTest.Result r = BlindTest.run(s, new AtomicBoolean(), p -> { });

        assertThat(r.planSeed()).isEqualTo(42);
        assertThat(r.planted()).isEqualTo(4);
        assertThat(r.caught()).isEqualTo(4);          // SKIP_DEDUP: measured 100/100 in MutationSanityTest
        assertThat(r.falseAlarms()).isZero();
        assertThat(r.cleanPasses()).isEqualTo(6);
        assertThat(r.caught() + r.missed() + r.falseAlarms() + r.cleanPasses()).isEqualTo(r.seeds());
        BlindTest.PerBug dedup = r.perBug().getFirst();
        assertThat(dedup.caughtBy()).containsEntry("Linearizability", 4);
        // A caught seed keeps its statistics up to the failure.
        for (BlindTest.Row row : r.rows()) {
            if (row.verdict().equals("caught")) {
                assertThat(row.stats().ops()).isPositive();
                assertThat(row.stats().failedAtMs()).isPositive();
            }
        }
    }

    @Test
    void fixedSettingsApplyToEverySeed() throws InterruptedException {
        BlindTest.Fixed fixed = new BlindTest.Fixed("crashLoop", true, true, 2);
        BlindTest.Result r = BlindTest.run(new BlindTest.Spec(3, 6, 1, 3, Map.of("SKIP_TORN_TAIL_CUT", 3), fixed),
                new AtomicBoolean(), p -> { });
        assertThat(r.rows()).extracting(BlindTest.Row::profile).containsOnly("crashLoop, group commit");
        assertThat(r.falseAlarms()).isZero();
        assertThat(r.planted()).isEqualTo(3);
    }
}
