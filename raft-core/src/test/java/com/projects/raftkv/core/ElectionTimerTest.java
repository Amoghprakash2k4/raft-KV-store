package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/** §5.2: followers start an election after a randomized timeout in [T, 2T) with no valid leader contact. */
class ElectionTimerTest {

    @Property
    void electionStartsWithinRandomizedWindow(@ForAll long seed) {
        RaftNodeCore c = core(N1, THREE, PersistentState.EMPTY, seed);
        int ticks = 0;
        while (c.status().soft().role() == Role.FOLLOWER) {
            tick(c);
            ticks++;
            assertThat(ticks).isLessThanOrEqualTo(2 * T);
        }
        assertThat(ticks).isBetween(T, 2 * T - 1);
    }

    @Test
    void startingAnElectionIncrementsTermVotesForSelfAndPersistsBeforeAsking() {
        RaftNodeCore c = core(N1, THREE, state(4, Optional.empty(), 1, 3), 7);

        Ready r = tickUntilElection(c);

        assertThat(c.status().hard()).isEqualTo(new HardState(5, Optional.of(N1)));
        assertThat(r.persist()).containsExactly(new PersistOp.SaveHardState(new HardState(5, Optional.of(N1))));
        assertThat(r.messages()).containsExactlyInAnyOrder(
                new Message.RequestVote(N1, N2, 5, 2, 3),
                new Message.RequestVote(N1, N3, 5, 2, 3));
        assertThat(r.softState()).contains(new SoftState(Role.CANDIDATE, 5, Optional.empty()));
    }

    @Test
    void splitVoteLeadsToNewElectionInNextTerm() {
        RaftNodeCore c = fresh(N1);
        tickUntilElection(c);
        receive(c, new Message.RequestVoteResponse(N2, N1, 1, false));
        receive(c, new Message.RequestVoteResponse(N3, N1, 1, false));

        tickUntilElection2(c);

        assertThat(c.status().hard()).isEqualTo(new HardState(2, Optional.of(N1)));
    }

    @Test
    void idleTicksDoNotPersistAnything() {
        RaftNodeCore c = fresh(N1);
        for (int i = 0; i < T - 1; i++) {
            assertThat(tick(c).isEmpty()).isTrue();
        }
    }

    @Test
    void sameSeedSameInputsProduceIdenticalReadys() {
        List<Ready> a = runScript(core(N1, THREE, PersistentState.EMPTY, 99));
        List<Ready> b = runScript(core(N1, THREE, PersistentState.EMPTY, 99));
        assertThat(a).isEqualTo(b);
    }

    private static List<Ready> runScript(RaftNodeCore c) {
        java.util.ArrayList<Ready> out = new java.util.ArrayList<>();
        for (int i = 0; i < 3 * T; i++) out.add(tick(c));
        out.add(receive(c, new Message.RequestVote(N2, N1, 9, 0, 0)));
        for (int i = 0; i < 3 * T; i++) out.add(tick(c));
        return out;
    }

    /** Ticks while still a candidate of the current term, until the term advances. */
    private static void tickUntilElection2(RaftNodeCore c) {
        long term = c.status().hard().currentTerm();
        for (int i = 0; i < 2 * T && c.status().hard().currentTerm() == term; i++) tick(c);
        assertThat(c.status().hard().currentTerm()).isEqualTo(term + 1);
        assertThat(c.status().soft().role()).isEqualTo(Role.CANDIDATE);
    }
}
