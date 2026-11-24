package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Figure 8 and §5.4.2: a leader must not commit an entry from an earlier term just because it is
 * stored on a majority. In Figure 8 (c), S1 (leader of term 4) has replicated the term-2 entry at
 * index 2 to S1, S2, S3; if S1 crashed, S5 (with a term-3 entry at index 2) could still be elected by
 * S2, S3, S4 and overwrite it (d). Only once an entry of term 4 is on a majority (e) is index 2 safe.
 */
class Figure8Test {

    /** S1 recovers with log [1, 2] and wins term 4; its no-op goes at index 3. */
    private static RaftNodeCore s1(Set<ProtocolMutation> mutations) {
        PersistentState s = new PersistentState(new HardState(3, java.util.Optional.empty()), entries(1, 1, 2));
        RaftNodeCore c = RaftNodeCore.withMutationsForTesting(
                config(N1, FIVE), s, new java.util.SplittableRandom(42), mutations);
        tickUntilElection(c);
        receive(c, new Message.RequestVoteResponse(N2, N1, 4, true));
        receive(c, new Message.RequestVoteResponse(N3, N1, 4, true));
        assertThat(c.status().soft().role()).isEqualTo(Role.LEADER);
        return c;
    }

    @Test
    void oldTermEntryOnAMajorityIsNotCommittedUntilACurrentTermEntryIs() {
        RaftNodeCore leader = s1(Set.of());

        // (c): S2 and S3 store index 2 (term 2), but not the no-op yet. S1+S2+S3 is a majority of 5.
        receive(leader, ok(N2, N1, 4, 2));
        receive(leader, ok(N3, N1, 4, 2));
        assertThat(leader.status().commitIndex()).as("index 2 is from term 2: not committable by counting").isZero();

        // (e): the term-4 no-op reaches a majority; index 3 commits, and indexes 1-2 commit with it.
        receive(leader, ok(N2, N1, 4, 3));
        Ready r = receive(leader, ok(N3, N1, 4, 3));
        assertThat(leader.status().commitIndex()).isEqualTo(3);
        assertThat(r.toApply()).extracting(LogEntry::index).containsExactly(1L, 2L, 3L);
    }

    /** The mutation hook really breaks the rule: the simulator test relies on this. */
    @Test
    void mutationCommitsTheOldTermEntryByCounting() {
        RaftNodeCore leader = s1(Set.of(ProtocolMutation.COMMIT_OLD_TERM_BY_COUNT));

        receive(leader, ok(N2, N1, 4, 2));
        receive(leader, ok(N3, N1, 4, 2));

        assertThat(leader.status().commitIndex()).isEqualTo(2);
    }

    /** Figure 8 (d): with index 2 uncommitted, S5 (last term 3) is more up to date than S2/S3 (last term 2). */
    @Test
    void s5CanWinAgainstLogsThatOnlyHaveTheOldTermEntry() {
        RaftNodeCore s2 = core(N2, FIVE, stateWithLog(4, 1, 2), 1);

        Ready r = receive(s2, new Message.RequestVote(N5, N2, 5, 2, 3));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N2, N5, 5, true));
    }

    /** Once S2 holds the term-4 entry, S5 cannot get its vote: the committed prefix is protected (§5.4.1). */
    @Test
    void s5CannotWinOnceTheCurrentTermEntryIsReplicated() {
        RaftNodeCore s2 = core(N2, FIVE, stateWithLog(4, 1, 2, 4), 1);

        Ready r = receive(s2, new Message.RequestVote(N5, N2, 5, 2, 3));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N2, N5, 5, false));
    }

    /** Mutation hook check: skipping the up-to-date check grants the vote it must refuse. */
    @Test
    void mutationSkipsTheUpToDateCheck() {
        PersistentState s = stateWithLog(4, 1, 2, 4);
        RaftNodeCore s2 = RaftNodeCore.withMutationsForTesting(config(N2, FIVE), s,
                new java.util.SplittableRandom(1), Set.of(ProtocolMutation.SKIP_UP_TO_DATE_CHECK));

        Ready r = receive(s2, new Message.RequestVote(N5, N2, 5, 2, 3));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N2, N5, 5, true));
    }

    @Test
    void normalConstructorHasNoMutations() {
        assertThat(core(N1, THREE, PersistentState.EMPTY, 1).mutations()).isEmpty();
        assertThat(List.of(ProtocolMutation.values())).hasSize(6);   // M2: 3, M3: 1, M4: 2
    }
}
