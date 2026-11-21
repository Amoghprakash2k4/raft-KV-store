package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Figure 2 RequestVote receiver rules, §5.1 term rule, §5.4.1 election restriction. */
class RequestVoteTest {

    @Test
    void grantsVoteInHigherTermAndPersistsVoteWithReply() {
        RaftNodeCore c = fresh(N1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 3, 0, 0));

        HardState expected = new HardState(3, Optional.of(N2));
        assertThat(r.persist()).containsExactly(new PersistOp.SaveHardState(expected));
        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 3, true));
        assertThat(c.status().hard()).isEqualTo(expected);
    }

    @Test
    void rejectsStaleTermWithCurrentTermAndChangesNothing() {
        RaftNodeCore c = core(N1, THREE, state(5, Optional.empty()), 1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 4, 0, 0));

        assertThat(r.persist()).isEmpty();
        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 5, false));
    }

    @Test
    void votesAtMostOncePerTermButRepeatsGrantToSameCandidate() {
        RaftNodeCore c = fresh(N1);
        receive(c, new Message.RequestVote(N2, N1, 1, 0, 0));

        Ready other = receive(c, new Message.RequestVote(N3, N1, 1, 0, 0));
        Ready duplicate = receive(c, new Message.RequestVote(N2, N1, 1, 0, 0));

        assertThat(other.messages()).containsExactly(new Message.RequestVoteResponse(N1, N3, 1, false));
        assertThat(duplicate.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 1, true));
        assertThat(duplicate.persist()).isEmpty();
    }

    @Test
    void rejectsCandidateWithLowerLastLogTerm() {
        RaftNodeCore c = core(N1, THREE, state(3, Optional.empty(), 1, 3), 1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 4, 5, 2));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 4, false));
    }

    @Test
    void rejectsCandidateWithSameLastTermButShorterLog() {
        RaftNodeCore c = core(N1, THREE, state(3, Optional.empty(), 1, 3, 3), 1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 4, 2, 3));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 4, false));
    }

    @Test
    void grantsCandidateWithHigherLastTermEvenIfShorter() {
        RaftNodeCore c = core(N1, THREE, state(3, Optional.empty(), 1, 1, 1), 1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 4, 1, 2));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 4, true));
    }

    /** Higher term is adopted (and persisted, with votedFor cleared) even when the vote is refused. */
    @Test
    void adoptsHigherTermEvenWhenRejectingForStaleLog() {
        RaftNodeCore c = core(N1, THREE, state(3, Optional.of(N1), 1, 3), 1);

        Ready r = receive(c, new Message.RequestVote(N2, N1, 7, 1, 1));

        assertThat(r.persist()).containsExactly(new PersistOp.SaveHardState(new HardState(7, Optional.empty())));
        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N2, 7, false));
    }

    @Test
    void grantingAVoteResetsTheElectionTimer() {
        RaftNodeCore c = fresh(N1);
        tickN(c, T - 1);
        receive(c, new Message.RequestVote(N2, N1, 1, 0, 0));

        tickN(c, T - 1);

        assertThat(c.status().soft().role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    void rejectedVoteDoesNotResetTheElectionTimer() {
        RaftNodeCore c = core(N1, THREE, state(3, Optional.empty(), 3), 1);
        int ticks = 0;
        // Keep refusing stale-log candidates every tick; the election must still fire within 2T.
        while (c.status().soft().role() == Role.FOLLOWER) {
            receive(c, new Message.RequestVote(N2, N1, 3, 0, 0));
            tick(c);
            ticks++;
            assertThat(ticks).isLessThan(2 * T);
        }
    }

    /** Crash-recovery: a recovered vote must still bind for its term. */
    @Test
    void recoveredVoteIsHonouredAfterRestart() {
        RaftNodeCore restarted = core(N1, THREE, state(4, Optional.of(N2)), 5);

        Ready r = receive(restarted, new Message.RequestVote(N3, N1, 4, 0, 0));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N3, 4, false));
    }

    @Test
    void leaderRefusesVoteInItsOwnTerm() {
        RaftNodeCore leader = leaderOf3(N1);
        long term = leader.status().hard().currentTerm();

        Ready r = receive(leader, new Message.RequestVote(N3, N1, term, 0, 0));

        assertThat(r.messages()).containsExactly(new Message.RequestVoteResponse(N1, N3, term, false));
        assertThat(leader.status().soft().role()).isEqualTo(Role.LEADER);
    }
}
