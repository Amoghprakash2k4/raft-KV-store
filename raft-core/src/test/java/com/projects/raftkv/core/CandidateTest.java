package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Figure 2 "Candidates" rules, §5.2. */
class CandidateTest {

    @Test
    void becomesLeaderOnMajorityAndSendsHeartbeatsImmediately() {
        RaftNodeCore c = fresh(N1);
        tickUntilElection(c);

        Ready r = receive(c, new Message.RequestVoteResponse(N2, N1, 1, true));

        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.LEADER, 1, Optional.of(N1)));
        assertThat(r.softState()).contains(new SoftState(Role.LEADER, 1, Optional.of(N1)));
        assertThat(r.messages()).containsExactlyInAnyOrder(heartbeat(N1, N2, 1), heartbeat(N1, N3, 1));
    }

    @Test
    void duplicatedVoteIsCountedOnce() {
        RaftNodeCore c = core(N1, FIVE, PersistentState.EMPTY, 3);
        tickUntilElection(c);

        receive(c, new Message.RequestVoteResponse(N2, N1, 1, true));
        receive(c, new Message.RequestVoteResponse(N2, N1, 1, true));

        assertThat(c.status().soft().role()).isEqualTo(Role.CANDIDATE);
        receive(c, new Message.RequestVoteResponse(N3, N1, 1, true));
        assertThat(c.status().soft().role()).isEqualTo(Role.LEADER);
    }

    @Test
    void voteFromAnEarlierElectionDoesNotCount() {
        RaftNodeCore c = core(N1, FIVE, PersistentState.EMPTY, 3);
        tickUntilElection(c);           // term 1
        long first = c.status().hard().currentTerm();
        for (int i = 0; i < 2 * T && c.status().hard().currentTerm() == first; i++) tick(c); // term 2

        receive(c, new Message.RequestVoteResponse(N2, N1, first, true));
        receive(c, new Message.RequestVoteResponse(N3, N1, first, true));

        assertThat(c.status().soft().role()).isEqualTo(Role.CANDIDATE);
    }

    @Test
    void stepsDownOnHigherTermResponse() {
        RaftNodeCore c = fresh(N1);
        tickUntilElection(c);

        Ready r = receive(c, new Message.RequestVoteResponse(N2, N1, 5, false));

        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 5, Optional.empty()));
        assertThat(r.persist()).containsExactly(new PersistOp.SaveHardState(new HardState(5, Optional.empty())));
    }

    /** §5.2: a candidate that hears from a legitimate leader of its own term steps down; its vote stays. */
    @Test
    void stepsDownOnAppendEntriesFromLeaderOfSameTerm() {
        RaftNodeCore c = fresh(N1);
        tickUntilElection(c);

        Ready r = receive(c, heartbeat(N2, N1, 1));

        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 1, Optional.of(N2)));
        assertThat(c.status().hard().votedFor()).contains(N1);
        assertThat(r.messages()).containsExactly(new Message.AppendEntriesResponse(N1, N2, 1, true, 0));
    }
}
