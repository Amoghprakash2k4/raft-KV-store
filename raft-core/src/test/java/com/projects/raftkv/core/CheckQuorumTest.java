package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * CheckQuorum (thesis §6.2) and leader stickiness (§4.2.3), DESIGN.md M5-A. A leader that has not heard
 * from a majority within an election timeout steps down; a node that heard from a live leader within the
 * minimum election timeout ignores RequestVote (neither votes nor adopts the higher term).
 */
class CheckQuorumTest {

    static RaftNodeCore cq(NodeId self, PersistentState s) {
        return new RaftNodeCore(config(self, THREE).withCheckQuorum(true), s, new SplittableRandom(42));
    }

    /** A check-quorum leader of term 1 (N1, votes from N2). */
    static RaftNodeCore leader() {
        RaftNodeCore c = cq(N1, PersistentState.EMPTY);
        tickUntilElection(c);
        receive(c, new Message.RequestVoteResponse(N2, N1, 1, true));
        assertThat(c.status().soft().role()).isEqualTo(Role.LEADER);
        return c;
    }

    @Test
    void aLeaderThatHearsFromNobodyStepsDownAfterOneElectionTimeout() {
        RaftNodeCore l = leader();
        tickN(l, T - 1);
        assertThat(l.status().soft().role()).isEqualTo(Role.LEADER);

        Ready r = tick(l);
        assertThat(l.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 1, Optional.empty()));
        assertThat(r.softState()).contains(new SoftState(Role.FOLLOWER, 1, Optional.empty()));
        assertThat(l.status().hard()).isEqualTo(new HardState(1, Optional.of(N1)));   // same term, vote kept
    }

    @Test
    void aLeaderThatHearsFromAMajorityStays() {
        RaftNodeCore l = leader();
        for (int i = 0; i < 10 * T; i++) {
            tick(l);
            receive(l, ok(N2, N1, 1, 1));                               // N2 answers; N3 is silent
        }
        assertThat(l.status().soft().role()).isEqualTo(Role.LEADER);
    }

    @Test
    void repliesFromAnOlderTermDoNotCount() {
        RaftNodeCore c = cq(N1, stateWithLog(1));
        tickUntilElection(c);                                           // term 2
        receive(c, new Message.RequestVoteResponse(N2, N1, 2, true));
        for (int i = 0; i < T; i++) {
            tick(c);
            receive(c, ok(N2, N1, 1, 0));                               // a late reply from term 1
        }
        assertThat(c.status().soft().role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    void steppingDownRejectsPendingReads() {
        RaftNodeCore l = leader();
        l.step(new Event.ReadRequested(7));
        Ready r = null;
        for (int i = 0; i < T; i++) r = tick(l);
        assertThat(r.notices()).contains(new ClientNotice.ReadRejected(7, Optional.empty()));
    }

    @Test
    void withoutTheFlagAnIsolatedLeaderStaysLeader() {
        RaftNodeCore l = leaderOf3(N1);
        tickN(l, 10 * T);
        assertThat(l.status().soft().role()).isEqualTo(Role.LEADER);
    }

    @Test
    void aFollowerWithALiveLeaderIgnoresRequestVote() {
        RaftNodeCore f = cq(N2, PersistentState.EMPTY);
        receive(f, heartbeat(N1, N2, 1));

        Ready r = receive(f, new Message.RequestVote(N3, N2, 5, 0, 0));
        assertThat(r.messages()).isEmpty();
        assertThat(f.status().hard()).isEqualTo(new HardState(1, Optional.empty()));
        assertThat(f.status().soft().leader()).contains(N1);

        tickN(f, T);                         // no leader contact for T ticks (it may even have started an election)
        Ready later = receive(f, new Message.RequestVote(N3, N2, 5, 0, 0));
        assertThat(later.messages()).containsExactly(new Message.RequestVoteResponse(N2, N3, 5, true));
    }

    @Test
    void aLeaderIgnoresRequestVoteWhileItHasItsQuorum() {
        RaftNodeCore l = leader();
        Ready r = receive(l, new Message.RequestVote(N3, N1, 5, 9, 9));
        assertThat(r.messages()).isEmpty();
        assertThat(l.status().soft()).isEqualTo(new SoftState(Role.LEADER, 1, Optional.of(N1)));
    }

    @Test
    void aHigherTermOnAReplyStillDeposesALeader() {
        // Stickiness only filters vote requests: any other message with a higher term is obeyed (§5.1).
        RaftNodeCore l = leader();
        receive(l, new Message.AppendEntriesResponse(N2, N1, 4, false, 0, 0, 0, 0));
        assertThat(l.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 4, Optional.empty()));
    }
}
