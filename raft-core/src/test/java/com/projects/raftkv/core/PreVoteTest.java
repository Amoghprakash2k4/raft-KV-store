package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * PreVote (thesis §9.6, DESIGN.md M5-A). A node whose election timer fires first asks "would you vote for
 * me in term T+1?" without raising its term or persisting anything; only a majority of yes answers starts
 * a real election. A receiver says yes only if the candidate's log is up to date (§5.4.1) and it has not
 * heard from a live leader within the minimum election timeout.
 */
class PreVoteTest {

    static RaftConfig preVoteConfig(NodeId self, List<NodeId> members) {
        return config(self, members).withPreVote(true);
    }

    static RaftNodeCore pv(NodeId self, List<NodeId> members, PersistentState s) {
        return new RaftNodeCore(preVoteConfig(self, members), s, new SplittableRandom(42));
    }

    static RaftNodeCore pvMutated(NodeId self, PersistentState s, ProtocolMutation m) {
        return RaftNodeCore.withMutationsForTesting(preVoteConfig(self, THREE), s, new SplittableRandom(42), Set.of(m));
    }

    /** Ticks until the node is a pre-candidate; returns the Ready of that tick. */
    static Ready tickUntilPreVote(RaftCore c) {
        for (int i = 0; i < 2 * T; i++) {
            Ready r = tick(c);
            if (c.status().soft().role() == Role.PRE_CANDIDATE) return r;
        }
        throw new AssertionError("no pre-vote within 2T ticks");
    }

    @Test
    void aTimeoutStartsAPreVoteWithoutChangingTermOrVote() {
        RaftNodeCore c = pv(N1, THREE, stateWithLog(3, 1, 3));
        Ready r = tickUntilPreVote(c);

        assertThat(c.status().hard()).isEqualTo(new HardState(3, Optional.empty()));
        assertThat(r.persist()).isEmpty();                              // nothing to fsync: nothing changed
        assertThat(r.messages()).containsExactlyInAnyOrder(
                new Message.PreVote(N1, N2, 4, 2, 3), new Message.PreVote(N1, N3, 4, 2, 3));
        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.PRE_CANDIDATE, 3, Optional.empty()));
    }

    @Test
    void aMajorityOfPreVotesStartsTheRealElection() {
        RaftNodeCore c = pv(N1, THREE, PersistentState.EMPTY);
        tickUntilPreVote(c);

        // The granted reply carries the proposed term (1): it must not count as "a higher term, step down".
        Ready r = receive(c, new Message.PreVoteResponse(N2, N1, 1, true));

        assertThat(c.status().soft().role()).isEqualTo(Role.CANDIDATE);
        assertThat(c.status().hard()).isEqualTo(new HardState(1, Optional.of(N1)));
        assertThat(r.messages()).containsExactlyInAnyOrder(
                new Message.RequestVote(N1, N2, 1, 0, 0), new Message.RequestVote(N1, N3, 1, 0, 0));
    }

    @Test
    void duplicatedAndRejectedPreVotesDoNotMakeAMajority() {
        RaftNodeCore c = pv(N1, FIVE, PersistentState.EMPTY);
        tickUntilPreVote(c);

        receive(c, new Message.PreVoteResponse(N2, N1, 1, true));
        receive(c, new Message.PreVoteResponse(N2, N1, 1, true));       // duplicate
        receive(c, new Message.PreVoteResponse(N3, N1, 0, false));
        assertThat(c.status().soft().role()).isEqualTo(Role.PRE_CANDIDATE);

        receive(c, new Message.PreVoteResponse(N4, N1, 1, true));       // self + N2 + N4 = 3 of 5
        assertThat(c.status().soft().role()).isEqualTo(Role.CANDIDATE);
    }

    @Test
    void aGrantFromAnEarlierPreVoteRoundDoesNotCount() {
        RaftNodeCore c = pv(N1, FIVE, stateWithLog(3));
        tickUntilPreVote(c);
        receive(c, new Message.PreVoteResponse(N2, N1, 4, true));
        receive(c, new Message.PreVoteResponse(N3, N1, 2, true));       // a grant for another term: ignored
        assertThat(c.status().soft().role()).isEqualTo(Role.PRE_CANDIDATE);
    }

    @Test
    void anUnansweredPreCandidateRetriesInTheSameTerm() {
        RaftNodeCore c = pv(N1, THREE, PersistentState.EMPTY);
        tickUntilPreVote(c);
        List<Message> later = tickN(c, 2 * T);

        assertThat(ofType(later, Message.PreVote.class)).isNotEmpty().allMatch(m -> m.term() == 1);
        assertThat(c.status().hard().currentTerm()).isZero();           // no term inflation while cut off
    }

    @Test
    void aReceiverGrantsWithoutChangingItsOwnState() {
        RaftNodeCore f = pv(N2, THREE, PersistentState.EMPTY);
        Ready r = receive(f, new Message.PreVote(N1, N2, 1, 0, 0));

        assertThat(r.messages()).containsExactly(new Message.PreVoteResponse(N2, N1, 1, true));
        assertThat(r.persist()).isEmpty();
        assertThat(f.status().hard()).isEqualTo(new HardState(0, Optional.empty()));
        assertThat(f.status().soft().role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    void aReceiverThatHeardFromALeaderRecentlySaysNo() {
        RaftNodeCore f = pv(N2, THREE, PersistentState.EMPTY);
        receive(f, heartbeat(N3, N2, 1));                               // N3 leads term 1

        Ready r = receive(f, new Message.PreVote(N1, N2, 2, 0, 0));
        assertThat(r.messages()).containsExactly(new Message.PreVoteResponse(N2, N1, 1, false));

        tickN(f, T - 1);                                                // still within the minimum timeout
        assertThat(receive(f, new Message.PreVote(N1, N2, 2, 0, 0)).messages())
                .containsExactly(new Message.PreVoteResponse(N2, N1, 1, false));
        tick(f);                                                        // T ticks without leader contact
        assertThat(ofType(receive(f, new Message.PreVote(N1, N2, 2, 0, 0)).messages(), Message.PreVoteResponse.class))
                .singleElement().matches(Message.PreVoteResponse::granted);
    }

    @Test
    void aReceiverWithAMoreUpToDateLogSaysNo() {
        RaftNodeCore f = pv(N2, THREE, stateWithLog(2, 1, 2));
        Ready r = receive(f, new Message.PreVote(N1, N2, 3, 5, 1));     // longer log, but older last term
        assertThat(r.messages()).containsExactly(new Message.PreVoteResponse(N2, N1, 2, false));
    }

    @Test
    void aStalePreVoteIsRejectedWithTheNewerTermAndThePreCandidateCatchesUp() {
        RaftNodeCore f = pv(N2, THREE, stateWithLog(5));
        Ready r = receive(f, new Message.PreVote(N1, N2, 4, 0, 0));
        assertThat(r.messages()).containsExactly(new Message.PreVoteResponse(N2, N1, 5, false));
        assertThat(f.status().hard().currentTerm()).isEqualTo(5);

        RaftNodeCore c = pv(N1, THREE, stateWithLog(3));
        tickUntilPreVote(c);
        receive(c, new Message.PreVoteResponse(N2, N1, 5, false));      // a rejection from a newer term
        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 5, Optional.empty()));
    }

    @Test
    void aPreVoteNeverRaisesTheReceiversTerm() {
        RaftNodeCore f = pv(N2, THREE, stateWithLog(2, 1, 2));
        receive(f, new Message.PreVote(N1, N2, 9, 0, 0));               // rejected (stale log) and term 9
        assertThat(f.status().hard().currentTerm()).isEqualTo(2);
    }

    @Test
    void aPreCandidateFollowsALeaderOfItsTerm() {
        RaftNodeCore c = pv(N1, THREE, stateWithLog(2, 2));
        tickUntilPreVote(c);
        receive(c, append(N2, N1, 2, 1, 2, List.of(), 1));
        assertThat(c.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 2, Optional.of(N2)));
    }

    @Test
    void aLeaderSaysNoToPreVotes() {
        RaftNodeCore l = pv(N1, THREE, PersistentState.EMPTY);
        tickUntilPreVote(l);
        receive(l, new Message.PreVoteResponse(N2, N1, 1, true));
        receive(l, new Message.RequestVoteResponse(N2, N1, 1, true));
        assertThat(l.status().soft().role()).isEqualTo(Role.LEADER);

        Ready r = receive(l, new Message.PreVote(N3, N1, 2, 9, 1));
        assertThat(r.messages()).containsExactly(new Message.PreVoteResponse(N1, N3, 1, false));
        assertThat(l.status().soft().role()).isEqualTo(Role.LEADER);
    }

    @Test
    void withoutTheFlagATimeoutGoesStraightToAnElection() {
        RaftNodeCore c = fresh(N1);
        tickUntilElection(c);
        assertThat(c.status().hard().currentTerm()).isEqualTo(1);
    }

    @Test
    void mutationPreVoteBumpsTermInflatesTheTermWhileCutOff() {
        RaftNodeCore c = pvMutated(N1, PersistentState.EMPTY, ProtocolMutation.PREVOTE_BUMPS_TERM);
        tickN(c, 6 * T);
        assertThat(c.status().hard().currentTerm()).isGreaterThan(1);
    }

    @Test
    void mutationPreVoteSkipsLogCheckGrantsAStaleLog() {
        RaftNodeCore f = pvMutated(N2, stateWithLog(2, 1, 2), ProtocolMutation.PREVOTE_SKIPS_LOG_CHECK);
        assertThat(ofType(receive(f, new Message.PreVote(N1, N2, 3, 5, 1)).messages(), Message.PreVoteResponse.class))
                .singleElement().matches(Message.PreVoteResponse::granted);
    }
}
