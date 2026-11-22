package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Figure 2 "Leaders" heartbeat rule, AppendEntries receiver term rules, §5.2. */
class LeaderAndFollowerTest {

    @Test
    void leaderSendsHeartbeatsEveryIntervalAndNeverStartsAnElection() {
        RaftNodeCore leader = leaderOf3(N1);
        long term = leader.status().hard().currentTerm();

        List<Message> sent = tickN(leader, 10 * T);

        assertThat(ofType(sent, Message.AppendEntries.class)).hasSize(2 * (10 * T / HEARTBEAT));
        assertThat(leader.status().soft()).isEqualTo(new SoftState(Role.LEADER, term, Optional.of(N1)));
    }

    @Test
    void leaderStepsDownOnHigherTermAndStartsAFreshElectionTimer() {
        RaftNodeCore leader = leaderOf3(N1);

        receive(leader, reject(N2, N1, 9, 0));

        assertThat(leader.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 9, Optional.empty()));
        assertThat(tickN(leader, T - 1)).isEmpty();
    }

    @Test
    void followerAcceptsHeartbeatLearnsLeaderAndResetsTimer() {
        RaftNodeCore f = fresh(N1);
        for (int i = 0; i < 5; i++) {
            tickN(f, T - 1);
            Ready r = receive(f, heartbeat(N2, N1, 1));
            assertThat(r.messages()).containsExactly(ok(N1, N2, 1, 0));
        }
        assertThat(f.status().soft()).isEqualTo(new SoftState(Role.FOLLOWER, 1, Optional.of(N2)));
    }

    @Test
    void staleHeartbeatIsRejectedAndDoesNotResetTimer() {
        RaftNodeCore f = core(N1, THREE, state(5, Optional.empty()), 2);
        int ticks = 0;
        while (f.status().soft().role() == Role.FOLLOWER) {
            Ready r = receive(f, heartbeat(N2, N1, 4));
            assertThat(r.messages()).containsExactly(new Message.AppendEntriesResponse(N1, N2, 5, false, 0, 0, 0, 0));
            tick(f);
            ticks++;
            assertThat(ticks).isLessThan(2 * T);
        }
    }

    @Test
    void staleResponsesAreIgnored() {
        RaftNodeCore leader = leaderOf3(N1);
        long term = leader.status().hard().currentTerm();

        Ready r = receive(leader, reject(N2, N1, term - 1, 0));

        assertThat(r.isEmpty()).isTrue();
    }

    /** A second leader in the same term is an Election Safety violation; fail loudly. */
    @Test
    void appendEntriesFromAnotherLeaderInOwnTermIsAFatalSafetyViolation() {
        RaftNodeCore leader = leaderOf3(N1);
        long term = leader.status().hard().currentTerm();

        assertThatThrownBy(() -> receive(leader, heartbeat(N2, N1, term)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Election Safety");
    }

    @Test
    void followerDetectsTwoDifferentLeadersInOneTerm() {
        RaftNodeCore f = fresh(N1);
        receive(f, heartbeat(N2, N1, 1));

        assertThatThrownBy(() -> receive(f, heartbeat(N3, N1, 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Election Safety");
    }

    @Test
    void rejectsMessagesNotAddressedToThisNode() {
        RaftNodeCore f = fresh(N1);
        assertThatThrownBy(() -> receive(f, heartbeat(N2, N3, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
