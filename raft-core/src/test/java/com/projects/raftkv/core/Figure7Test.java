package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Figure 7: the leader for term 8 and six possible follower logs (a)-(f). Whatever the follower holds
 * (missing entries, extra uncommitted entries, or both), the consistency check plus nextIndex backup
 * makes its log identical to the leader's (§5.3).
 */
class Figure7Test {

    private static final long[] LEADER = {1, 1, 1, 4, 4, 5, 5, 6, 6, 6};

    static List<Arguments> followers() {
        return List.of(
                Arguments.of("a", new long[] {1, 1, 1, 4, 4, 5, 5, 6, 6}),
                Arguments.of("b", new long[] {1, 1, 1, 4}),
                Arguments.of("c", new long[] {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}),
                Arguments.of("d", new long[] {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}),
                Arguments.of("e", new long[] {1, 1, 1, 4, 4, 4, 4}),
                Arguments.of("f", new long[] {1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}));
    }

    @ParameterizedTest(name = "follower ({0})")
    @MethodSource("followers")
    void followerConvergesToTheLeadersLog(String name, long[] followerTerms) {
        RaftNodeCore leader = leaderAt(N1, THREE, 8, stateWithLog(7, LEADER), List.of(N3));
        RaftNodeCore follower = core(N2, THREE, stateWithLog(7, followerTerms), 7);
        MiniCluster c = new MiniCluster().add(leader).add(follower).isolate(N3);

        for (int i = 0; i < 3 * followerTerms.length + 5; i++) {
            c.step(N1, new Event.Tick());
            c.deliverAll();
        }

        // The leader's log is Figure 7's plus its own no-op (index 11, term 8).
        assertThat(terms(leader.log())).isEqualTo(termList(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8));
        assertThat(terms(follower.log())).isEqualTo(terms(leader.log()));
        for (long i = 1; i <= leader.log().lastIndex(); i++) {
            assertThat(follower.log().entryAt(i)).isEqualTo(leader.log().entryAt(i));
        }
        // Leader + this follower are a majority of 3, so the no-op and everything before it commit.
        assertThat(leader.status().commitIndex()).isEqualTo(11);
        assertThat(follower.status().commitIndex()).isEqualTo(11);
    }

    /**
     * Fast backup: one round trip per conflicting term instead of one per entry (thesis §5.3). With
     * plain decrement-by-one, follower (f) would need 8 probes; with hints every case takes at most 3.
     */
    @ParameterizedTest(name = "follower ({0})")
    @MethodSource("followers")
    void fastBackupConvergesInAFewRoundTrips(String name, long[] followerTerms) {
        RaftNodeCore leader = leaderAt(N1, THREE, 8, stateWithLog(7, LEADER), List.of(N3));
        RaftNodeCore follower = core(N2, THREE, stateWithLog(7, followerTerms), 7);
        MiniCluster c = new MiniCluster().add(leader).add(follower).isolate(N3);

        c.step(N1, new Event.Tick());
        c.step(N1, new Event.Tick());                    // heartbeat interval: the first probe goes out
        c.deliverAll();

        assertThat(terms(follower.log())).isEqualTo(terms(leader.log()));
        assertThat(c.appendEntriesDelivered()).isLessThanOrEqualTo(3);
    }
}
