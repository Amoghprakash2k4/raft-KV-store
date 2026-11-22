package com.projects.raftkv.sim;

import static com.projects.raftkv.sim.LogFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.Role;
import org.junit.jupiter.api.Test;

/** Each M2 checker accepts a legal history and reports the illegal one with its own message. */
class ReplicationCheckersTest {

    private static final NodeId N1 = new NodeId(1), N2 = new NodeId(2);

    // ---------------------------------------------------------------- Log Matching (invariant 3)

    @Test
    void logMatchingAcceptsLogsThatAgreeOnSharedEntries() {
        LogMatchingChecker c = new LogMatchingChecker();
        ListLog a = log("x", 1, 1, 2), b = log("x", 1, 1, 2, 3);
        c.observe(0, N1, a, tracked(a), 1);
        assertThatCode(() -> c.observe(1, N2, b, tracked(b), 1)).doesNotThrowAnyException();
    }

    /** Same (index, term) at index 3 but different entries at index 2 below it. */
    @Test
    void logMatchingRejectsSameIndexAndTermWithDifferentPrefixes() {
        LogMatchingChecker c = new LogMatchingChecker();
        ListLog a = log("x", 1, 1, 2);
        ListLog b = log("x", 1, 1, 2);
        b.entries.set(1, log("y", 1, 1).entries.get(1));
        c.observe(0, N1, a, tracked(a), 1);
        assertThatThrownBy(() -> c.observe(1, N2, b, tracked(b), 1))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Log Matching");
    }

    // ---------------------------------------------------------------- Leader Append-Only (invariant 2)

    @Test
    void leaderAppendOnlyAllowsGrowthAndTruncationAfterSteppingDown() {
        LeaderAppendOnlyChecker c = new LeaderAppendOnlyChecker();
        c.observe(0, status(1, Role.LEADER, 2, 0, 0), 3, false);
        c.observe(1, status(1, Role.LEADER, 2, 0, 0), 5, false);
        assertThatCode(() -> c.observe(2, status(1, Role.FOLLOWER, 3, 0, 0), 1, true)).doesNotThrowAnyException();
    }

    @Test
    void leaderAppendOnlyRejectsALeaderThatTruncates() {
        LeaderAppendOnlyChecker c = new LeaderAppendOnlyChecker();
        c.observe(0, status(1, Role.LEADER, 2, 0, 0), 5, false);
        assertThatThrownBy(() -> c.observe(1, status(1, Role.LEADER, 2, 0, 0), 5, true))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Leader Append-Only");
    }

    // ---------------------------------------------------------------- State Machine Safety (invariant 5)

    @Test
    void stateMachineSafetyAcceptsTheSameEntriesAppliedEverywhere() {
        StateMachineSafetyChecker c = new StateMachineSafetyChecker();
        c.observe(0, status(1, Role.LEADER, 2, 3, 3), log("x", 1, 1, 2));
        assertThatCode(() -> c.observe(1, status(2, Role.FOLLOWER, 2, 2, 2), log("x", 1, 1))).doesNotThrowAnyException();
    }

    @Test
    void stateMachineSafetyRejectsADifferentEntryAppliedAtTheSameIndex() {
        StateMachineSafetyChecker c = new StateMachineSafetyChecker();
        c.observe(0, status(1, Role.LEADER, 2, 2, 2), log("x", 1, 1));
        assertThatThrownBy(() -> c.observe(1, status(2, Role.FOLLOWER, 3, 2, 2), log("x", 1, 3)))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("State Machine Safety");
    }

    /** After a restart lastApplied starts again from 0; re-applying the same entries is fine. */
    @Test
    void stateMachineSafetyHandlesRestarts() {
        StateMachineSafetyChecker c = new StateMachineSafetyChecker();
        c.observe(0, status(1, Role.FOLLOWER, 2, 2, 2), log("x", 1, 1));
        c.observe(1, status(1, Role.FOLLOWER, 2, 0, 0), log("x", 1, 1));
        assertThatCode(() -> c.observe(2, status(1, Role.FOLLOWER, 2, 2, 2), log("x", 1, 1))).doesNotThrowAnyException();
    }

    @Test
    void replicaStateRejectsDifferentStatesAtTheSameLastApplied() {
        ReplicaStateChecker c = new ReplicaStateChecker();
        c.observe(0, N1, 4, 111);
        c.observe(1, N2, 4, 111);
        assertThatThrownBy(() -> c.observe(2, new NodeId(3), 4, 222))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Replica state divergence");
    }

    // ---------------------------------------------------------------- Leader Completeness (invariant 4)

    @Test
    void leaderCompletenessAcceptsALaterLeaderThatHasEveryCommittedEntry() {
        LeaderCompletenessChecker c = new LeaderCompletenessChecker();
        ListLog l1 = log("x", 1, 1, 2);
        c.observe(0, status(1, Role.LEADER, 2, 3, 3), tracked(l1));
        ListLog l2 = log("x", 1, 1, 2, 3);
        assertThatCode(() -> c.observe(1, status(2, Role.LEADER, 3, 0, 0), tracked(l2))).doesNotThrowAnyException();
    }

    @Test
    void leaderCompletenessRejectsALaterLeaderMissingACommittedEntry() {
        LeaderCompletenessChecker c = new LeaderCompletenessChecker();
        c.observe(0, status(1, Role.LEADER, 2, 3, 3), tracked(log("x", 1, 1, 2)));
        assertThatThrownBy(() -> c.observe(1, status(2, Role.LEADER, 3, 0, 0), tracked(log("x", 1, 1))))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Leader Completeness");
    }

    @Test
    void leaderCompletenessRejectsALaterLeaderWithADifferentEntryAtACommittedIndex() {
        LeaderCompletenessChecker c = new LeaderCompletenessChecker();
        c.observe(0, status(1, Role.LEADER, 2, 3, 3), tracked(log("x", 1, 1, 2)));
        assertThatThrownBy(() -> c.observe(1, status(2, Role.LEADER, 3, 0, 0), tracked(log("x", 1, 1, 3))))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Leader Completeness");
    }

    /** A stale leader of an older term need not hold entries committed in later terms. */
    @Test
    void leaderCompletenessIgnoresEntriesCommittedInLaterTerms() {
        LeaderCompletenessChecker c = new LeaderCompletenessChecker();
        c.observe(0, status(2, Role.LEADER, 5, 3, 3), tracked(log("x", 1, 1, 5)));
        assertThatCode(() -> c.observe(1, status(1, Role.LEADER, 4, 0, 0), tracked(log("x", 1, 1))))
                .doesNotThrowAnyException();
        assertThat(c.committedEntries()).isEqualTo(3);
    }

    @Test
    void trackerRecomputesOnlyFromTheFirstChangedIndex() {
        ListLog l = log("x", 1, 1, 2);
        NodeLogTracker t = tracked(l);
        long h2 = t.hashAt(2);
        l.entries.remove(2);
        l.entries.add(log("y", 1, 1, 3).entries.get(2));
        t.onTruncate(3);
        t.onAppend(3);

        NodeLogTracker.Change ch = t.refresh(l);

        assertThat(ch.fromIndex()).isEqualTo(3);
        assertThat(ch.truncated()).isTrue();
        assertThat(t.hashAt(2)).isEqualTo(h2);
        assertThat(t.hashAt(3)).isNotEqualTo(tracked(log("x", 1, 1, 2)).hashAt(3));
    }
}
