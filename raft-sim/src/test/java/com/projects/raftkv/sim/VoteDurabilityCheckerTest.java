package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.SoftState;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class VoteDurabilityCheckerTest {

    @Test
    void allowsVotingOnceThenMovingToHigherTerms() {
        VoteDurabilityChecker c = new VoteDurabilityChecker();
        assertThatCode(() -> {
            c.observe(0, status(5, null));
            c.observe(1, status(5, 2));
            c.observe(2, status(5, 2));
            c.observe(3, status(6, null));
            c.observe(4, status(7, 1));
        }).doesNotThrowAnyException();
    }

    @Test
    void rejectsChangedVoteInSameTerm() {
        VoteDurabilityChecker c = new VoteDurabilityChecker();
        c.observe(0, status(5, 2));
        assertThatThrownBy(() -> c.observe(1, status(5, 3))).isInstanceOf(InvariantViolation.class);
    }

    /** What a restart with a lost vote looks like. */
    @Test
    void rejectsForgottenVoteInSameTerm() {
        VoteDurabilityChecker c = new VoteDurabilityChecker();
        c.observe(0, status(5, 2));
        assertThatThrownBy(() -> c.observe(1, status(5, null))).isInstanceOf(InvariantViolation.class);
    }

    @Test
    void rejectsTermGoingBackwards() {
        VoteDurabilityChecker c = new VoteDurabilityChecker();
        c.observe(0, status(5, null));
        assertThatThrownBy(() -> c.observe(1, status(4, null))).isInstanceOf(InvariantViolation.class);
    }

    private static RaftStatus status(long term, Integer votedFor) {
        NodeId n1 = new NodeId(1);
        return new RaftStatus(n1, new SoftState(Role.FOLLOWER, term, Optional.empty()),
                new HardState(term, Optional.ofNullable(votedFor).map(NodeId::new)), 0, 0);
    }
}
