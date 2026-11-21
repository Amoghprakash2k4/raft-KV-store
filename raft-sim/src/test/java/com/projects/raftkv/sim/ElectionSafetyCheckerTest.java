package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.core.SoftState;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ElectionSafetyCheckerTest {

    @Test
    void allowsOneLeaderPerTermAcrossManyTerms() {
        ElectionSafetyChecker c = new ElectionSafetyChecker();
        c.observe(0, leader(1, 1));
        c.observe(1, leader(1, 1));
        c.observe(2, leader(2, 2));
        c.observe(3, follower(3, 2));
        assertThat(c.leadersElected()).isEqualTo(2);
    }

    /** Checked over the whole history, not just "right now": the first leader may already be gone. */
    @Test
    void rejectsSecondLeaderInSameTermEvenAfterFirstSteppedDown() {
        ElectionSafetyChecker c = new ElectionSafetyChecker();
        c.observe(0, leader(1, 4));
        c.observe(5, follower(1, 5));
        assertThatThrownBy(() -> c.observe(9, leader(2, 4)))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("Election Safety");
    }

    static RaftStatus leader(int id, long term) {
        NodeId n = new NodeId(id);
        return new RaftStatus(n, new SoftState(Role.LEADER, term, Optional.of(n)),
                new HardState(term, Optional.of(n)), 0, 0);
    }

    static RaftStatus follower(int id, long term) {
        return new RaftStatus(new NodeId(id), new SoftState(Role.FOLLOWER, term, Optional.empty()),
                new HardState(term, Optional.empty()), 0, 0);
    }
}
