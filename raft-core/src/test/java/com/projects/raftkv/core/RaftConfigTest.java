package com.projects.raftkv.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftConfigTest {

    private static final List<NodeId> THREE = List.of(new NodeId(1), new NodeId(2), new NodeId(3));

    @Test
    void quorumIsStrictMajority() {
        assertThat(config(THREE).quorum()).isEqualTo(2);
        List<NodeId> five = List.of(new NodeId(1), new NodeId(2), new NodeId(3), new NodeId(4), new NodeId(5));
        assertThat(config(five).quorum()).isEqualTo(3);
    }

    @Test
    void peersExcludeSelf() {
        assertThat(config(THREE).peers()).containsExactly(new NodeId(2), new NodeId(3));
    }

    @Test
    void rejectsSelfNotInMembers() {
        assertThatThrownBy(() -> new RaftConfig(new NodeId(9), THREE, 10, 2, 64))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDuplicateMembers() {
        List<NodeId> dup = List.of(new NodeId(1), new NodeId(1), new NodeId(2));
        assertThatThrownBy(() -> config(dup)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnsupportedClusterSize() {
        assertThatThrownBy(() -> config(List.of(new NodeId(1), new NodeId(2))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsHeartbeatNotWellBelowElectionTimeout() {
        assertThatThrownBy(() -> new RaftConfig(new NodeId(1), THREE, 4, 2, 64))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RaftConfig config(List<NodeId> members) {
        return new RaftConfig(new NodeId(1), members, 10, 2, 64);
    }
}
