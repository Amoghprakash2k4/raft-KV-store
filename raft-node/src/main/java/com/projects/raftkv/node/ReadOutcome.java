package com.projects.raftkv.node;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import java.util.Objects;
import java.util.Optional;

/** How a ReadIndex read (thesis §6.4) ended, as reported by the node that received it. */
public sealed interface ReadOutcome {
    long readId();

    /**
     * Leadership was confirmed for {@code readIndex} and the state machine, having applied at least that
     * far, answered {@code result}. Linearizable: it reflects every write committed before the read began.
     */
    record Ok(long readId, long readIndex, Bytes result) implements ReadOutcome {
        public Ok { Objects.requireNonNull(result); }
    }

    /** This node is not (or stopped being) the leader. Retry at {@code leaderHint} if known. */
    record Rejected(long readId, Optional<NodeId> leaderHint) implements ReadOutcome {
        public Rejected { Objects.requireNonNull(leaderHint); }
    }
}
