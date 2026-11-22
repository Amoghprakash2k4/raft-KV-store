package com.projects.raftkv.node;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import java.util.Objects;
import java.util.Optional;

/** How a client proposal ended, as reported by the node that received it. */
public sealed interface ProposalOutcome {
    long proposalId();

    /** The proposal's entry was committed and applied at {@code index}; {@code result} is the state machine's reply. */
    record Committed(long proposalId, long index, Bytes result) implements ProposalOutcome {
        public Committed { Objects.requireNonNull(result); }
    }

    /** This node is not the leader. Retry at {@code leaderHint} if known (thesis §6.2). */
    record Rejected(long proposalId, Optional<NodeId> leaderHint) implements ProposalOutcome {
        public Rejected { Objects.requireNonNull(leaderHint); }
    }

    /**
     * The entry was placed at {@code index} but a different entry was committed there: a new leader
     * overwrote it (thesis §6.3). Definitely not applied, so retrying is safe.
     */
    record Lost(long proposalId, long index) implements ProposalOutcome {}
}
