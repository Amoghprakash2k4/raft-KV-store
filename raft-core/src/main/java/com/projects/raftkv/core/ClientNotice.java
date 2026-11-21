package com.projects.raftkv.core;

import java.util.Optional;

/** Outcomes the driver relays to waiting clients. */
public sealed interface ClientNotice {
    /**
     * Entry placed at (index, term). NOT a commit. The driver must check at apply time that the
     * applied entry at {@code index} still has {@code term}; if not, the proposal was overwritten.
     */
    record ProposalAccepted(long proposalId, long index, long term) implements ClientNotice {}

    record ProposalRejected(long proposalId, Optional<NodeId> leaderHint) implements ClientNotice {}

    /** Leadership confirmed for readIndex; serve once lastApplied >= readIndex. */
    record ReadReady(long readId, long readIndex) implements ClientNotice {}

    record ReadRejected(long readId, Optional<NodeId> leaderHint) implements ClientNotice {}
}
