package com.projects.raftkv.sim;

import static com.projects.raftkv.sim.LogFixtures.log;
import static com.projects.raftkv.sim.LogFixtures.status;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.HardState;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.PersistentState;
import com.projects.raftkv.core.Role;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Invariant 6, Durability: what a node synced is what it recovers, and commits rest on a durable majority. */
class DurabilityCheckerTest {

    private static final NodeId N1 = new NodeId(1), N2 = new NodeId(2), N3 = new NodeId(3);
    private static final HardState T2 = new HardState(2, Optional.empty());
    private static final List<LogEntry> E = log("x", 1, 2, 2).entries;

    private static DurabilityChecker checker() { return new DurabilityChecker(List.of(N1, N2, N3), true); }

    private static PersistentState state(HardState hs, int entries) {
        return new PersistentState(hs, E.subList(0, entries));
    }

    @Test
    void recoveringExactlyWhatWasSyncedIsFine() {
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, T2);
        c.onAppend(N1, E.subList(0, 2));
        c.onSync(N1);
        assertThatCode(() -> c.onRecover(0, N1, state(T2, 2))).doesNotThrowAnyException();
    }

    @Test
    void losingSyncedEntriesIsAViolation() {
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, T2);
        c.onAppend(N1, E.subList(0, 2));
        c.onSync(N1);
        assertThatThrownBy(() -> c.onRecover(0, N1, state(T2, 1)))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Durability");
    }

    @Test
    void losingASyncedVoteIsAViolation() {
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, new HardState(3, Optional.of(N2)));
        c.onSync(N1);
        assertThatThrownBy(() -> c.onRecover(0, N1, new PersistentState(new HardState(3, Optional.empty()), List.of())))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Durability");
    }

    @Test
    void anyPrefixOfTheUnsyncedWritesMaySurviveACrash() {
        for (int survived = 0; survived <= 2; survived++) {
            HardState t1 = new HardState(1, Optional.empty());
            DurabilityChecker c = checker();
            c.onSaveHardState(N1, t1);
            c.onAppend(N1, E.subList(0, 1));
            c.onSync(N1);
            c.onSaveHardState(N1, T2);            // pending op 1
            c.onAppend(N1, E.subList(1, 3));      // pending op 2
            PersistentState recovered = switch (survived) {
                case 0 -> new PersistentState(t1, E.subList(0, 1));
                case 1 -> new PersistentState(T2, E.subList(0, 1));
                default -> state(T2, 3);
            };
            assertThatCode(() -> c.onRecover(0, N1, recovered)).doesNotThrowAnyException();
        }
    }

    @Test
    void unsyncedWritesCannotSurviveOutOfOrder() {
        HardState t3VotedN2 = new HardState(3, Optional.of(N2));
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, T2);
        c.onAppend(N1, E.subList(0, 1));
        c.onSync(N1);
        c.onAppend(N1, E.subList(1, 3));          // pending op 1
        c.onSaveHardState(N1, t3VotedN2);         // pending op 2
        // Op 2 survived but op 1, written before it, did not: impossible for an append-only log.
        assertThatThrownBy(() -> c.onRecover(0, N1, new PersistentState(t3VotedN2, E.subList(0, 1))))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("Durability");
    }

    @Test
    void aCommitHeldDurablyByAMajorityIsFine() {
        DurabilityChecker c = checker();
        for (NodeId n : List.of(N1, N2)) {
            c.onAppend(n, E.subList(0, 2));
            c.onSync(n);
        }
        assertThatCode(() -> c.observeCommit(0, status(1, Role.LEADER, 2, 2, 0), log("x", 1, 2, 2)))
                .doesNotThrowAnyException();
    }

    @Test
    void aCommitNotYetDurableOnAMajorityIsAViolation() {
        DurabilityChecker c = checker();
        c.onAppend(N1, E.subList(0, 2));
        c.onSync(N1);
        c.onAppend(N2, E.subList(0, 2));          // written but not fsynced: does not count
        assertThatThrownBy(() -> c.observeCommit(0, status(1, Role.LEADER, 2, 2, 0), log("x", 1, 2, 2)))
                .isInstanceOf(InvariantViolation.class).hasMessageContaining("majority");
    }

    @Test
    void truncationIsPartOfTheModel() {
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, T2);
        c.onAppend(N1, E.subList(0, 3));
        c.onTruncate(N1, 2);
        c.onSync(N1);
        assertThatThrownBy(() -> c.onRecover(0, N1, state(T2, 3))).isInstanceOf(InvariantViolation.class);
        DurabilityChecker c2 = checker();
        c2.onSaveHardState(N1, T2);
        c2.onAppend(N1, E.subList(0, 3));
        c2.onTruncate(N1, 2);
        c2.onSync(N1);
        assertThatCode(() -> c2.onRecover(0, N1, state(T2, 1))).doesNotThrowAnyException();
    }

    /** (c): a node may not durably delete an entry once it is committed, except under a snapshot. */
    @Test
    void durablyDeletingACommittedEntryIsAViolation() {
        DurabilityChecker c = checker();
        for (NodeId n : List.of(N1, N2)) {
            c.onSaveHardState(n, T2);
            c.onAppend(n, E.subList(0, 3));
            c.onSync(n);
        }
        c.observeCommit(0, status(1, Role.LEADER, 2, 3, 0), log("x", 1, 2, 2));
        c.onTruncate(N2, 3);
        assertThatThrownBy(() -> c.onSync(N2)).isInstanceOf(InvariantViolation.class).hasMessageContaining("committed entry 3");
    }

    /** Figure 8: a node still in an older term was not in the committing majority and may lose the entry. */
    @Test
    void aNodeInAnOlderTermMayLoseAnEntryCommittedInALaterTerm() {
        DurabilityChecker c = checker();
        for (NodeId n : List.of(N1, N2)) {
            c.onSaveHardState(n, T2);
            c.onAppend(n, E.subList(0, 3));
            c.onSync(n);
        }
        c.onSaveHardState(N3, new com.projects.raftkv.core.HardState(1, java.util.Optional.empty()));
        c.onAppend(N3, E.subList(0, 3));
        c.onSync(N3);
        c.observeCommit(0, status(1, Role.LEADER, 2, 3, 0), log("x", 1, 2, 2));
        c.onTruncate(N3, 3);
        assertThatCode(() -> c.onSync(N3)).doesNotThrowAnyException();
    }

    @Test
    void coveringCommittedEntriesWithASnapshotIsFine() {
        DurabilityChecker c = checker();
        c.onSaveHardState(N1, T2);
        c.onAppend(N1, E.subList(0, 3));
        c.onSync(N1);
        c.observeCommit(0, status(1, Role.LEADER, 2, 0, 0), log("x", 1, 2, 2));
        c.onSnapshotSaved(N1, new com.projects.raftkv.core.Snapshot(2, 2, com.projects.raftkv.core.Bytes.EMPTY));
        c.onCompactLog(N1, 2, 2);
        assertThatCode(() -> c.onSync(N1)).doesNotThrowAnyException();
    }
}
