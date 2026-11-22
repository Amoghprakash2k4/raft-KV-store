package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Figure 2 "AppendEntries RPC, receiver implementation", steps 2-5, and §5.3. The follower is N1;
 * the leader N2 is in term 3 throughout.
 */
class AppendEntriesFollowerTest {

    private static final long TERM = 3;

    /** Step 4: append entries not already in the log, persist them, ack with the last index they cover. */
    @Test
    void appendsNewEntriesPersistsThemAndAcksMatchIndex() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 1), 1);
        List<LogEntry> sent = entries(3, 2, 3);

        Ready r = receive(f, append(N2, N1, TERM, 2, 1, sent, 0));

        assertThat(terms(f.log())).isEqualTo(termList(1, 1, 2, 3));
        assertThat(r.persist()).containsExactly(new PersistOp.AppendEntries(sent));
        assertThat(r.messages()).containsExactly(ok(N1, N2, TERM, 4));
    }

    /**
     * Step 2: reply false if the log has no entry at prevLogIndex. The rejection echoes prevLogIndex and
     * hints (fast backup) that the log ends early: conflictTerm = 0, conflictIndex = lastIndex + 1.
     */
    @Test
    void rejectsWhenPrevLogIndexIsBeyondTheLog() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1), 1);

        Ready r = receive(f, append(N2, N1, TERM, 5, 3, entries(6, 3), 0));

        assertThat(r.messages()).containsExactly(reject(N1, N2, TERM, 5, 0, 2));
        assertThat(r.persist()).isEmpty();
        assertThat(f.log().lastIndex()).isEqualTo(1);
    }

    /**
     * Step 2: reply false if the entry at prevLogIndex has a different term. Nothing is deleted yet.
     * Fast backup hint: the conflicting term and the first index we hold for it.
     */
    @Test
    void rejectsWhenTermAtPrevLogIndexDiffers() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 2, 2, 2), 1);

        Ready r = receive(f, append(N2, N1, TERM, 4, 3, entries(5, 3), 0));

        assertThat(r.messages()).containsExactly(reject(N1, N2, TERM, 4, 2, 2));
        assertThat(terms(f.log())).isEqualTo(termList(1, 2, 2, 2));
    }

    /** Step 3: an existing entry that conflicts (same index, different term) is deleted with all that follow. */
    @Test
    void truncatesFromTheFirstRealConflictOnly() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 1, 2, 2), 1);
        List<LogEntry> sent = entries(2, 1, 3, 3);   // index 2 matches, index 3 conflicts

        Ready r = receive(f, append(N2, N1, TERM, 1, 1, sent, 0));

        assertThat(terms(f.log())).isEqualTo(termList(1, 1, 3, 3));
        assertThat(f.log().entryAt(2)).contains(sent.get(0));
        assertThat(r.persist()).containsExactly(
                new PersistOp.TruncateSuffix(3), new PersistOp.AppendEntries(sent.subList(1, 3)));
        assertThat(r.messages()).containsExactly(ok(N1, N2, TERM, 4));
    }

    /** Idempotence: a duplicated AppendEntries changes nothing and writes nothing. */
    @Test
    void duplicateAppendEntriesIsANoOp() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1), 1);
        Message.AppendEntries ae = append(N2, N1, TERM, 1, 1, entries(2, 3, 3), 0);
        receive(f, ae);

        Ready again = receive(f, ae);

        assertThat(again.persist()).isEmpty();
        assertThat(again.messages()).containsExactly(ok(N1, N2, TERM, 3));
        assertThat(terms(f.log())).isEqualTo(termList(1, 3, 3));
    }

    /**
     * §5.3 / the brief: an old AppendEntries delivered late must not truncate entries that already
     * match. Here entries 2-4 are present; a reordered message carrying only entry 2 arrives.
     */
    @Test
    void reorderedOlderAppendEntriesNeverTruncatesMatchingEntries() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1), 1);
        List<LogEntry> all = entries(2, 3, 3, 3);
        receive(f, append(N2, N1, TERM, 1, 1, all, 0));

        Ready late = receive(f, append(N2, N1, TERM, 1, 1, all.subList(0, 1), 0));

        assertThat(late.persist()).isEmpty();
        assertThat(terms(f.log())).isEqualTo(termList(1, 3, 3, 3));
        // It acks only what that message verified, never more.
        assertThat(late.messages()).containsExactly(ok(N1, N2, TERM, 2));
    }

    /** Step 5: commitIndex = min(leaderCommit, index of last new entry), and committed entries are applied. */
    @Test
    void commitIndexIsCappedByTheLastEntryThisMessageVerified() {
        // The follower has stale extra entries (term 2) beyond what the leader will verify.
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 1, 2, 2), 1);

        Ready r = receive(f, append(N2, N1, TERM, 2, 1, List.of(), 4));

        assertThat(f.status().commitIndex()).isEqualTo(2);
        assertThat(r.toApply()).extracting(LogEntry::index).containsExactly(1L, 2L);
        assertThat(f.status().lastApplied()).isEqualTo(2);
    }

    @Test
    void commitIndexNeverDecreasesAndEntriesAreAppliedExactlyOnce() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 1, 1), 1);
        Ready first = receive(f, append(N2, N1, TERM, 3, 1, List.of(), 3));

        Ready stale = receive(f, append(N2, N1, TERM, 3, 1, List.of(), 1));

        assertThat(first.toApply()).extracting(LogEntry::index).containsExactly(1L, 2L, 3L);
        assertThat(stale.toApply()).isEmpty();
        assertThat(f.status().commitIndex()).isEqualTo(3);
    }

    /** A rejected AppendEntries still comes from the current leader: it resets the timer and names the leader. */
    @Test
    void rejectedButCurrentAppendEntriesStillCountsAsLeaderContact() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM), 1);
        for (int i = 0; i < 5; i++) {
            tickN(f, T - 1);
            receive(f, append(N2, N1, TERM, 7, 3, List.of(), 0));
        }
        assertThat(f.status().soft().role()).isEqualTo(Role.FOLLOWER);
        assertThat(f.status().soft().leader()).contains(N2);
    }

    /** Deleting a committed entry would break State Machine Safety; fail loudly instead of doing it. */
    @Test
    void conflictAtACommittedIndexIsAFatalSafetyViolation() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM, 1, 2), 1);
        receive(f, append(N2, N1, TERM, 2, 2, List.of(), 2));   // both entries now committed

        assertThatThrownBy(() -> receive(f, append(N2, N1, TERM, 1, 1, entries(2, 3), 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("State Machine Safety");
    }

    /** A follower does not accept proposals; it points the client at the leader it knows (thesis §6.2). */
    @Test
    void proposalToFollowerIsRejectedWithLeaderHint() {
        RaftNodeCore f = core(N1, THREE, stateWithLog(TERM), 1);
        receive(f, append(N2, N1, TERM, 0, 0, List.of(), 0));

        Ready r = f.step(new Event.ProposeCommand(77, Bytes.EMPTY));

        assertThat(r.notices()).containsExactly(new ClientNotice.ProposalRejected(77, java.util.Optional.of(N2)));
        assertThat(r.persist()).isEmpty();
        assertThat(f.log().lastIndex()).isZero();
    }
}
