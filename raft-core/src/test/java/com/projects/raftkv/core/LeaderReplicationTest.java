package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Figure 2 "Rules for Servers / Leaders", nextIndex/matchIndex bookkeeping, and §5.3 / §8.
 * N1 becomes leader of term 2 with a recovered log of [1, 1] unless stated otherwise.
 */
class LeaderReplicationTest {

    private static final long TERM = 2;

    private static RaftNodeCore leader() {
        return leaderAt(N1, THREE, TERM, stateWithLog(1, 1, 1), List.of(N2));
    }

    /** §8: a new leader appends a no-op in its own term and sends it right away, prevLog = its old tail. */
    @Test
    void newLeaderAppendsAndBroadcastsANoOpInItsTerm() {
        PersistentState s = new PersistentState(new HardState(TERM - 1, java.util.Optional.empty()), entries(1, 1, 1));
        RaftNodeCore c = core(N1, THREE, s, 42);
        tickUntilElection(c);

        Ready won = receive(c, new Message.RequestVoteResponse(N2, N1, TERM, true));

        LogEntry noop = new LogEntry(3, TERM, new EntryPayload.Noop());
        assertThat(c.log().entryAt(3)).contains(noop);
        assertThat(won.persist()).containsExactly(new PersistOp.AppendEntries(List.of(noop)));
        assertThat(appendsTo(won, N2)).containsExactly(append(N1, N2, TERM, 2, 1, List.of(noop), 0));
        assertThat(appendsTo(won, N3)).containsExactly(append(N1, N3, TERM, 2, 1, List.of(noop), 0));
    }

    @Test
    void proposalIsAppendedPersistedAcceptedAndSentToEveryPeer() {
        RaftNodeCore l = leader();
        Bytes cmd = Bytes.copyOf(new byte[] {7});

        Ready r = l.step(new Event.ProposeCommand(5, cmd));

        LogEntry e = new LogEntry(4, TERM, new EntryPayload.Command(cmd));
        assertThat(l.log().entryAt(4)).contains(e);
        assertThat(r.persist()).containsExactly(new PersistOp.AppendEntries(List.of(e)));
        assertThat(r.notices()).containsExactly(new ClientNotice.ProposalAccepted(5, 4, TERM));
        // M2-A: nextIndex has not moved (no reply yet), so the batch still starts at the no-op.
        assertThat(appendsTo(r, N2)).singleElement().satisfies(ae -> {
            assertThat(ae.prevLogIndex()).isEqualTo(2);
            assertThat(ae.entries()).extracting(LogEntry::index).containsExactly(3L, 4L);
        });
        assertThat(appendsTo(r, N3)).hasSize(1);
        assertThat(r.toApply()).isEmpty();
    }

    /** Figure 2: commit N once a majority (counting the leader) has matchIndex >= N and log[N].term == currentTerm. */
    @Test
    void commitsOnceAMajorityHasACurrentTermEntryAndAppliesEverythingUpToIt() {
        RaftNodeCore l = leader();

        Ready r = receive(l, ok(N2, N1, TERM, 3));

        assertThat(l.status().commitIndex()).isEqualTo(3);
        assertThat(r.toApply()).extracting(LogEntry::index).containsExactly(1L, 2L, 3L);
        // Followers learn the new commitIndex on the next AppendEntries.
        List<Message> hb = tickN(l, HEARTBEAT);
        assertThat(ofType(hb, Message.AppendEntries.class)).allSatisfy(ae -> assertThat(ae.leaderCommit()).isEqualTo(3));
    }

    @Test
    void oneAckIsNotAMajorityOfFive() {
        RaftNodeCore l = leaderAt(N1, FIVE, TERM, stateWithLog(1, 1), List.of(N2, N3));

        receive(l, ok(N2, N1, TERM, 2));
        assertThat(l.status().commitIndex()).isZero();

        receive(l, ok(N4, N1, TERM, 2));
        assertThat(l.status().commitIndex()).isEqualTo(2);
    }

    /** Reordered or duplicated acks never move matchIndex backwards (matchIndex = max(old, new)). */
    @Test
    void reorderedAcksNeverMoveMatchIndexBackwards() {
        RaftNodeCore l = leader();
        l.step(new Event.ProposeCommand(1, Bytes.EMPTY));            // index 4
        receive(l, ok(N2, N1, TERM, 4));

        Ready late = receive(l, ok(N2, N1, TERM, 3));                 // an older ack arrives late

        assertThat(l.progressOf(N2)).contains(new Progress(5, 4));
        assertThat(late.messages()).isEmpty();
        assertThat(l.status().commitIndex()).isEqualTo(4);
    }

    /** A rejection that answers the latest probe lowers nextIndex by one and re-probes immediately. */
    @Test
    void rejectionLowersNextIndexAndRetriesAtOnce() {
        RaftNodeCore l = leader();                                    // nextIndex[N2] = 3, probe prevLogIndex = 2

        Ready r = receive(l, reject(N2, N1, TERM, 2));

        assertThat(l.progressOf(N2)).contains(new Progress(2, 0));
        assertThat(appendsTo(r, N2)).singleElement().satisfies(ae -> {
            assertThat(ae.prevLogIndex()).isEqualTo(1);
            assertThat(ae.prevLogTerm()).isEqualTo(1);
            assertThat(ae.entries()).extracting(LogEntry::index).containsExactly(2L, 3L);
        });
    }

    /**
     * Fast backup (thesis §5.3): the follower names the conflicting term and its first index for it.
     * The leader has entries of that term, so it skips to just after its own last entry of that term.
     */
    @Test
    void fastBackupJumpsPastTheLeadersLastEntryOfTheConflictingTerm() {
        // Leader log [1, 2, 2, 4, 4] + no-op (term 5) at 6. nextIndex[N2] = 6, probe prevLogIndex = 5.
        RaftNodeCore l = leaderAt(N1, THREE, 5, stateWithLog(4, 1, 2, 2, 4, 4), List.of(N2));

        receive(l, reject(N2, N1, 5, 5, 2, 2));          // follower: index 5 has term 2, which starts at 2

        assertThat(l.progressOf(N2)).contains(new Progress(4, 0));
    }

    /** The leader has no entry of the conflicting term: skip that whole term on the follower. */
    @Test
    void fastBackupSkipsATermTheLeaderDoesNotHave() {
        RaftNodeCore l = leaderAt(N1, THREE, 5, stateWithLog(4, 1, 2, 2, 4, 4), List.of(N2));

        receive(l, reject(N2, N1, 5, 5, 3, 3));          // follower: term 3 from index 3 on

        assertThat(l.progressOf(N2)).contains(new Progress(3, 0));
    }

    /** The follower's log is too short: resume right after its last entry. */
    @Test
    void fastBackupResumesAfterAShortFollowerLog() {
        RaftNodeCore l = leaderAt(N1, THREE, 5, stateWithLog(4, 1, 2, 2, 4, 4), List.of(N2));

        receive(l, reject(N2, N1, 5, 5, 0, 2));          // follower has only 1 entry

        assertThat(l.progressOf(N2)).contains(new Progress(2, 0));
    }

    /** A hint never moves nextIndex up, and never below matchIndex + 1. */
    @Test
    void fastBackupHintIsClamped() {
        RaftNodeCore l = leaderAt(N1, THREE, 5, stateWithLog(4, 1, 2, 2, 4, 4), List.of(N2));
        receive(l, ok(N2, N1, 5, 2));                     // match 2, next 6
        receive(l, reject(N2, N1, 5, 5, 0, 1));           // bogus hint below matchIndex

        assertThat(l.progressOf(N2)).contains(new Progress(3, 2));
    }

    /** A duplicated or delayed rejection for a probe we already moved past is ignored. */
    @Test
    void staleRejectionIsIgnored() {
        RaftNodeCore l = leader();
        receive(l, reject(N2, N1, TERM, 2));                           // nextIndex -> 2

        Ready dup = receive(l, reject(N2, N1, TERM, 2));               // same rejection again

        assertThat(l.progressOf(N2)).contains(new Progress(2, 0));
        assertThat(dup.messages()).isEmpty();
    }

    /** nextIndex never drops below matchIndex + 1: entries known to match are never re-probed. */
    @Test
    void nextIndexNeverDropsBelowMatchIndexPlusOne() {
        RaftNodeCore l = leader();
        receive(l, ok(N2, N1, TERM, 3));                               // match 3, next 4
        l.step(new Event.ProposeCommand(1, Bytes.EMPTY));              // index 4, probe prevLogIndex = 3

        receive(l, reject(N2, N1, TERM, 3));

        assertThat(l.progressOf(N2)).contains(new Progress(4, 3));
    }

    /** M2-A: heartbeats carry the entries a follower is missing, capped at maxEntriesPerAppend. */
    @Test
    void heartbeatsCarryMissingEntriesInBoundedBatches() {
        RaftConfig cfg = new RaftConfig(N1, THREE, T, HEARTBEAT, 2);
        PersistentState s = new PersistentState(new HardState(TERM - 1, java.util.Optional.empty()), entries(1, 1, 1, 1, 1));
        RaftNodeCore l = new RaftNodeCore(cfg, s, new java.util.SplittableRandom(42));
        tickUntilElection(l);
        receive(l, new Message.RequestVoteResponse(N2, N1, TERM, true));
        receive(l, reject(N2, N1, TERM, 4));                           // nextIndex[N2] -> 4
        receive(l, reject(N2, N1, TERM, 3));                           // nextIndex[N2] -> 3

        List<Message.AppendEntries> hb = ofType(tickN(l, HEARTBEAT), Message.AppendEntries.class)
                .stream().filter(m -> m.to().equals(N2)).toList();

        assertThat(hb).singleElement().satisfies(ae -> {
            assertThat(ae.prevLogIndex()).isEqualTo(2);
            assertThat(ae.entries()).extracting(LogEntry::index).containsExactly(3L, 4L);
        });
    }

    /** An ack that brings a lagging follower forward triggers the next batch immediately. */
    @Test
    void progressingAckSendsTheNextBatch() {
        RaftNodeCore l = leader();
        for (int i = 0; i < 3; i++) l.step(new Event.ProposeCommand(i, Bytes.EMPTY)); // indexes 4..6

        Ready r = receive(l, ok(N2, N1, TERM, 4));

        assertThat(appendsTo(r, N2)).singleElement().satisfies(ae -> {
            assertThat(ae.prevLogIndex()).isEqualTo(4);
            assertThat(ae.entries()).extracting(LogEntry::index).containsExactly(5L, 6L);
        });
    }

    @Test
    void responsesFromAnOlderTermAreIgnored() {
        RaftNodeCore l = leader();

        Ready r = receive(l, ok(N2, N1, TERM - 1, 3));

        assertThat(r.isEmpty()).isTrue();
        assertThat(l.progressOf(N2)).contains(new Progress(3, 0));
    }

    /** Leader Append-Only: stepping down and later truncating is a follower action, never a leader one. */
    @Test
    void leaderNeverTruncatesItsOwnLog() {
        RaftNodeCore l = leader();
        l.step(new Event.ProposeCommand(1, Bytes.EMPTY));
        List<Long> before = terms(l.log());

        receive(l, reject(N2, N1, TERM, 3));
        receive(l, ok(N3, N1, TERM, 1));
        tickN(l, 3 * HEARTBEAT);

        assertThat(terms(l.log())).isEqualTo(before);
    }
}
