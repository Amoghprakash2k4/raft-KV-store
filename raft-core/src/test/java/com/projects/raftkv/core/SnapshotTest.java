package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/** Log compaction and InstallSnapshot (§7, Figure 13) and the boundary rules of DESIGN.md D1a. */
class SnapshotTest {

    static final int CHUNK = 64;

    static Bytes data(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) b[i] = (byte) (i * 7 + 3);
        return Bytes.copyOf(b);
    }

    static RaftNodeCore core(NodeId self, PersistentState s) {
        return new RaftNodeCore(new RaftConfig(self, THREE, T, HEARTBEAT, 64, CHUNK), s, new SplittableRandom(42));
    }

    static PersistentState withSnapshot(long term, Snapshot snap, long... entryTerms) {
        return new PersistentState(new HardState(term, Optional.empty()), Optional.of(snap), entries(snap.index() + 1, entryTerms));
    }

    static Message.InstallSnapshot chunk(long term, Snapshot s, int offset, int len) {
        byte[] all = s.data().toByteArray();
        int end = Math.min(all.length, offset + len);
        return new Message.InstallSnapshot(N1, N2, term, s.index(), s.term(), offset,
                Bytes.copyOf(java.util.Arrays.copyOfRange(all, offset, end)), end == all.length);
    }

    static Message.InstallSnapshotResponse onlyResponse(Ready r) {
        List<Message.InstallSnapshotResponse> rs = ofType(r.messages(), Message.InstallSnapshotResponse.class);
        assertThat(rs).hasSize(1);
        return rs.get(0);
    }

    // ---------------------------------------------------------------- the log base (D1a rules 2, 6)

    @Test
    void theSnapshotBoundaryHasATermAndEntriesBelowItAreGone() {
        RaftLog log = new RaftLog(5, 2, entries(6, 2, 3));
        assertThat(log.lastIndex()).isEqualTo(7);
        assertThat(log.termAt(5)).hasValue(2);
        assertThat(log.termAt(4)).isEmpty();
        assertThat(log.entryAt(5)).isEmpty();
        assertThat(log.entryAt(6)).isPresent();
        assertThat(log.lastIndexOfTerm(2)).isEqualTo(6);
        assertThat(log.firstIndexOfTermAt(6)).isEqualTo(6);
        assertThatThrownBy(() -> log.truncateFrom(5)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void resetBaseKeepsTheSuffixAfterAMatchingEntryAndDiscardsOtherwise() {
        RaftLog keep = new RaftLog(0, 0, entries(1, 1, 1, 2, 2));
        assertThat(keep.resetBase(3, 2)).isTrue();
        assertThat(keep.snapshotIndex()).isEqualTo(3);
        assertThat(keep.lastIndex()).isEqualTo(4);
        assertThat(keep.termAt(4)).hasValue(2);

        RaftLog discard = new RaftLog(0, 0, entries(1, 1, 1, 2, 2));
        assertThat(discard.resetBase(3, 3)).isFalse();    // we hold index 3, but with term 2
        assertThat(discard.lastIndex()).isEqualTo(3);
        assertThat(discard.lastTerm()).isEqualTo(3);
    }

    @Test
    void recoveryFromASnapshotStartsWithEverythingInItCommittedAndApplied() {
        RaftNodeCore c = core(N2, withSnapshot(3, new Snapshot(10, 2, data(5)), 2, 3));
        assertThat(c.status().commitIndex()).isEqualTo(10);
        assertThat(c.status().lastApplied()).isEqualTo(10);
        assertThat(c.log().lastIndex()).isEqualTo(12);
        assertThat(c.snapshot()).hasValue(new Snapshot(10, 2, data(5)));
    }

    @Test
    void persistentStateRequiresTheLogToContinueTheSnapshot() {
        assertThatThrownBy(() -> new PersistentState(new HardState(3, Optional.empty()),
                Optional.of(new Snapshot(10, 2, data(1))), entries(10, 2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PersistentState(new HardState(3, Optional.empty()),
                Optional.of(new Snapshot(10, 3, data(1))), entries(11, 2)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- local compaction (D1a rule 1)

    @Test
    void aSnapshotOfAppliedStateCompactsTheLog() {
        RaftNodeCore follower = core(N2, stateWithLog(1));
        receive(follower, append(N1, N2, 1, 0, 0, entries(1, 1, 1, 1, 1), 3));   // commits and applies 1..3
        assertThat(follower.status().lastApplied()).isEqualTo(3);

        Ready r = follower.step(new Event.SnapshotCreated(3, 1, data(9)));

        assertThat(r.persist()).containsExactly(new PersistOp.CompactLog(3, 1));
        assertThat(follower.log().snapshotIndex()).isEqualTo(3);
        assertThat(follower.log().entryAt(3)).isEmpty();
        assertThat(follower.log().lastIndex()).isEqualTo(4);
        assertThat(follower.snapshot()).hasValue(new Snapshot(3, 1, data(9)));
    }

    @Test
    void aSnapshotBeyondLastAppliedIsAProgrammingError() {
        RaftNodeCore follower = core(N2, stateWithLog(1, 1, 1));
        assertThatThrownBy(() -> follower.step(new Event.SnapshotCreated(2, 1, data(1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anOlderSnapshotThanTheCurrentBaseIsIgnored() {
        RaftNodeCore c = core(N2, withSnapshot(2, new Snapshot(10, 2, data(5)), 2));
        Ready r = c.step(new Event.SnapshotCreated(8, 2, data(3)));
        assertThat(r.persist()).isEmpty();
        assertThat(c.log().snapshotIndex()).isEqualTo(10);
    }

    // ---------------------------------------------------------------- AppendEntries below the base (D1a rule 3)

    @Test
    void anAppendEntriesStartingInsideTheSnapshotIsTrimmedNotRejected() {
        RaftNodeCore follower = core(N2, withSnapshot(1, new Snapshot(5, 1, data(5))));
        Ready r = receive(follower, append(N1, N2, 1, 3, 1, entries(4, 1, 1, 1, 1), 7));

        List<Message.AppendEntriesResponse> rs = ofType(r.messages(), Message.AppendEntriesResponse.class);
        assertThat(rs).singleElement().satisfies(x -> {
            assertThat(x.success()).isTrue();
            assertThat(x.matchIndex()).isEqualTo(7);
        });
        assertThat(follower.log().snapshotIndex()).isEqualTo(5);
        assertThat(follower.log().lastIndex()).isEqualTo(7);
        assertThat(r.persist()).containsExactly(new PersistOp.AppendEntries(entries(6, 1, 1)));
        assertThat(follower.status().commitIndex()).isEqualTo(7);
    }

    @Test
    void anAppendEntriesEntirelyInsideTheSnapshotAcksTheSnapshotIndex() {
        RaftNodeCore follower = core(N2, withSnapshot(1, new Snapshot(5, 1, data(5))));
        Ready r = receive(follower, append(N1, N2, 1, 1, 1, entries(2, 1, 1), 3));
        assertThat(ofType(r.messages(), Message.AppendEntriesResponse.class))
                .singleElement().satisfies(x -> assertThat(x.matchIndex()).isEqualTo(5));
        assertThat(r.persist()).isEmpty();
    }

    // ---------------------------------------------------------------- InstallSnapshot receiver (Figure 13)

    @Test
    void chunksAreAssembledInOrderAndTheSnapshotInstalledWhenDone() {
        Snapshot s = new Snapshot(8, 1, data(150));                  // 3 chunks of 64
        RaftNodeCore follower = core(N2, stateWithLog(1, 1, 1));
        assertThat(onlyResponse(receive(follower, chunk(1, s, 0, CHUNK))).nextOffset()).isEqualTo(64);
        assertThat(onlyResponse(receive(follower, chunk(1, s, 64, CHUNK))).nextOffset()).isEqualTo(128);
        Ready last = receive(follower, chunk(1, s, 128, CHUNK));

        assertThat(onlyResponse(last).done()).isTrue();
        assertThat(last.persist()).containsExactly(new PersistOp.InstallSnapshot(s));
        assertThat(last.snapshotToRestore()).hasValue(s);
        assertThat(last.toApply()).isEmpty();
        assertThat(follower.log().snapshotIndex()).isEqualTo(8);
        assertThat(follower.log().lastIndex()).isEqualTo(8);          // short log, no entry 8: discarded
        assertThat(follower.status().commitIndex()).isEqualTo(8);
        assertThat(follower.status().lastApplied()).isEqualTo(8);
    }

    @Test
    void aChunkAtTheWrongOffsetIsAnsweredWithTheOffsetNeeded() {
        Snapshot s = new Snapshot(8, 1, data(150));
        RaftNodeCore follower = core(N2, stateWithLog(1));
        Message.InstallSnapshotResponse first = onlyResponse(receive(follower, chunk(1, s, 64, CHUNK)));
        assertThat(first.nextOffset()).isZero();
        assertThat(first.done()).isFalse();
        receive(follower, chunk(1, s, 0, CHUNK));
        assertThat(onlyResponse(receive(follower, chunk(1, s, 0, CHUNK))).nextOffset()).isEqualTo(64);   // duplicate
    }

    @Test
    void installingKeepsTheLogSuffixAfterAMatchingEntry() {
        Snapshot s = new Snapshot(5, 1, data(10));
        RaftNodeCore follower = core(N2, stateWithLog(2, 1, 1, 1, 1, 1, 2, 2, 2));
        Ready r = receive(follower, chunk(2, s, 0, CHUNK));
        assertThat(r.snapshotToRestore()).hasValue(s);
        assertThat(follower.log().snapshotIndex()).isEqualTo(5);
        assertThat(follower.log().lastIndex()).isEqualTo(8);
        assertThat(List.of(follower.log().termAt(6), follower.log().termAt(7), follower.log().termAt(8)))
                .allSatisfy(t -> assertThat(t).hasValue(2));
    }

    @Test
    void installingDiscardsALogThatDoesNotHoldTheSnapshotsLastEntry() {
        Snapshot s = new Snapshot(5, 2, data(10));
        RaftNodeCore follower = core(N2, stateWithLog(2, 1, 1, 1, 1, 1, 1, 1));
        receive(follower, chunk(2, s, 0, CHUNK));
        assertThat(follower.log().snapshotIndex()).isEqualTo(5);
        assertThat(follower.log().lastIndex()).isEqualTo(5);
    }

    @Test
    void aSnapshotTheFollowerHasAlreadyCommittedPastChangesNothing() {
        RaftNodeCore follower = core(N2, withSnapshot(1, new Snapshot(6, 1, data(5)), 1, 1));
        Ready r = receive(follower, chunk(1, new Snapshot(4, 1, data(3)), 0, CHUNK));
        assertThat(onlyResponse(r).done()).isTrue();
        assertThat(r.persist()).isEmpty();
        assertThat(r.snapshotToRestore()).isEmpty();
        assertThat(follower.log().snapshotIndex()).isEqualTo(6);
        assertThat(follower.log().lastIndex()).isEqualTo(8);
    }

    @Test
    void anInstallSnapshotFromAStaleTermIsRejected() {
        RaftNodeCore follower = core(N2, stateWithLog(5));
        Ready r = receive(follower, chunk(4, new Snapshot(3, 1, data(3)), 0, CHUNK));
        assertThat(onlyResponse(r).term()).isEqualTo(5);
        assertThat(onlyResponse(r).done()).isFalse();
        assertThat(r.persist()).isEmpty();
    }

    // ---------------------------------------------------------------- leader side (D1a rule 4) end to end

    @Test
    void aFollowerBehindTheLeadersSnapshotCatchesUpThroughInstallSnapshot() {
        Snapshot s = new Snapshot(10, 1, data(200));                   // 4 chunks
        RaftNodeCore leader = core(N1, withSnapshot(1, s, 1, 1));       // entries 11, 12
        RaftNodeCore follower = core(N2, PersistentState.EMPTY);
        MiniCluster c = new MiniCluster().add(leader).add(follower).isolate(N3);
        tickUntilElection(leader);
        c.enqueue(receive(leader, new Message.RequestVoteResponse(N2, N1, 2, true)));
        c.deliverAll();
        for (int i = 0; i < HEARTBEAT; i++) c.tickAll();               // a heartbeat carries the new commit index

        assertThat(follower.snapshot()).hasValue(s);
        assertThat(follower.log().snapshotIndex()).isEqualTo(10);
        assertThat(follower.log().lastIndex()).isEqualTo(13);          // 11, 12 and the leader's no-op
        assertThat(leader.status().commitIndex()).isEqualTo(13);
        assertThat(follower.status().commitIndex()).isEqualTo(13);
        assertThat(leader.progressOf(N2)).hasValue(new Progress(14, 13));

        List<Snapshot> restored = new ArrayList<>();
        for (Ready r : c.readiesOf(N2)) r.snapshotToRestore().ifPresent(restored::add);
        assertThat(restored).containsExactly(s);
    }

    @Test
    void aHeartbeatRetransmitsTheCurrentChunkWhenAReplyIsLost() {
        Snapshot s = new Snapshot(10, 1, data(100));
        RaftNodeCore leader = core(N1, withSnapshot(1, s));
        tickUntilElection(leader);
        receive(leader, new Message.RequestVoteResponse(N2, N1, 2, true));
        receive(leader, reject(N2, N1, 2, 10, 0, 1));                 // N2's log is empty: nextIndex -> 1
        List<Message.InstallSnapshot> sent = new ArrayList<>();
        for (int i = 0; i < HEARTBEAT; i++) sent.addAll(ofType(tick(leader).messages(), Message.InstallSnapshot.class));
        assertThat(sent).filteredOn(m -> m.to().equals(N2)).isNotEmpty()
                .allSatisfy(m -> assertThat(m.offset()).isZero());
    }
}
