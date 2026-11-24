package com.projects.raftkv.core;

import static com.projects.raftkv.core.CoreFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * ReadIndex (thesis §6.4, DESIGN.md M4-B). A leader answers a read without writing to the log by:
 * (1) having committed an entry of its own term, (2) recording readIndex = commitIndex, (3) confirming
 * with a majority that it is still leader, via a read round echoed on AppendEntries replies. The
 * driver then waits for lastApplied >= readIndex and queries the state machine.
 */
class ReadIndexTest {

    private static List<ClientNotice.ReadReady> ready(Ready r) {
        return r.notices().stream().filter(ClientNotice.ReadReady.class::isInstance)
                .map(ClientNotice.ReadReady.class::cast).toList();
    }

    private static List<ClientNotice.ReadRejected> rejected(Ready r) {
        return r.notices().stream().filter(ClientNotice.ReadRejected.class::isInstance)
                .map(ClientNotice.ReadRejected.class::cast).toList();
    }

    private static Ready read(RaftCore c, long readId) {
        return c.step(new Event.ReadRequested(readId));
    }

    private static Message.AppendEntriesResponse ack(NodeId from, NodeId to, long term, long match, long round) {
        return new Message.AppendEntriesResponse(from, to, term, true, match, 0, 0, 0, round);
    }

    /** A leader of 3 whose no-op (index 1) is committed: rule 1 is satisfied. */
    private static RaftNodeCore committedLeader() {
        RaftNodeCore c = leaderOf3(N1);
        receive(c, ok(N2, N1, c.status().hard().currentTerm(), 1));
        assertThat(c.status().commitIndex()).isEqualTo(1);
        return c;
    }

    private static long term(RaftCore c) { return c.status().hard().currentTerm(); }

    // ---------------------------------------------------------------- non-leaders

    @Test
    void aFollowerRejectsWithTheLeaderItKnows() {
        RaftNodeCore f = fresh(N2);
        receive(f, heartbeat(N1, N2, 1));
        Ready r = read(f, 7);
        assertThat(rejected(r)).containsExactly(new ClientNotice.ReadRejected(7, Optional.of(N1)));
    }

    @Test
    void aCandidateRejectsWithoutAHint() {
        RaftNodeCore c = fresh(N2);
        tickUntilElection(c);
        assertThat(rejected(read(c, 7))).containsExactly(new ClientNotice.ReadRejected(7, Optional.empty()));
    }

    // ---------------------------------------------------------------- rule 2: confirm leadership

    @Test
    void aReadStartsARoundAndIsReleasedOnlyAfterAMajorityEchoesIt() {
        RaftNodeCore c = committedLeader();
        long t = term(c);

        Ready r = read(c, 7);
        assertThat(ready(r)).isEmpty();
        List<Message.AppendEntries> sent = ofType(r.messages(), Message.AppendEntries.class);
        assertThat(sent).extracting(Message.AppendEntries::to).containsExactlyInAnyOrder(N2, N3);
        assertThat(sent).allSatisfy(m -> assertThat(m.readRound()).isEqualTo(1));

        // A reply to an AppendEntries sent before the read started proves nothing about "now".
        assertThat(ready(receive(c, ack(N2, N1, t, 1, 0)))).isEmpty();
        // N2 echoes round 1: with the leader itself that is 2 of 3.
        assertThat(ready(receive(c, ack(N2, N1, t, 1, 1)))).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    @Test
    void aFollowerEchoesTheRoundEvenWhenItRejectsTheEntries() {
        RaftNodeCore f = fresh(N2);
        Ready r = receive(f, new Message.AppendEntries(N1, N2, 1, 5, 1, List.of(), 0, 4));
        Message.AppendEntriesResponse resp = ofType(r.messages(), Message.AppendEntriesResponse.class).get(0);
        assertThat(resp.success()).isFalse();
        assertThat(resp.readRound()).isEqualTo(4);
    }

    /**
     * Regression (gate, chaos 5 nodes, seed 4811): a long-delayed AppendEntries from this leader's OLD
     * term reached a follower already in the current term. The rejection carries the follower's current
     * term, so an echoed round would look like an ack from this term, for a round number from another
     * term. The leader then counted it and served a stale read. A stale-term rejection must echo nothing.
     */
    @Test
    void aRejectionOfAnOlderTermEchoesNoRound() {
        RaftNodeCore f = core(N5, FIVE, PersistentState.EMPTY, 42);
        receive(f, heartbeat(N3, N5, 10));                          // N5 is now in term 10
        Ready r = receive(f, new Message.AppendEntries(N2, N5, 6, 0, 0, List.of(), 0, 10));
        Message.AppendEntriesResponse resp = ofType(r.messages(), Message.AppendEntriesResponse.class).get(0);
        assertThat(resp.term()).isEqualTo(10);
        assertThat(resp.success()).isFalse();
        assertThat(resp.readRound()).isZero();

        Ready s = receive(f, new Message.InstallSnapshot(N2, N5, 6, 5, 6, 0, Bytes.EMPTY, true, 10));
        Message.InstallSnapshotResponse sresp = ofType(s.messages(), Message.InstallSnapshotResponse.class).get(0);
        assertThat(sresp.term()).isEqualTo(10);
        assertThat(sresp.readRound()).isZero();
    }

    /** The seed-4811 story at the leader: an old term's round number must never confirm a read. */
    @Test
    void anOldTermsRoundCannotConfirmARead() {
        // N1 led term 2 and sent read round 10 then; now it leads term 4, a round counter restarted at 0.
        RaftNodeCore n5 = core(N5, FIVE, PersistentState.EMPTY, 42);
        receive(n5, heartbeat(N1, N5, 4));                          // N5 follows N1 in term 4
        Ready stale = receive(n5, new Message.AppendEntries(N1, N5, 2, 0, 0, List.of(), 0, 10));
        Message.AppendEntriesResponse echoed = ofType(stale.messages(), Message.AppendEntriesResponse.class).get(0);

        List<NodeId> voters = List.of(N2, N3);
        RaftNodeCore leader = leaderAt(N1, FIVE, 4, PersistentState.EMPTY, voters);
        receive(leader, ok(N2, N1, 4, 1));
        receive(leader, ok(N3, N1, 4, 1));
        read(leader, 7);                                            // round 1
        receive(leader, ack(N2, N1, 4, 1, 1));                      // 2 of 5 so far
        assertThat(ready(receive(leader, echoed))).isEmpty();       // the stale echo adds nothing
    }

    /** A rejection in our term still proves the follower accepts us as leader of this term. */
    @Test
    void aRejectionInTheCurrentTermCountsAsAnAck() {
        RaftNodeCore c = committedLeader();
        read(c, 7);
        Message.AppendEntriesResponse rej = new Message.AppendEntriesResponse(N3, N1, term(c), false, 0, 0, 0, 1, 1);
        assertThat(ready(receive(c, rej))).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    @Test
    void duplicateAndReorderedAcksAreHarmless() {
        List<NodeId> voters = List.of(N2, N3);
        RaftNodeCore c = leaderAt(N1, FIVE, 2, PersistentState.EMPTY, voters);
        receive(c, ok(N2, N1, 2, 1));
        receive(c, ok(N3, N1, 2, 1));
        assertThat(c.status().commitIndex()).isEqualTo(1);

        read(c, 7);
        // The same follower twice is still one vote: 2 of 5, not a majority.
        assertThat(ready(receive(c, ack(N2, N1, 2, 1, 1)))).isEmpty();
        assertThat(ready(receive(c, ack(N2, N1, 2, 1, 1)))).isEmpty();
        // An old, reordered reply cannot lower what N2 acknowledged, nor count for N4.
        assertThat(ready(receive(c, ack(N2, N1, 2, 1, 0)))).isEmpty();
        assertThat(ready(receive(c, ack(N4, N1, 2, 1, 0)))).isEmpty();
        assertThat(ready(receive(c, ack(N4, N1, 2, 1, 1)))).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    /** At most one round in flight: reads arriving meanwhile wait and then share the next round. */
    @Test
    void readsThatArriveDuringARoundShareTheNextOne() {
        RaftNodeCore c = committedLeader();
        long t = term(c);
        read(c, 1);
        Ready second = read(c, 2);
        Ready third = read(c, 3);
        assertThat(ofType(second.messages(), Message.AppendEntries.class)).isEmpty();
        assertThat(ofType(third.messages(), Message.AppendEntries.class)).isEmpty();

        Ready r = receive(c, ack(N2, N1, t, 1, 1));
        assertThat(ready(r)).containsExactly(new ClientNotice.ReadReady(1, 1));
        List<Message.AppendEntries> round2 = ofType(r.messages(), Message.AppendEntries.class);
        assertThat(round2).isNotEmpty().allSatisfy(m -> assertThat(m.readRound()).isEqualTo(2));

        assertThat(ready(receive(c, ack(N3, N1, t, 1, 2))))
                .containsExactly(new ClientNotice.ReadReady(2, 1), new ClientNotice.ReadReady(3, 1));
    }

    /** A lost round is retried: every heartbeat carries the current round. */
    @Test
    void heartbeatsCarryTheCurrentRound() {
        RaftNodeCore c = committedLeader();
        read(c, 7);
        List<Message> hb = tickN(c, HEARTBEAT);
        assertThat(ofType(hb, Message.AppendEntries.class)).isNotEmpty()
                .allSatisfy(m -> assertThat(m.readRound()).isEqualTo(1));
    }

    @Test
    void theReadIndexIsTheCommitIndexWhenTheReadArrived() {
        RaftNodeCore c = committedLeader();
        long t = term(c);
        c.step(new Event.ProposeCommand(1, cmd("x").data()));   // index 2, not committed yet
        read(c, 7);
        receive(c, ok(N2, N1, t, 2));                            // commit 2, but the read was at 1
        Ready r = receive(c, ack(N2, N1, t, 2, 1));
        assertThat(ready(r)).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    // ---------------------------------------------------------------- rule 1: a current-term commit

    /**
     * A new leader's commitIndex can lag behind what earlier leaders committed (it learns the commit
     * point only by committing its own no-op, §8). Until then it must not pick a readIndex.
     */
    @Test
    void aNewLeaderWaitsForItsNoOpBeforeChoosingAReadIndex() {
        RaftNodeCore c = leaderOf3(N1);
        long t = term(c);
        Ready r = read(c, 7);
        assertThat(ready(r)).isEmpty();
        assertThat(ofType(r.messages(), Message.AppendEntries.class)).isEmpty();   // no round yet

        // An echo of round 0 must not release it either.
        assertThat(ready(receive(c, ack(N3, N1, t, 0, 0)))).isEmpty();

        Ready commit = receive(c, ok(N2, N1, t, 1));                   // the no-op commits ...
        assertThat(ready(commit)).isEmpty();
        assertThat(ofType(commit.messages(), Message.AppendEntries.class))
                .allSatisfy(m -> assertThat(m.readRound()).isEqualTo(1));  // ... and the round starts
        assertThat(ready(receive(c, ack(N2, N1, t, 1, 1)))).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    // ---------------------------------------------------------------- losing leadership

    @Test
    void steppingDownRejectsEveryPendingRead() {
        RaftNodeCore c = leaderOf3(N1);
        long t = term(c);
        read(c, 1);                                   // waiting for the no-op
        receive(c, ok(N2, N1, t, 1));
        read(c, 2);                                   // in round 1
        read(c, 3);                                   // waiting for a round
        Ready r = receive(c, new Message.RequestVote(N3, N1, t + 1, 5, t));
        assertThat(rejected(r)).extracting(ClientNotice.ReadRejected::readId).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(ready(r)).isEmpty();
        // Late echoes of the old term change nothing.
        assertThat(receive(c, ack(N2, N1, t, 1, 1)).notices()).isEmpty();
    }

    /**
     * The stale-read scenario ReadIndex exists for: an old leader cut off from the majority keeps
     * believing it leads, while a new leader commits a write. The old leader must never answer.
     */
    @Test
    void aPartitionedOldLeaderNeverServesAndLaterRejects() {
        MiniCluster mc = new MiniCluster();
        for (NodeId id : THREE) mc.add(core(id, THREE, PersistentState.EMPTY, id.value()));
        // N1 wins term 1.
        RaftNodeCore n1 = mc.node(N1);
        mc.step(N1, new Event.Tick());
        while (n1.status().soft().role() != Role.CANDIDATE) mc.step(N1, new Event.Tick());
        mc.deliverAll();
        mc.tickAll();
        assertThat(n1.status().soft().role()).isEqualTo(Role.LEADER);
        assertThat(n1.status().commitIndex()).isEqualTo(1);

        mc.isolate(N1);
        // N2 and N3 elect a new leader and commit a write N1 never sees.
        RaftNodeCore n2 = mc.node(N2);
        while (n2.status().soft().role() != Role.CANDIDATE) mc.step(N2, new Event.Tick());
        mc.deliverAll();
        assertThat(n2.status().soft().role()).isEqualTo(Role.LEADER);
        mc.step(N2, new Event.ProposeCommand(1, cmd("x=2").data()));
        mc.deliverAll();
        mc.tickAll();
        assertThat(n2.status().commitIndex()).isGreaterThanOrEqualTo(3);

        List<Ready> n1Readies = new ArrayList<>();
        n1Readies.add(n1.step(new Event.ReadRequested(9)));
        for (int i = 0; i < 5 * T; i++) n1Readies.add(n1.step(new Event.Tick()));
        assertThat(n1Readies).allSatisfy(r -> assertThat(ready(r)).isEmpty());

        // Healed: the first message from the new term deposes N1, and its read is rejected.
        Ready r = receive(n1, new Message.AppendEntries(N2, N1, term(n2), 0, 0, List.of(), 0));
        assertThat(rejected(r)).extracting(ClientNotice.ReadRejected::readId).containsExactly(9L);
    }

    // ---------------------------------------------------------------- mutations (M4-F)

    @Test
    void mutationReadWithoutQuorumCheckAnswersAtOnce() {
        RaftNodeCore c = RaftNodeCore.withMutationsForTesting(config(N1, THREE), PersistentState.EMPTY,
                new SplittableRandom(42), Set.of(ProtocolMutation.READ_WITHOUT_QUORUM_CHECK));
        tickUntilElection(c);
        receive(c, new Message.RequestVoteResponse(N2, N1, term(c), true));
        receive(c, ok(N2, N1, term(c), 1));
        assertThat(ready(read(c, 7))).containsExactly(new ClientNotice.ReadReady(7, 1));
    }

    @Test
    void mutationReadBeforeCurrentTermCommitUsesTheLaggingCommitIndex() {
        RaftNodeCore c = RaftNodeCore.withMutationsForTesting(config(N1, THREE), PersistentState.EMPTY,
                new SplittableRandom(42), Set.of(ProtocolMutation.READ_BEFORE_CURRENT_TERM_COMMIT));
        tickUntilElection(c);
        receive(c, new Message.RequestVoteResponse(N2, N1, term(c), true));
        read(c, 7);
        assertThat(ready(receive(c, ack(N2, N1, term(c), 0, 1)))).containsExactly(new ClientNotice.ReadReady(7, 0));
    }
}
