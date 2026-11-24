package com.projects.raftkv.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.client.ClientSession.Attempt;
import com.projects.raftkv.client.ClientSession.Decision;
import com.projects.raftkv.client.ClientSession.Reply;
import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * The client's half of exactly-once (thesis §6.3): one request in flight, the SAME seq on every retry,
 * leader redirect (§6.2), and a new seq only for a new operation.
 */
class ClientSessionTest {

    private static final NodeId N1 = new NodeId(1), N2 = new NodeId(2), N3 = new NodeId(3);
    private static final Bytes OK = Bytes.copyOf(new byte[] {1});

    private final ClientSession s = new ClientSession(7, List.of(N1, N2, N3), new SplittableRandom(1),
            new ClientSession.Config(150, 10));

    private static KvRequest request(Attempt a) {
        return KvCodec.decodeRequest(a.payload());
    }

    private static Attempt send(Decision d) {
        assertThat(d).isInstanceOf(Decision.Send.class);
        return ((Decision.Send) d).attempt();
    }

    @Test
    void eachWriteGetsTheNextSeqAndEveryRetryReusesIt() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        assertThat(request(a1)).isEqualTo(new KvRequest(7, 1, new KvCommand.Put("x", "1")));

        Attempt a2 = send(s.onTimeout(a1.attemptId()));
        assertThat(a2.attemptId()).isNotEqualTo(a1.attemptId());
        assertThat(a2.target()).isNotEqualTo(a1.target());        // a timeout tries another node
        assertThat(a2.payload()).isEqualTo(a1.payload());         // same clientId, same seq, same command

        Attempt a3 = send(s.onReply(a2.attemptId(), new Reply.Retry()));
        assertThat(a3.payload()).isEqualTo(a1.payload());
        assertThat(s.onReply(a3.attemptId(), new Reply.Ok(OK))).isEqualTo(new Decision.Completed(1, OK));

        assertThat(request(s.write(new KvCommand.Delete("x"))).seq()).isEqualTo(2);
    }

    /** Any attempt's success answers the operation: they all carry the same request, deduplicated. */
    @Test
    void aLateSuccessOfAnEarlierAttemptCompletesTheOperation() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        send(s.onTimeout(a1.attemptId()));
        assertThat(s.onReply(a1.attemptId(), new Reply.Ok(OK))).isEqualTo(new Decision.Completed(1, OK));
        assertThat(s.busy()).isFalse();
    }

    /** Only the latest attempt's failure matters; an older attempt failing changes nothing. */
    @Test
    void aFailureOfAnEarlierAttemptIsIgnored() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        Attempt a2 = send(s.onTimeout(a1.attemptId()));
        assertThat(s.onReply(a1.attemptId(), new Reply.NotLeader(Optional.empty()))).isEqualTo(new Decision.Ignore());
        assertThat(s.onTimeout(a1.attemptId())).isEqualTo(new Decision.Ignore());
        assertThat(s.busy()).isTrue();
        assertThat(send(s.onTimeout(a2.attemptId())).payload()).isEqualTo(a1.payload());
    }

    @Test
    void repliesForAFinishedOperationAreIgnored() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        s.onReply(a1.attemptId(), new Reply.Ok(OK));
        s.write(new KvCommand.Put("x", "2"));
        assertThat(s.onReply(a1.attemptId(), new Reply.Ok(OK))).isEqualTo(new Decision.Ignore());
        assertThat(s.busy()).isTrue();
    }

    /** Thesis §6.2: a follower answers with the leader it knows, and the client goes there at once. */
    @Test
    void aLeaderHintIsFollowedImmediately() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        NodeId hint = a1.target().equals(N3) ? N2 : N3;
        Attempt a2 = send(s.onReply(a1.attemptId(), new Reply.NotLeader(Optional.of(hint))));
        assertThat(a2.target()).isEqualTo(hint);
        assertThat(a2.delayMs()).isZero();
    }

    /** No hint (an election is running): back off, then try another node. */
    @Test
    void withoutAHintItBacksOffAndTriesAnotherNode() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        Attempt a2 = send(s.onReply(a1.attemptId(), new Reply.NotLeader(Optional.empty())));
        assertThat(a2.target()).isNotEqualTo(a1.target());
        assertThat(a2.delayMs()).isEqualTo(10);
    }

    @Test
    void theNodeThatAnsweredIsTheNextOperationsFirstTarget() {
        Attempt a1 = s.write(new KvCommand.Put("x", "1"));
        Attempt a2 = send(s.onTimeout(a1.attemptId()));
        s.onReply(a2.attemptId(), new Reply.Ok(OK));
        assertThat(s.write(new KvCommand.Put("x", "2")).target()).isEqualTo(a2.target());
    }

    /** ReadIndex reads change nothing, so they need no session: no seq, client 0 in the query. */
    @Test
    void readsUseNoSeq() {
        Attempt r = s.read("x");
        assertThat(r.read()).isTrue();
        assertThat(request(r)).isEqualTo(new KvRequest(0, 0, new KvCommand.Get("x")));
        s.onReply(r.attemptId(), new Reply.Ok(OK));
        assertThat(request(s.write(new KvCommand.Put("x", "1"))).seq()).isEqualTo(1);
    }

    @Test
    void oneOperationAtATime() {
        s.write(new KvCommand.Put("x", "1"));
        assertThatThrownBy(() -> s.read("x")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void writesMustNotBeReads() {
        assertThatThrownBy(() -> s.write(new KvCommand.Get("x"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void attemptIdsAreUniqueAcrossClients() {
        ClientSession other = new ClientSession(8, List.of(N1, N2, N3), new SplittableRandom(1),
                new ClientSession.Config(150, 10));
        assertThat(other.write(new KvCommand.Put("x", "1")).attemptId())
                .isNotEqualTo(s.write(new KvCommand.Put("x", "1")).attemptId());
    }
}
