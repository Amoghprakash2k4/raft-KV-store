package com.projects.raftkv.client;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.random.RandomGenerator;

/**
 * The client's half of exactly-once semantics (thesis §6.3) and leader redirect (§6.2), as a pure state
 * machine: no network, no clock, no threads. The caller sends each {@link Attempt}, schedules its
 * timeout, and feeds back replies and timeouts. The simulator drives it today; a network client (M5)
 * will wrap it.
 *
 * <p>Rules:
 * <ul>
 *   <li>One operation in flight (DESIGN.md M4-A; no pipelining). A write gets the next seq; every retry
 *       of it re-sends exactly the same request (same clientId, seq, command), so the cluster's session
 *       table can recognise duplicates.</li>
 *   <li>Any attempt's success completes the operation: all attempts carry the same request, so they
 *       all get the same (deduplicated) answer.</li>
 *   <li>Only the latest attempt's failure or timeout causes a retry, so retries never multiply.</li>
 *   <li>A rejection with a leader hint goes straight to the hint; without one (an election is running)
 *       the client backs off and tries another node; a timeout tries another node.</li>
 *   <li>Reads (ReadIndex, §6.4) change nothing, so they carry no session: client 0, seq 0.</li>
 * </ul>
 */
public final class ClientSession {

    /** {@code timeoutMs}: how long the caller waits for a reply; {@code retryBackoffMs}: pause before a blind retry. */
    public record Config(long timeoutMs, long retryBackoffMs) {
        public Config {
            if (timeoutMs < 1 || retryBackoffMs < 0) throw new IllegalArgumentException("bad client config");
        }
    }

    /** One try of the current operation: send {@code payload} to {@code target} after {@code delayMs}. */
    public record Attempt(long attemptId, NodeId target, boolean read, Bytes payload, long delayMs) {
        public Attempt { Objects.requireNonNull(target); Objects.requireNonNull(payload); }
    }

    /** What a node answered to one attempt. */
    public sealed interface Reply {
        /** Committed and applied (a write) or served by ReadIndex (a read). */
        record Ok(Bytes result) implements Reply {
            public Ok { Objects.requireNonNull(result); }
        }

        /** Not the leader; maybe knows who is. */
        record NotLeader(Optional<NodeId> hint) implements Reply {
            public NotLeader { Objects.requireNonNull(hint); }
        }

        /** The node may or may not have executed it (overwritten entry, snapshot): send the same request again. */
        record Retry() implements Reply {}
    }

    /** What the caller should do next. */
    public sealed interface Decision {
        /** The operation {@code opNumber} finished with {@code result}; the session is idle. */
        record Completed(long opNumber, Bytes result) implements Decision {}

        /** Send this attempt (after its delay) and schedule its timeout. */
        record Send(Attempt attempt) implements Decision {}

        /** Nothing to do: a reply for an older attempt or an operation that already finished. */
        record Ignore() implements Decision {}
    }

    private record Op(long number, boolean read, Bytes payload) {}

    private final long clientId;
    private final List<NodeId> members;
    private final RandomGenerator rng;
    private final Config config;

    private long seq;
    private long opNumber;
    private long attemptCounter;
    private Optional<NodeId> leaderGuess = Optional.empty();
    private Op current;                                    // null when idle
    /** Attempts of the current operation and their targets. */
    private final Map<Long, NodeId> attemptsOfCurrent = new HashMap<>();
    private long latestAttempt;
    private NodeId latestTarget;

    public ClientSession(long clientId, List<NodeId> members, RandomGenerator rng, Config config) {
        if (clientId <= 0) throw new IllegalArgumentException("clientId must be positive (0 means no session)");
        if (clientId >= (1L << 23)) throw new IllegalArgumentException("clientId too large");
        this.clientId = clientId;
        this.members = List.copyOf(members);
        if (this.members.size() < 2) throw new IllegalArgumentException("need at least 2 members");
        this.rng = Objects.requireNonNull(rng);
        this.config = Objects.requireNonNull(config);
    }

    public long clientId() { return clientId; }

    public Config config() { return config; }

    public boolean busy() { return current != null; }

    /** The seq of the last write started (0 before the first). */
    public long seq() { return seq; }

    /** Starts a write with the next seq. Gets go through {@link #read}. */
    public Attempt write(KvCommand command) {
        Objects.requireNonNull(command);
        if (command instanceof KvCommand.Get) throw new IllegalArgumentException("reads use read(), not write()");
        requireIdle();
        seq++;
        return start(false, KvCodec.encode(new KvRequest(clientId, seq, command)));
    }

    /** Starts a linearizable read of {@code key} (ReadIndex, thesis §6.4). */
    public Attempt read(String key) {
        requireIdle();
        return start(true, KvCodec.encode(new KvRequest(0, 0, new KvCommand.Get(key))));
    }

    public Decision onReply(long attemptId, Reply reply) {
        Objects.requireNonNull(reply);
        if (current == null || !attemptsOfCurrent.containsKey(attemptId)) return new Decision.Ignore();
        return switch (reply) {
            case Reply.Ok ok -> {
                Op done = current;
                leaderGuess = Optional.of(attemptsOfCurrent.get(attemptId));   // it answered: start there next time
                current = null;
                attemptsOfCurrent.clear();
                yield new Decision.Completed(done.number(), ok.result());
            }
            case Reply.NotLeader nl -> {
                if (attemptId != latestAttempt) yield new Decision.Ignore();
                leaderGuess = nl.hint();
                yield nl.hint().isPresent()
                        ? new Decision.Send(attempt(nl.hint().get(), 0))
                        : new Decision.Send(attempt(otherThan(latestTarget), config.retryBackoffMs()));
            }
            case Reply.Retry r -> attemptId != latestAttempt ? new Decision.Ignore()
                    : new Decision.Send(attempt(latestTarget, config.retryBackoffMs()));
        };
    }

    /** The caller's timer for {@code attemptId} fired with no reply: try another node. */
    public Decision onTimeout(long attemptId) {
        if (current == null || attemptId != latestAttempt) return new Decision.Ignore();
        leaderGuess = Optional.empty();
        return new Decision.Send(attempt(otherThan(latestTarget), 0));
    }

    private Attempt start(boolean read, Bytes payload) {
        current = new Op(++opNumber, read, payload);
        NodeId target = leaderGuess.orElseGet(() -> members.get(rng.nextInt(members.size())));
        return attempt(target, 0);
    }

    private Attempt attempt(NodeId target, long delayMs) {
        long id = (clientId << 40) | ++attemptCounter;
        attemptsOfCurrent.put(id, target);
        latestAttempt = id;
        latestTarget = target;
        return new Attempt(id, target, current.read(), current.payload(), delayMs);
    }

    private NodeId otherThan(NodeId n) {
        NodeId pick;
        do {
            pick = members.get(rng.nextInt(members.size()));
        } while (pick.equals(n));
        return pick;
    }

    private void requireIdle() {
        if (current != null) throw new IllegalStateException("operation " + current.number() + " still in flight");
    }
}
