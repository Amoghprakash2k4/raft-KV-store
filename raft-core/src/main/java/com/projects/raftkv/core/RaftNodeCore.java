package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * Deterministic Raft core. Implements leader election (M1): Figure 2 "Rules for Servers",
 * RequestVote, and the term/heartbeat parts of AppendEntries.
 *
 * <p>Each {@link #step} mutates in-memory state and returns a {@link Ready}. Per DESIGN.md E1 the
 * driver executes that Ready (persist + fsync first) before the next step, so the core may treat
 * everything it emitted as already durable.
 */
public final class RaftNodeCore implements RaftCore {

    private final RaftConfig config;
    private final NodeId self;
    private final RandomGenerator rng;
    private final RaftLog log;

    // Persistent state (Figure 2). Durable once the Ready that changed it is executed.
    private long currentTerm;
    private Optional<NodeId> votedFor;

    // Volatile state.
    private RoleState role;
    private int electionElapsed;
    private int randomizedElectionTimeout;
    private int heartbeatElapsed;

    // What the driver was last told, so each Ready carries only changes.
    private HardState lastEmittedHardState;
    private SoftState lastEmittedSoftState;
    private final List<Message> outbox = new ArrayList<>();

    public RaftNodeCore(RaftConfig config, PersistentState recovered, RandomGenerator rng) {
        this.config = Objects.requireNonNull(config);
        this.self = config.self();
        this.rng = Objects.requireNonNull(rng);
        this.log = new RaftLog(recovered.entries());
        this.currentTerm = recovered.hardState().currentTerm();
        this.votedFor = recovered.hardState().votedFor();
        votedFor.ifPresent(v -> {
            if (!config.members().contains(v))
                throw new IllegalArgumentException("recovered votedFor " + v + " is not a member");
        });
        this.role = new RoleState.Follower(Optional.empty());
        resetElectionTimer();
        this.lastEmittedHardState = hardState();
        this.lastEmittedSoftState = softState();
    }

    @Override
    public Ready step(Event event) {
        Objects.requireNonNull(event);
        outbox.clear();
        switch (event) {
            case Event.Tick t -> onTick();
            case Event.MessageReceived(Message m) -> onMessage(m);
            case Event.ProposeCommand p ->
                    throw new UnsupportedOperationException("client proposals arrive in M2: " + p);
            case Event.ReadRequested r ->
                    throw new UnsupportedOperationException("ReadIndex arrives in M4: " + r);
        }
        return drainReady();
    }

    @Override
    public RaftStatus status() {
        return new RaftStatus(self, softState(), hardState(), 0, 0);
    }

    @Override
    public LogView log() {
        return log;
    }

    /** Visible for tests: the current randomized election timeout in ticks. */
    int randomizedElectionTimeout() {
        return randomizedElectionTimeout;
    }

    // ---------------------------------------------------------------- timers

    private void onTick() {
        switch (role) {
            case RoleState.Follower f -> tickElection();
            case RoleState.Candidate c -> tickElection();
            case RoleState.Leader l -> tickHeartbeat();
        }
    }

    /** Figure 2 Followers/Candidates: election timeout elapses without valid leader contact → new election. */
    private void tickElection() {
        electionElapsed++;
        if (electionElapsed >= randomizedElectionTimeout) {
            startElection();
        }
    }

    /** Figure 2 Leaders: send empty AppendEntries during idle periods to prevent election timeouts (§5.2). */
    private void tickHeartbeat() {
        heartbeatElapsed++;
        if (heartbeatElapsed >= config.heartbeatIntervalTicks()) {
            heartbeatElapsed = 0;
            broadcastHeartbeat();
        }
    }

    /**
     * Restarts the election timer with a fresh timeout drawn uniformly from [T, 2T) (§5.2).
     * Called only when granting a vote, on valid AppendEntries from the current leader, when starting
     * an election, and when a leader steps down (its timer was not running).
     */
    private void resetElectionTimer() {
        electionElapsed = 0;
        int t = config.electionTimeoutTicks();
        randomizedElectionTimeout = t + rng.nextInt(t);
    }

    // ---------------------------------------------------------------- transitions

    /**
     * Figure 2 Candidates: increment currentTerm, vote for self, reset election timer, send RequestVote
     * to all other servers. The term and self-vote reach disk before any RequestVote leaves (E1).
     */
    private void startElection() {
        currentTerm++;
        votedFor = Optional.of(self);
        role = new RoleState.Candidate(Set.of(self));
        resetElectionTimer();
        for (NodeId peer : config.peers()) {
            send(new Message.RequestVote(self, peer, currentTerm, log.lastIndex(), log.lastTerm()));
        }
        maybeBecomeLeader();
    }

    private void maybeBecomeLeader() {
        if (role instanceof RoleState.Candidate c && c.votes().size() >= config.quorum()) {
            becomeLeader();
        }
    }

    /** Figure 2 Leaders: upon election, send initial empty AppendEntries to each server. */
    private void becomeLeader() {
        role = new RoleState.Leader();
        heartbeatElapsed = 0;
        // SHORTCUT: (M1 only) no no-op entry in the new term (§8); added in M2. See DEVIATIONS.md P2.
        broadcastHeartbeat();
    }

    /**
     * §5.1 / Figure 2 All Servers: if a message carries term > currentTerm, set currentTerm = term,
     * clear votedFor and convert to follower. The leader is unknown until it contacts us.
     * A former leader gets a fresh election timer; candidates and followers keep their running timer,
     * because a higher term alone is not valid leader contact.
     */
    private void stepDownToHigherTerm(long term) {
        boolean wasLeader = role instanceof RoleState.Leader;
        currentTerm = term;
        votedFor = Optional.empty();
        role = new RoleState.Follower(Optional.empty());
        if (wasLeader) resetElectionTimer();
    }

    // ---------------------------------------------------------------- messages

    private void onMessage(Message m) {
        if (!m.to().equals(self))
            throw new IllegalArgumentException(self + " received message addressed to " + m.to() + ": " + m);
        if (m.from().equals(self) || !config.members().contains(m.from()))
            throw new IllegalArgumentException(self + " received message from invalid sender: " + m);

        if (m.term() > currentTerm) {
            stepDownToHigherTerm(m.term());
        }
        switch (m) {
            case Message.RequestVote rv -> onRequestVote(rv);
            case Message.RequestVoteResponse r -> onRequestVoteResponse(r);
            case Message.AppendEntries ae -> onAppendEntries(ae);
            case Message.AppendEntriesResponse r -> onAppendEntriesResponse(r);
        }
    }

    /**
     * Figure 2 RequestVote receiver:
     * 1. Reply false if term < currentTerm.
     * 2. Grant if votedFor is null or candidateId, and the candidate's log is at least as up to date (§5.4.1).
     * Granting persists votedFor (before the reply leaves, per E1) and resets the election timer.
     */
    private void onRequestVote(Message.RequestVote rv) {
        boolean grant = false;
        if (rv.term() == currentTerm) {
            boolean mayVote = votedFor.isEmpty() || votedFor.get().equals(rv.from());
            if (mayVote && log.isAtLeastAsUpToDateAsMine(rv.lastLogIndex(), rv.lastLogTerm())) {
                votedFor = Optional.of(rv.from());
                resetElectionTimer();
                grant = true;
            }
        }
        send(new Message.RequestVoteResponse(self, rv.from(), currentTerm, grant));
    }

    /**
     * Figure 2 Candidates: become leader on votes from a majority. Responses from other terms or
     * received in another role are ignored; duplicates are idempotent (votes are a set).
     */
    private void onRequestVoteResponse(Message.RequestVoteResponse r) {
        if (!(role instanceof RoleState.Candidate c) || r.term() != currentTerm || !r.voteGranted()) {
            return;
        }
        role = c.withVote(r.from());
        maybeBecomeLeader();
    }

    /**
     * Figure 2 AppendEntries receiver (M1 subset):
     * 1. Reply false if term < currentTerm (no timer reset: not valid leader contact).
     * Otherwise the sender is the leader of currentTerm: candidates step down (§5.2), the timer resets,
     * and step 2 (prevLogIndex/prevLogTerm match) is checked. Entries and leaderCommit arrive in M2.
     */
    private void onAppendEntries(Message.AppendEntries ae) {
        if (ae.term() < currentTerm) {
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0));
            return;
        }
        switch (role) {
            case RoleState.Leader l -> throw new IllegalStateException(
                    "Election Safety violated: " + self + " is leader of term " + currentTerm
                            + " but received AppendEntries from " + ae.from() + " for the same term");
            case RoleState.Candidate c -> role = new RoleState.Follower(Optional.of(ae.from()));
            case RoleState.Follower f -> {
                if (f.leader().isPresent() && !f.leader().get().equals(ae.from())) {
                    throw new IllegalStateException("Election Safety violated: " + self + " saw leaders "
                            + f.leader().get() + " and " + ae.from() + " in term " + currentTerm);
                }
                role = new RoleState.Follower(Optional.of(ae.from()));
            }
        }
        resetElectionTimer();

        OptionalLong prevTerm = log.termAt(ae.prevLogIndex());
        if (prevTerm.isEmpty() || prevTerm.getAsLong() != ae.prevLogTerm()) {
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0));
            return;
        }
        if (!ae.entries().isEmpty()) {
            throw new UnsupportedOperationException("log replication arrives in M2: " + ae);
        }
        send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, true, ae.prevLogIndex()));
    }

    /** M1: only the term rule applies (handled in onMessage). M2 updates matchIndex/nextIndex here. */
    private void onAppendEntriesResponse(Message.AppendEntriesResponse r) {
        // Intentionally empty in M1.
    }

    // SHORTCUT: (M1 only) heartbeats always use prevLogIndex = prevLogTerm = 0 because the leader does
    // not yet track nextIndex. Valid per Figure 2 (index 0 always matches) but replicates nothing.
    // Replaced in M2. Listed in DEVIATIONS.md.
    private void broadcastHeartbeat() {
        for (NodeId peer : config.peers()) {
            send(new Message.AppendEntries(self, peer, currentTerm, 0, 0, List.of(), 0));
        }
    }

    // ---------------------------------------------------------------- output

    private void send(Message m) {
        outbox.add(m);
    }

    private HardState hardState() {
        return new HardState(currentTerm, votedFor);
    }

    private SoftState softState() {
        Optional<NodeId> leader = switch (role) {
            case RoleState.Leader l -> Optional.of(self);
            case RoleState.Follower f -> f.leader();
            case RoleState.Candidate c -> Optional.empty();
        };
        return new SoftState(role.role(), currentTerm, leader);
    }

    private Ready drainReady() {
        HardState hs = hardState();
        List<PersistOp> persist = hs.equals(lastEmittedHardState)
                ? List.of() : List.of(new PersistOp.SaveHardState(hs));
        lastEmittedHardState = hs;

        SoftState ss = softState();
        Optional<SoftState> soft = ss.equals(lastEmittedSoftState) ? Optional.empty() : Optional.of(ss);
        lastEmittedSoftState = ss;

        Ready ready = new Ready(persist, outbox, List.of(), List.of(), soft);
        outbox.clear();
        return ready;
    }
}
