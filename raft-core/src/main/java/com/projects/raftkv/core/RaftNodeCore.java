package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * Deterministic Raft core: leader election (M1) and log replication (M2), following Figure 2
 * ("State", "AppendEntries RPC", "RequestVote RPC", "Rules for Servers").
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
    private final Set<ProtocolMutation> mutations;

    // Persistent state (Figure 2). Durable once the Ready that changed it is executed.
    private long currentTerm;
    private Optional<NodeId> votedFor;

    // Volatile state (Figure 2). Both restart at 0 after a crash: the state machine is rebuilt by
    // re-applying the log (until M3 snapshots).
    private long commitIndex;
    private long lastApplied;
    private RoleState role;
    private int electionElapsed;
    private int randomizedElectionTimeout;
    private int heartbeatElapsed;

    // Output being collected for the current step.
    private HardState lastEmittedHardState;
    private SoftState lastEmittedSoftState;
    private final List<Message> outbox = new ArrayList<>();
    private final List<PersistOp> logOps = new ArrayList<>();
    private final List<ClientNotice> notices = new ArrayList<>();

    public RaftNodeCore(RaftConfig config, PersistentState recovered, RandomGenerator rng) {
        this(config, recovered, rng, Set.of());
    }

    private RaftNodeCore(RaftConfig config, PersistentState recovered, RandomGenerator rng,
                         Set<ProtocolMutation> mutations) {
        this.config = Objects.requireNonNull(config);
        this.self = config.self();
        this.rng = Objects.requireNonNull(rng);
        this.mutations = mutations.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(mutations));
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

    /**
     * A core that deliberately breaks the given protocol rules (DESIGN.md M2-C). For mutation tests
     * only: they prove the simulator's checkers notice each bug.
     */
    public static RaftNodeCore withMutationsForTesting(RaftConfig config, PersistentState recovered,
                                                       RandomGenerator rng, Set<ProtocolMutation> mutations) {
        return new RaftNodeCore(config, recovered, rng, Objects.requireNonNull(mutations));
    }

    @Override
    public Ready step(Event event) {
        Objects.requireNonNull(event);
        switch (event) {
            case Event.Tick t -> onTick();
            case Event.MessageReceived(Message m) -> onMessage(m);
            case Event.ProposeCommand p -> onPropose(p);
            case Event.ReadRequested r ->
                    throw new UnsupportedOperationException("ReadIndex arrives in M4: " + r);
        }
        return drainReady();
    }

    @Override
    public RaftStatus status() {
        return new RaftStatus(self, softState(), hardState(), commitIndex, lastApplied);
    }

    @Override
    public LogView log() {
        return log;
    }

    /** Visible for tests: the current randomized election timeout in ticks. */
    int randomizedElectionTimeout() {
        return randomizedElectionTimeout;
    }

    /** Visible for tests: the leader's progress for a peer; empty when not leader. */
    Optional<Progress> progressOf(NodeId peer) {
        return role instanceof RoleState.Leader l ? Optional.of(l.progress(peer)) : Optional.empty();
    }

    /** Visible for tests. */
    Set<ProtocolMutation> mutations() {
        return mutations;
    }

    // ---------------------------------------------------------------- timers

    private void onTick() {
        switch (role) {
            case RoleState.Follower f -> tickElection();
            case RoleState.Candidate c -> tickElection();
            case RoleState.Leader l -> tickHeartbeat(l);
        }
    }

    /** Figure 2 Followers/Candidates: election timeout elapses without valid leader contact → new election. */
    private void tickElection() {
        electionElapsed++;
        if (electionElapsed >= randomizedElectionTimeout) {
            startElection();
        }
    }

    /**
     * Figure 2 Leaders: send AppendEntries during idle periods to prevent election timeouts (§5.2).
     * Per M2-A the same message also carries any entries a follower is missing.
     */
    private void tickHeartbeat(RoleState.Leader l) {
        heartbeatElapsed++;
        if (heartbeatElapsed >= config.heartbeatIntervalTicks()) {
            heartbeatElapsed = 0;
            broadcastAppend(l);
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

    /**
     * Figure 2 Leaders, upon election: reinitialize nextIndex = lastIndex + 1 and matchIndex = 0, then
     * append a no-op entry in the new term (§8) and send it at once. The no-op is what lets this leader
     * commit the entries it inherited: it may only commit entries of its own term by counting replicas
     * (Figure 8), and earlier entries then commit with it.
     */
    private void becomeLeader() {
        RoleState.Leader l = new RoleState.Leader(config.peers(), log.lastIndex() + 1);
        role = l;
        heartbeatElapsed = 0;
        appendAsLeader(new EntryPayload.Noop());
        broadcastAppend(l);
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

    // ---------------------------------------------------------------- client proposals

    /**
     * §5.3: the leader appends the command to its own log and replicates it. The proposal is only
     * accepted (placed at (index, term)), not committed; the driver learns the outcome at apply time.
     * A non-leader rejects it with the leader it knows, so the client can redirect (thesis §6.2).
     */
    private void onPropose(Event.ProposeCommand p) {
        if (role instanceof RoleState.Leader l) {
            LogEntry e = appendAsLeader(new EntryPayload.Command(p.command()));
            notices.add(new ClientNotice.ProposalAccepted(p.proposalId(), e.index(), e.term()));
            broadcastAppend(l);
        } else {
            notices.add(new ClientNotice.ProposalRejected(p.proposalId(), softState().leader()));
        }
    }

    /** Leader Append-Only: a leader only ever adds entries at the end of its log, in its own term. */
    private LogEntry appendAsLeader(EntryPayload payload) {
        LogEntry e = new LogEntry(log.lastIndex() + 1, currentTerm, payload);
        log.append(List.of(e));
        logOps.add(new PersistOp.AppendEntries(List.of(e)));
        return e;
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
            boolean upToDate = log.isAtLeastAsUpToDateAsMine(rv.lastLogIndex(), rv.lastLogTerm())
                    || mutations.contains(ProtocolMutation.SKIP_UP_TO_DATE_CHECK);   // MUTATION (test only)
            if (mayVote && upToDate) {
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
     * Figure 2 AppendEntries receiver:
     * <ol>
     *   <li>Reply false if term < currentTerm (no timer reset: not valid leader contact).</li>
     *   <li>Reply false if the log has no entry at prevLogIndex whose term is prevLogTerm.</li>
     *   <li>If an existing entry conflicts with a new one (same index, different term), delete it and
     *       all that follow. Only on a real conflict: a reordered old message whose entries already
     *       match must not delete the newer entries after them (§5.3).</li>
     *   <li>Append any new entries not already in the log.</li>
     *   <li>If leaderCommit > commitIndex, commitIndex = min(leaderCommit, index of last new entry).
     *       "Last new entry" is the last one this message verified, not our lastIndex: entries beyond
     *       it may be stale leftovers from an older leader.</li>
     * </ol>
     * Otherwise the sender is the leader of currentTerm: candidates step down (§5.2) and the timer resets.
     */
    private void onAppendEntries(Message.AppendEntries ae) {
        if (ae.term() < currentTerm) {
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(), 0, 0));
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
        if (prevTerm.isEmpty()) {
            // Fast backup hint: our log ends before prevLogIndex.
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(),
                    0, log.lastIndex() + 1));
            return;
        }
        if (prevTerm.getAsLong() != ae.prevLogTerm()) {
            // Fast backup hint: the term we hold there and where our run of that term starts.
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(),
                    prevTerm.getAsLong(), log.firstIndexOfTermAt(ae.prevLogIndex())));
            return;
        }

        if (mutations.contains(ProtocolMutation.TRUNCATE_ON_MATCH)) {
            truncateAfterPrevUnconditionally(ae);                    // MUTATION (test only)
        } else {
            mergeEntries(ae.entries());
        }

        long lastNewEntry = ae.prevLogIndex() + ae.entries().size();
        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(ae.leaderCommit(), lastNewEntry));
        }
        send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, true, lastNewEntry, 0, 0, 0));
    }

    /** Steps 3 and 4: skip entries we already have, truncate at the first real conflict, append the rest. */
    private void mergeEntries(List<LogEntry> entries) {
        int firstNew = entries.size();
        for (int i = 0; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            OptionalLong existing = log.termAt(e.index());
            if (existing.isEmpty()) {
                firstNew = i;
                break;
            }
            if (existing.getAsLong() != e.term()) {
                if (e.index() <= commitIndex) {
                    throw new IllegalStateException("State Machine Safety violated: " + self
                            + " would delete committed entry " + e.index() + " (commitIndex " + commitIndex
                            + ") for " + e);
                }
                log.truncateFrom(e.index());
                logOps.add(new PersistOp.TruncateSuffix(e.index()));
                firstNew = i;
                break;
            }
        }
        if (firstNew < entries.size()) {
            List<LogEntry> rest = entries.subList(firstNew, entries.size());
            log.append(rest);
            logOps.add(new PersistOp.AppendEntries(rest));
        }
    }

    /**
     * MUTATION (test only): the classic bug of replacing everything after prevLogIndex with the
     * message's entries. A reordered old message then deletes newer, possibly committed, entries.
     * No committed-entry guard here: the simulator's checkers must catch the consequence.
     */
    private void truncateAfterPrevUnconditionally(Message.AppendEntries ae) {
        if (log.lastIndex() > ae.prevLogIndex()) {
            log.truncateFrom(ae.prevLogIndex() + 1);
            logOps.add(new PersistOp.TruncateSuffix(ae.prevLogIndex() + 1));
        }
        if (!ae.entries().isEmpty()) {
            log.append(ae.entries());
            logOps.add(new PersistOp.AppendEntries(ae.entries()));
        }
        commitIndex = Math.min(commitIndex, log.lastIndex());
        lastApplied = Math.min(lastApplied, commitIndex);
    }

    /**
     * Figure 2 Leaders: on success, matchIndex = max(old, reply) and nextIndex = max(old, reply + 1);
     * monotonic, so reordered and duplicated replies are harmless. On failure, back nextIndex up and
     * retry, but only for a rejection of the latest probe (rejectedIndex == nextIndex - 1). Replies from
     * other terms are ignored.
     */
    private void onAppendEntriesResponse(Message.AppendEntriesResponse r) {
        if (!(role instanceof RoleState.Leader l) || r.term() != currentTerm) return;
        Progress p = l.progress(r.from());
        if (r.success()) {
            if (r.matchIndex() > log.lastIndex())
                throw new IllegalStateException("follower " + r.from() + " acked index " + r.matchIndex()
                        + " beyond leader's last index " + log.lastIndex());
            if (r.matchIndex() <= p.matchIndex()) return;           // stale or duplicate ack
            Progress next = new Progress(Math.max(p.nextIndex(), r.matchIndex() + 1), r.matchIndex());
            l.update(r.from(), next);
            maybeAdvanceCommit(l);
            if (next.nextIndex() <= log.lastIndex()) sendAppend(r.from(), next);
        } else {
            if (r.rejectedIndex() != p.nextIndex() - 1) return;     // answers an older probe
            Progress next = new Progress(backedUpNextIndex(p, r), p.matchIndex());
            l.update(r.from(), next);
            sendAppend(r.from(), next);
        }
    }

    /**
     * Fast log backup (thesis §5.3; the brief's conflictTerm/conflictIndex). One round trip per
     * conflicting term instead of one per entry:
     * <ul>
     *   <li>conflictTerm = 0 (follower log too short): resume at conflictIndex = its lastIndex + 1.</li>
     *   <li>the leader also has entries of conflictTerm: resume after the leader's last one, since
     *       Log Matching says everything up to there may match.</li>
     *   <li>otherwise skip the follower's whole run of conflictTerm: resume at conflictIndex.</li>
     * </ul>
     * Clamped to [matchIndex + 1, nextIndex - 1]: always progress, never re-probe known matches. A wrong
     * hint can only cost round trips, never safety: the consistency check still guards every append.
     */
    private long backedUpNextIndex(Progress p, Message.AppendEntriesResponse r) {
        long candidate = r.conflictIndex();
        if (r.conflictTerm() > 0) {
            long last = log.lastIndexOfTerm(r.conflictTerm());
            if (last > 0) candidate = last + 1;
        }
        return Math.max(p.matchIndex() + 1, Math.min(candidate, p.nextIndex() - 1));
    }

    /**
     * Figure 2 Leaders / §5.4.2 (Figure 8): commitIndex = the largest N such that a majority (counting
     * the leader, whose own entries are durable before anything is sent, E1) has matchIndex >= N and
     * log[N].term == currentTerm. Older entries become committed indirectly through such an N.
     */
    private void maybeAdvanceCommit(RoleState.Leader l) {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n).getAsLong() != currentTerm
                    && !mutations.contains(ProtocolMutation.COMMIT_OLD_TERM_BY_COUNT)) {   // MUTATION (test only)
                return;   // terms never increase going backwards, so no smaller N qualifies either
            }
            int replicas = 1;   // the leader itself
            for (Progress p : l.all()) {
                if (p.matchIndex() >= n) replicas++;
            }
            if (replicas >= config.quorum()) {
                commitIndex = n;
                return;
            }
        }
    }

    /** Figure 2 / M2-A: send each peer the entries from its nextIndex (possibly none: a heartbeat). */
    private void broadcastAppend(RoleState.Leader l) {
        for (NodeId peer : config.peers()) {
            sendAppend(peer, l.progress(peer));
        }
    }

    private void sendAppend(NodeId peer, Progress p) {
        long prevIndex = p.nextIndex() - 1;
        long prevTerm = log.termAt(prevIndex).orElseThrow(
                () -> new IllegalStateException("nextIndex " + p.nextIndex() + " beyond log for " + peer));
        List<LogEntry> entries = log.entriesFrom(p.nextIndex(), config.maxEntriesPerAppend());
        send(new Message.AppendEntries(self, peer, currentTerm, prevIndex, prevTerm, entries, commitIndex));
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

    /**
     * Packages this step's effects. Persist order: hard state first, then log changes in the order
     * they happened, so that every entry on disk has term <= the durable currentTerm. Committed
     * entries not yet applied are handed out once, in index order (State Machine Safety relies on
     * every node applying the same entries in the same order).
     */
    private Ready drainReady() {
        List<PersistOp> persist = new ArrayList<>();
        HardState hs = hardState();
        if (!hs.equals(lastEmittedHardState)) persist.add(new PersistOp.SaveHardState(hs));
        lastEmittedHardState = hs;
        persist.addAll(logOps);

        List<LogEntry> toApply = new ArrayList<>();
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            toApply.add(log.entryAt(i).orElseThrow(
                    () -> new IllegalStateException("commitIndex " + commitIndex + " beyond log")));
        }
        lastApplied = Math.max(lastApplied, commitIndex);

        SoftState ss = softState();
        Optional<SoftState> soft = ss.equals(lastEmittedSoftState) ? Optional.empty() : Optional.of(ss);
        lastEmittedSoftState = ss;

        Ready ready = new Ready(persist, outbox, toApply, notices, soft);
        outbox.clear();
        logOps.clear();
        notices.clear();
        return ready;
    }
}
