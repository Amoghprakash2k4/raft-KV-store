package com.projects.raftkv.core;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * Deterministic Raft core: leader election (M1), log replication (M2), log compaction with
 * InstallSnapshot (M3) and ReadIndex reads (M4, thesis §6.4), following Figure 2 ("State", "AppendEntries RPC", "RequestVote RPC", "Rules for
 * Servers"), Figure 13 (InstallSnapshot) and the snapshot boundary rules in DESIGN.md D1a.
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

    // The latest snapshot's bytes (DESIGN.md M3-C): sent to followers whose nextIndex falls into it.
    private Optional<Snapshot> snapshot;
    // Follower side: an InstallSnapshot being assembled from chunks. Volatile; lost on restart.
    private Optional<Incoming> incoming = Optional.empty();
    private Optional<Snapshot> snapshotToRestore = Optional.empty();

    // Volatile state (Figure 2). After a crash both restart at the snapshot index (0 without one): the
    // state machine is restored from the snapshot and the rest is re-applied from the log.
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

    /** Chunks of one snapshot received so far, contiguous from offset 0. */
    private record Incoming(long index, long term, java.io.ByteArrayOutputStream bytes) {}

    public RaftNodeCore(RaftConfig config, PersistentState recovered, RandomGenerator rng) {
        this(config, recovered, rng, Set.of());
    }

    private RaftNodeCore(RaftConfig config, PersistentState recovered, RandomGenerator rng,
                         Set<ProtocolMutation> mutations) {
        this.config = Objects.requireNonNull(config);
        this.self = config.self();
        this.rng = Objects.requireNonNull(rng);
        this.mutations = mutations.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(mutations));
        this.snapshot = recovered.snapshot();
        this.log = new RaftLog(snapshot.map(Snapshot::index).orElse(0L), snapshot.map(Snapshot::term).orElse(0L),
                recovered.entries());
        this.commitIndex = log.snapshotIndex();   // a snapshot holds committed, applied state only
        this.lastApplied = log.snapshotIndex();
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
            case Event.SnapshotCreated sc -> onSnapshotCreated(sc);
            case Event.ReadRequested r -> onReadRequested(r);
        }
        if (role instanceof RoleState.Leader l) serviceReads(l);
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

    /** Visible for tests: the snapshot this core holds (its own or an installed one). */
    Optional<Snapshot> snapshot() {
        return snapshot;
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
        if (role instanceof RoleState.Leader l) rejectPendingReads(l);
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

    // ---------------------------------------------------------------- ReadIndex reads (thesis §6.4)

    /**
     * A read arrives. Only the leader can serve it; others reject with the leader they know, like
     * proposals (thesis §6.2). The leader queues it; {@link #serviceReads} gives it a read index and a
     * round when it may.
     */
    private void onReadRequested(Event.ReadRequested r) {
        if (role instanceof RoleState.Leader l) {
            l.waitingReads.add(r.readId());
        } else {
            notices.add(new ClientNotice.ReadRejected(r.readId(), softState().leader()));
        }
    }

    /**
     * Runs after every step on the leader. Thesis §6.4, DESIGN.md M4-B:
     * <ol>
     *   <li>Wait until this leader has committed an entry of its own term (its no-op). Before that its
     *       commitIndex may lag behind entries earlier leaders committed and clients saw acknowledged.</li>
     *   <li>readIndex = commitIndex.</li>
     *   <li>Confirm it is still leader: start read round r+1 (at most one round in flight, so waiting
     *       reads batch into the next one), send it to every peer, and release each read once a
     *       majority has echoed a round at least as new as the read's. A peer echoing in our term
     *       had not voted for a newer leader when it replied, after the read arrived, so no newer
     *       leader could have committed anything at that point.</li>
     * </ol>
     * The driver then waits for lastApplied &gt;= readIndex before querying the state machine (step 4).
     */
    private void serviceReads(RoleState.Leader l) {
        boolean ownTermCommitted = log.termAt(commitIndex).orElse(-1) == currentTerm;
        boolean mayChooseReadIndex = ownTermCommitted
                || mutations.contains(ProtocolMutation.READ_BEFORE_CURRENT_TERM_COMMIT);   // MUTATION (test only)
        if (mutations.contains(ProtocolMutation.READ_WITHOUT_QUORUM_CHECK) && mayChooseReadIndex) {
            for (long id : l.waitingReads) notices.add(new ClientNotice.ReadReady(id, commitIndex));   // MUTATION (test only)
            l.waitingReads.clear();
            return;
        }
        if (!l.waitingReads.isEmpty() && mayChooseReadIndex
                && l.roundAckedByMajority(config.quorum()) >= l.readRound) {       // no round in flight
            l.readRound++;
            for (long id : l.waitingReads) l.readsInRound.add(new RoleState.Leader.PendingRead(id, commitIndex, l.readRound));
            l.waitingReads.clear();
            broadcastAppend(l);
        }
        long confirmed = l.roundAckedByMajority(config.quorum());
        for (Iterator<RoleState.Leader.PendingRead> it = l.readsInRound.iterator(); it.hasNext(); ) {
            RoleState.Leader.PendingRead p = it.next();
            if (p.round() > confirmed) break;                // rounds only grow along the list
            notices.add(new ClientNotice.ReadReady(p.readId(), p.readIndex()));
            it.remove();
        }
    }

    /** A leader that steps down can answer none of its reads: the client retries elsewhere. */
    private void rejectPendingReads(RoleState.Leader l) {
        for (RoleState.Leader.PendingRead p : l.readsInRound) notices.add(new ClientNotice.ReadRejected(p.readId(), Optional.empty()));
        for (long id : l.waitingReads) notices.add(new ClientNotice.ReadRejected(id, Optional.empty()));
        l.readsInRound.clear();
        l.waitingReads.clear();
    }

    // ---------------------------------------------------------------- snapshots (§7)

    /**
     * The driver saved a snapshot at index (DESIGN.md M3-E). Compact only what has been applied
     * (D1a rule 1), so nothing compacted can ever be needed for a consistency check. A snapshot that is
     * not newer than the current base (an installed one overtook it) is ignored.
     */
    private void onSnapshotCreated(Event.SnapshotCreated sc) {
        if (sc.index() <= log.snapshotIndex()) return;
        if (sc.index() > lastApplied)
            throw new IllegalArgumentException("snapshot at " + sc.index() + " beyond lastApplied " + lastApplied);
        if (log.termAt(sc.index()).orElseThrow() != sc.term())
            throw new IllegalArgumentException("snapshot term " + sc.term() + " does not match the log at " + sc.index());
        log.resetBase(sc.index(), sc.term());
        snapshot = Optional.of(new Snapshot(sc.index(), sc.term(), sc.data()));
        logOps.add(new PersistOp.CompactLog(sc.index(), sc.term()));
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
            case Message.InstallSnapshot is -> onInstallSnapshot(is);
            case Message.InstallSnapshotResponse r -> onInstallSnapshotResponse(r);
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
            // No read-round echo: this reply carries OUR term, the round belongs to the sender's older term,
            // and the leader would count it as an ack in our term (M4-B; gate seed 4811).
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(), 0, 0, 0));
            return;
        }
        followLeader(ae.from(), "AppendEntries");

        if (ae.prevLogIndex() < log.snapshotIndex()) {
            ae = skipCompactedEntries(ae);
        }
        OptionalLong prevTerm = log.termAt(ae.prevLogIndex());
        if (prevTerm.isEmpty()) {
            // Fast backup hint: our log ends before prevLogIndex.
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(),
                    0, log.lastIndex() + 1, ae.readRound()));
            return;
        }
        if (prevTerm.getAsLong() != ae.prevLogTerm()) {
            // Fast backup hint: the term we hold there and where our run of that term starts.
            send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, false, 0, ae.prevLogIndex(),
                    prevTerm.getAsLong(), log.firstIndexOfTermAt(ae.prevLogIndex()), ae.readRound()));
            return;
        }

        if (mutations.contains(ProtocolMutation.TRUNCATE_ON_MATCH)) {
            truncateAfterPrevUnconditionally(ae);                    // MUTATION (test only)
        } else {
            mergeEntries(ae.entries());
        }

        long lastNewEntry = ae.prevLogIndex() + ae.entries().size();   // >= snapshotIndex after the skip
        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(ae.leaderCommit(), lastNewEntry));
        }
        send(new Message.AppendEntriesResponse(self, ae.from(), currentTerm, true, lastNewEntry, 0, 0, 0,
                ae.readRound()));
    }

    /**
     * The sender of a valid AppendEntries or InstallSnapshot in our term is its leader: candidates step
     * down (§5.2), and the election timer resets. Two different senders in one term would break
     * Election Safety.
     */
    private void followLeader(NodeId leader, String what) {
        switch (role) {
            case RoleState.Leader l -> throw new IllegalStateException(
                    "Election Safety violated: " + self + " is leader of term " + currentTerm
                            + " but received " + what + " from " + leader + " for the same term");
            case RoleState.Candidate c -> role = new RoleState.Follower(Optional.of(leader));
            case RoleState.Follower f -> {
                if (f.leader().isPresent() && !f.leader().get().equals(leader)) {
                    throw new IllegalStateException("Election Safety violated: " + self + " saw leaders "
                            + f.leader().get() + " and " + leader + " in term " + currentTerm);
                }
                role = new RoleState.Follower(Optional.of(leader));
            }
        }
        resetElectionTimer();
    }

    /**
     * D1a rule 3: prevLogIndex lies inside our snapshot, so this is a stale or reordered message, not a
     * conflict. Entries up to snapshotIndex are committed and already in the snapshot (and in this
     * leader's log, by Leader Completeness), so drop them and treat the message as if it started at the
     * snapshot boundary, whose term we know.
     */
    private Message.AppendEntries skipCompactedEntries(Message.AppendEntries ae) {
        long base = log.snapshotIndex();
        List<LogEntry> rest = ae.entries().stream().filter(e -> e.index() > base).toList();
        return new Message.AppendEntries(ae.from(), ae.to(), ae.term(), base, log.snapshotTerm(), rest, ae.leaderCommit(),
                ae.readRound());
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
        l.ackRound(r.from(), r.readRound());                     // success or not: it accepts our term (M4-B)
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

    /**
     * Figure 2 / M2-A: send each peer the entries from its nextIndex (possibly none: a heartbeat). A peer
     * receiving a snapshot gets its current chunk again instead, which doubles as the heartbeat and
     * retransmits a lost chunk or reply.
     */
    private void broadcastAppend(RoleState.Leader l) {
        for (NodeId peer : config.peers()) {
            sendAppend(peer, l.progress(peer));
        }
    }

    private void sendAppend(NodeId peer, Progress p) {
        if (p.nextIndex() <= log.snapshotIndex()) {      // D1a rule 4: those entries are compacted away
            sendSnapshotChunk((RoleState.Leader) role, peer);
            return;
        }
        long prevIndex = p.nextIndex() - 1;
        long prevTerm = log.termAt(prevIndex).orElseThrow(
                () -> new IllegalStateException("nextIndex " + p.nextIndex() + " beyond log for " + peer));
        List<LogEntry> entries = log.entriesFrom(p.nextIndex(), config.maxEntriesPerAppend());
        send(new Message.AppendEntries(self, peer, currentTerm, prevIndex, prevTerm, entries, commitIndex,
                ((RoleState.Leader) role).readRound));
    }

    // ---------------------------------------------------------------- InstallSnapshot (§7, Figure 13)

    /**
     * Leader: send the chunk at the follower's acknowledged offset. A transfer of an older snapshot
     * (this leader has compacted again since) restarts from offset 0 with the latest one.
     */
    private void sendSnapshotChunk(RoleState.Leader l, NodeId peer) {
        Snapshot s = snapshot.orElseThrow(() -> new IllegalStateException("log compacted without a snapshot"));
        RoleState.Leader.Transfer t = l.transfer(peer).filter(x -> x.index() == s.index())
                .orElseGet(() -> new RoleState.Leader.Transfer(s.index(), s.term(), 0));
        l.setTransfer(peer, t);
        byte[] all = s.data().toByteArray();
        int from = (int) t.offset();
        int to = Math.min(all.length, from + config.snapshotChunkBytes());
        send(new Message.InstallSnapshot(self, peer, currentTerm, s.index(), s.term(), from,
                Bytes.copyOf(java.util.Arrays.copyOfRange(all, from, to)), to == all.length, l.readRound));
    }

    /**
     * Figure 13 receiver:
     * <ol>
     *   <li>Reply immediately if term &lt; currentTerm.</li>
     *   <li>(chunks) Create a new buffer at offset 0; write data at offset; reply and wait for more
     *       unless done. A chunk at the wrong offset is answered with the offset we need.</li>
     *   <li>When complete: if lastIncludedIndex &lt;= commitIndex we already have everything it holds:
     *       reply and change nothing (D1a rule 5). Otherwise install it: keep the log suffix after
     *       (lastIncludedIndex, lastIncludedTerm) if we hold that entry, else discard the log; persist;
     *       reset the state machine from the snapshot.</li>
     * </ol>
     */
    private void onInstallSnapshot(Message.InstallSnapshot m) {
        if (m.term() < currentTerm) {
            // No read-round echo for an older term's message (see onAppendEntries).
            send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), 0, false, 0));
            return;
        }
        followLeader(m.from(), "InstallSnapshot");
        boolean naive = mutations.contains(ProtocolMutation.NAIVE_INSTALL_SNAPSHOT);   // MUTATION (test only)
        if (m.lastIncludedIndex() <= commitIndex && !naive) {
            incoming = Optional.empty();
            send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), 0, true, m.readRound()));
            return;
        }
        Incoming in = incoming.filter(x -> x.index() == m.lastIncludedIndex() && x.term() == m.lastIncludedTerm())
                .orElse(null);
        if (in == null) {
            if (m.offset() != 0) {
                send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), 0, false, m.readRound()));
                return;
            }
            in = new Incoming(m.lastIncludedIndex(), m.lastIncludedTerm(), new java.io.ByteArrayOutputStream());
            incoming = Optional.of(in);
        }
        if (m.offset() != in.bytes().size()) {
            send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), in.bytes().size(), false, m.readRound()));
            return;
        }
        in.bytes().writeBytes(m.data().toByteArray());
        if (!m.done()) {
            send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), in.bytes().size(), false, m.readRound()));
            return;
        }
        incoming = Optional.empty();
        installSnapshot(new Snapshot(m.lastIncludedIndex(), m.lastIncludedTerm(), Bytes.copyOf(in.bytes().toByteArray())), naive);
        send(new Message.InstallSnapshotResponse(self, m.from(), currentTerm, m.lastIncludedIndex(), 0, true, m.readRound()));
    }

    private void installSnapshot(Snapshot s, boolean naive) {
        if (naive && log.lastIndex() > log.snapshotIndex()) {
            log.truncateFrom(log.snapshotIndex() + 1);                          // MUTATION (test only)
            logOps.add(new PersistOp.TruncateSuffix(log.snapshotIndex() + 1));
        }
        log.resetBase(s.index(), s.term());
        snapshot = Optional.of(s);
        logOps.add(new PersistOp.InstallSnapshot(s));
        if (naive) {
            commitIndex = s.index();                                             // MUTATION (test only)
        } else {
            commitIndex = Math.max(commitIndex, s.index());
        }
        lastApplied = s.index();
        snapshotToRestore = Optional.of(s);
    }

    /**
     * Leader: with one chunk in flight, a reply that moves the follower's offset forward triggers the
     * next chunk. A reply with a smaller offset (the follower restarted and lost its buffer, or an old
     * reply) resets the offset; the next heartbeat resends from there, so duplicated replies cannot
     * multiply the chunks in flight. When done, the follower holds everything through lastIncludedIndex.
     */
    private void onInstallSnapshotResponse(Message.InstallSnapshotResponse r) {
        if (!(role instanceof RoleState.Leader l) || r.term() != currentTerm) return;
        l.ackRound(r.from(), r.readRound());
        Optional<RoleState.Leader.Transfer> t = l.transfer(r.from()).filter(x -> x.index() == r.lastIncludedIndex());
        if (t.isEmpty()) return;                                            // answers an older transfer
        if (r.done()) {
            l.endTransfer(r.from());
            Progress p = l.progress(r.from());
            Progress next = new Progress(Math.max(p.nextIndex(), r.lastIncludedIndex() + 1),
                    Math.max(p.matchIndex(), r.lastIncludedIndex()));
            l.update(r.from(), next);
            maybeAdvanceCommit(l);
            if (next.nextIndex() <= log.lastIndex()) sendAppend(r.from(), next);
            return;
        }
        long offset = t.get().offset();
        l.setTransfer(r.from(), new RoleState.Leader.Transfer(t.get().index(), t.get().term(), r.nextOffset()));
        if (r.nextOffset() > offset) sendSnapshotChunk(l, r.from());
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

        Ready ready = new Ready(persist, outbox, toApply, notices, soft, snapshotToRestore);
        snapshotToRestore = Optional.empty();
        outbox.clear();
        logOps.clear();
        notices.clear();
        return ready;
    }
}
