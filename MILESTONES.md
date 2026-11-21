# Milestones

## M1: Leader election ✅ done (2026-11-22)

**Exit criterion:** invariant 1 (Election Safety) holds over 5,000 seeds, with partitions and crashes.

**Result:** 5,000 seeds × {3, 5} nodes with 0 violations. Each cluster size saw about 56k partitions
(some one-way), 28k crash/restarts and 27k pauses. Liveness held after healing in every run.
Mutation check: disks that lie about fsync produced two leaders in one term in 70 of 500 seeds, and the
checkers caught every one.

Replay: `./gradlew :raft-sim:test -Psim.seeds=5000` (gate), `-Psim.seed=N` (one seed).

---

## M2: Log replication, entry checklist

M2 is not done until every box is ticked.

### Remove the M1 shortcuts (DEVIATIONS.md)
- [ ] **P1:** heartbeats/AppendEntries use real `nextIndex[p]`: `prevLogIndex = nextIndex - 1`, `prevLogTerm = termAt(prevLogIndex)`, and send entries (Figure 2).
- [ ] **P2:** a new leader appends a no-op entry in its own term on election (§8). Without it, the leader can't commit entries from earlier terms (Figure 8).
- [ ] **P3:** `ProposeCommand` is handled by appending to the leader's log and emitting `ProposalAccepted`, or `ProposalRejected` with a leader hint on a non-leader. Non-empty AppendEntries are processed, not thrown. (`ReadRequested` stays loud until M4 and moves to the M4 checklist.)
- [ ] `NodeDriver` applies `Ready.toApply` to the `KvStateMachine` (step 3 of E1) instead of throwing.
- [ ] `grep -rnE "M1 only|arrives? in M2" raft-*/src/main` returns nothing, and P1 and P2 are deleted from DEVIATIONS.md (P3 shrinks to the M4 part). Today the grep finds 6 lines in `RaftNodeCore` and `NodeDriver`: the P1 and P2 shortcuts, the proposal throw, the non-empty AppendEntries throw and its Javadoc, and the driver's apply throw.

### New invariant checkers (separate classes, separate failure messages)
- [ ] **Log Matching (invariant 3, §5.3):** if two logs have the same (index, term), their prefixes up to that index are identical.
- [ ] **Leader Completeness (invariant 4, §5.4):** every committed entry is in the log of every leader of a later term. In M1 this was untestable because logs were empty.
- [ ] **State Machine Safety (invariant 5, §5.4.3):** no two nodes apply different entries at the same index.
- [ ] **Leader Append-Only (invariant 2):** a leader never overwrites or deletes entries in its own log.
- [ ] Election Safety stays its own checker. Leader Completeness is a different property, so it gets its own checker and its own failure message rather than being folded into Election Safety.

### Log up-to-date check under real load
- [ ] The election restriction (§5.4.1) is exercised by chaos runs with non-empty, diverging logs. In M1 it was covered only by unit tests.
- [ ] **Mutation:** skipping the log up-to-date check in `onRequestVote` is caught by Leader Completeness / State Machine Safety in some seed.
- [ ] **Mutation:** committing an entry from an older term by counting replicas (the Figure 8 bug) is caught.

### Unit scenarios from the paper
- [ ] The Figure 7 follower logs (a)–(f) all converge to the leader's log.
- [ ] The Figure 8 sequence: an old-term entry on a majority is not committed until a current-term entry is.
- [ ] Reordered or duplicated AppendEntries never truncate entries that already match.
- [ ] Reordered AppendEntriesResponses never move `matchIndex` backwards.

### Then
- [ ] Fast log backup (conflictTerm / conflictIndex), added only after the basic version passes.
- [ ] Gate: invariants 1–5 hold over 5,000 seeds with drops, duplicates, reordering, partitions and crashes.
