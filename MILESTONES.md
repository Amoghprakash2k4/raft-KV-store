# Milestones

## M1: Leader election ✅ done (2026-11-22)

**Exit criterion:** invariant 1 (Election Safety) holds over 5,000 seeds, with partitions and crashes.

**Result:** 5,000 seeds × {3, 5} nodes with 0 violations. Each cluster size saw about 56k partitions
(some one-way), 28k crash/restarts and 27k pauses. Liveness held after healing in every run.
Mutation check (disks that lie about fsync): the vote-durability checker caught it in 50 of 50 seeds,
on average 2.6 s into the run. With that checker off, the bug produced two leaders in one term in 70 of
500 seeds, and the Election Safety checker caught every one.

Replay: `./gradlew :raft-sim:test -Psim.seeds=5000` (gate), `-Psim.seed=N` (one seed).

---

## M2: Log replication ✅ done (gate passed 2026-11-22, accepted by the owner 2026-11-23)

**Exit criterion:** invariants 1–5 hold under drops, duplicates, reordering, partitions and crashes.

**Result:** 5,000 seeds × {3, 5} nodes with 0 violations of Election Safety, Leader Append-Only, Log
Matching, Leader Completeness and State Machine Safety, plus vote durability, replica KV-state agreement
and liveness (a leader exists, commands still commit, and every node catches up). Per cluster size:
about 28k crash/restarts, 56k partitions, 26–27k pauses, 3.24M client proposals, 2.0M (3 nodes) / 2.4M
(5 nodes) committed, ~127k proposals lost to overwriting, logs up to ~630 entries. Gate runtime ~90 s.

Mutation checks (each must be caught; measured rates):

| Mutation | Seeds | Caught by |
|---|---|---|
| Skip the log up-to-date check when voting (§5.4.1) | 100 | Leader Completeness 97 |
| Commit an old-term entry by counting (Figure 8), batch size 1 | 1000 | Leader Completeness 11 (Liveness 18, unrelated: slow 1-entry catch-up) |
| Same, batch size 64 | 200 | 0: the no-op travels with the old entries, so the bad window never opens |
| Truncate even when entries match (§5.3) | 100 | Leader Completeness 49, replica state divergence 47, state machine order guard 4 |
| Lying disks (M1), vote checker on | 50 | Vote durability 50 |
| Lying disks, vote checker off | 300 | Leader Completeness 148, Election Safety 26, Log Matching 10, State Machine Safety 4 |
| Control: no mutation, batch size 1 | 100 | no safety violations (1 slow-catch-up liveness miss) |

The basic version (decrement nextIndex by one) was safe on its own (0 failures over 500 seeds × {3, 5}
with a 20 s quiet period), but too slow to catch up within the normal 3 s window; fast backup fixed that.

Re-run on 2026-11-23 before committing: build green, gate identical (same counts, so runs stay
deterministic).

Replay: `./gradlew :raft-sim:test -Psim.seeds=5000` (gate), `-Psim.seed=N` (one seed).

### Entry checklist (all done)

#### Remove the M1 shortcuts (DEVIATIONS.md)
- [x] **P1:** heartbeats/AppendEntries use real `nextIndex[p]`: `prevLogIndex = nextIndex - 1`, `prevLogTerm = termAt(prevLogIndex)`, and send entries (Figure 2).
- [x] **P2:** a new leader appends a no-op entry in its own term on election (§8). Without it, the leader can't commit entries from earlier terms (Figure 8).
- [x] **P3:** `ProposeCommand` is handled by appending to the leader's log and emitting `ProposalAccepted`, or `ProposalRejected` with a leader hint on a non-leader. Non-empty AppendEntries are processed, not thrown. (`ReadRequested` stays loud until M4 and moves to the M4 checklist.)
- [x] `NodeDriver` applies `Ready.toApply` to the `KvStateMachine` (step 3 of E1) instead of throwing.
- [x] `grep -rnE "M1 only|arrives? in M2" raft-*/src/main` returns nothing, and P1 and P2 are deleted from DEVIATIONS.md (P3 shrinks to the M4 part). Done: the grep returns nothing.

#### New invariant checkers (separate classes, separate failure messages)
- [x] **Log Matching (invariant 3, §5.3):** if two logs have the same (index, term), their prefixes up to that index are identical.
- [x] **Leader Completeness (invariant 4, §5.4):** every committed entry is in the log of every leader of a later term. In M1 this was untestable because logs were empty.
- [x] **State Machine Safety (invariant 5, §5.4.3):** no two nodes apply different entries at the same index.
- [x] **Leader Append-Only (invariant 2):** a leader never overwrites or deletes entries in its own log.
- [x] Election Safety stays its own checker. Leader Completeness is a different property, so it gets its own checker and its own failure message rather than being folded into Election Safety.

#### Log up-to-date check under real load
- [x] The election restriction (§5.4.1) is exercised by chaos runs with non-empty, diverging logs. In M1 it was covered only by unit tests.
- [x] **Mutation:** skipping the log up-to-date check in `onRequestVote` is caught by Leader Completeness / State Machine Safety in some seed.
- [x] **Mutation:** committing an entry from an older term by counting replicas (the Figure 8 bug) is caught.

#### Unit scenarios from the paper
- [x] The Figure 7 follower logs (a)–(f) all converge to the leader's log.
- [x] The Figure 8 sequence: an old-term entry on a majority is not committed until a current-term entry is.
- [x] Reordered or duplicated AppendEntries never truncate entries that already match.
- [x] Reordered AppendEntriesResponses never move `matchIndex` backwards.

#### Then
- [x] Fast log backup (conflictTerm / conflictIndex), added only after the basic version passes.
- [x] Gate: invariants 1–5 hold over 5,000 seeds with drops, duplicates, reordering, partitions and crashes.

### Carried to M4
- [ ] `ReadRequested` / ReadIndex (P3 remainder). Client retries, sessions and dedup; lossy client links (H1).

---

## M3: Durability ✅ done (gate passed and accepted by the owner 2026-11-24)

**Exit criterion:** invariant 6 (Durability) holds under random crash loops, including crashes in the
middle of a write; invariants 1–5 keep holding.

**Built:** a segmented WAL with CRC32C records, torn-tail cut and checkpoints (`FileWalStorage`) on a
`Disk` abstraction (`RealDisk`, and `SimDisk` with a page-cache crash model); atomic snapshot files
(`SnapshotStore`); a log base in the core (D1a rules 1–5); driver-triggered snapshots
(`SnapshotCreated`); chunked InstallSnapshot (Figure 13); recovery from snapshot + WAL. Every simulated
node now runs the real WAL on a `SimDisk`, and every restart runs real recovery.

**Result:** 5,000 seeds × {3, 5} nodes × {chaos, crash loop} with 0 violations of invariants 1–6, vote
durability, replica-state agreement and liveness. Runtime 2 min 31 s.

| Config | Crashes | mid-fsync | Torn tails cut | Restarts checked | Snapshots | Installs | Max log in memory |
|---|---|---|---|---|---|---|---|
| chaos, 3 | 44,904 | 22,326 | 8,619 | 59,904 | 71,782 | 14,167 | 189 |
| chaos, 5 | 46,656 | 23,194 | 8,966 | 71,656 | 153,018 | 35,673 | 190 |
| crash loop, 3 | 301,100 | 140,956 | 53,392 | 316,100 | 43,782 | 5,631 | 120 |
| crash loop, 5 | 324,898 | 159,198 | 61,240 | 349,898 | 115,943 | 25,879 | 137 |

The first gate run failed on one seed (crash loop, 5 nodes, seed 327). It was a false positive in the
new committed-entry check: it also flagged a node still in an older term, which Raft allows to lose an
entry committed later without it (Figure 8). The check now applies only to nodes at or past the commit
term (EXPLANATION.md M3 §11).

Mutation checks (measured rates):

| Mutation | Config, seeds | Caught by |
|---|---|---|
| Lying fsync (on the real WAL) | chaos, 50 | Durability 50 |
| Same, durability checkers off | chaos, 300 | Leader Completeness 212, Election Safety 43, Log Matching 18, State Machine Safety 2 (passed 20, liveness 5) |
| Recovery keeps the torn tail | crash loop, 100 | Durability 100 |
| Snapshot renamed before its data is fsynced | crash loop, 100 | Durability 59 (needs a crash that loses some of the unsynced bytes) |
| Follower installs stale snapshots / always discards its log | chaos, 100 | Durability (c) 98, RaftLog guard 1 |
| M2 truncate on match (re-measured) | chaos, 100 | Durability (c) 100 |
| Controls (batch 1; crash loop) | 100 + 100 | no safety violations |

`./gradlew build`: 250 tests, ~20 s.

### Checklist
- [x] `Disk`, `RealDisk`, `SimDisk` (crash model tested on its own)
- [x] WAL: records + CRC32C, segments, torn-tail cut, fail-stop on older-segment corruption, every-offset property test
- [x] Simulator on the real WAL; crash-during-fsync faults; crash-loop config; `DurabilityChecker` (a), (b), (c)
- [x] Snapshots: canonical KV snapshot, atomic `SnapshotStore`, log base, `SnapshotCreated` / `CompactLog`, checkpoints, recovery merge
- [x] InstallSnapshot: chunks, offsets, retransmit on heartbeat, stale-install guard, keep vs discard
- [x] Mutations M3-F caught, with control runs
- [x] Gate over 5,000 seeds
- [x] Docs: DESIGN, DEVIATIONS, CLAUDE.md, EXPLANATION.md `# M3`
