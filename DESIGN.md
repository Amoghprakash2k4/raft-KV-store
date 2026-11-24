# Design decisions

Each entry: the decision, the alternatives we rejected, and why.

## D1. Durability and the core's in-memory log

**Decision.** Each node persists its own state through the `Storage` interface:
- hard state: `currentTerm` and `votedFor`
- the log entries
- a snapshot of the state machine (the sequence of applied commands up to `lastIncludedIndex`), from M3

On restart, `Storage.loadAll()` returns a `PersistentState`. The core is rebuilt from it and keeps an
in-memory copy of the log. While running, the core reads only that copy, and Storage is write-only
until the next restart. Snapshots bound how big the copy can get.

**Rejected.** The core reading entries through a synchronous `Storage` interface (etcd style). It uses
less memory, but it puts I/O and its failure modes inside the deterministic core.

**Why.** It keeps the core pure. Durability is a property of the driver: it persists first, then sends.
The core doesn't need to know about it.

### D1a. Snapshot boundary rules (M3)

The in-memory log is `(snapshotIndex, snapshotTerm, entries[snapshotIndex+1 ..])`.

1. **Compact only what has been applied.** The core trims entries `<= lastIncludedIndex` only when
   `lastIncludedIndex <= lastApplied`. Applied implies committed, so nothing it trims can ever be needed
   for a conflict check.
2. **The boundary still has a term.** `termAt(snapshotIndex)` returns `snapshotTerm`, so an
   AppendEntries with `prevLogIndex == snapshotIndex` passes the consistency check normally (Fig. 2).
   `termAt(i)` for `i < snapshotIndex` is empty, meaning compacted and not a mismatch.
3. **Follower gets `prevLogIndex < snapshotIndex`.** This is a stale or reordered AppendEntries, not a
   conflict. Skip the incoming entries with index `<= snapshotIndex` (they're committed and already in
   the snapshot) and process the rest. Never truncate below the snapshot.
4. **Leader with `nextIndex[p] <= snapshotIndex`.** The entries it would send are gone, so it sends
   InstallSnapshot instead of AppendEntries (§7).
5. **Follower receives InstallSnapshot** (Fig. 13):
   - If `lastIncludedIndex <= ` its own `commitIndex`, it's stale: reply and change nothing.
   - If its log has an entry at `lastIncludedIndex` with `lastIncludedTerm`, keep the suffix after it.
   - Otherwise discard the whole log.
   - Then persist the snapshot and the new log base, then restore the state machine.
6. `PersistentState` validation changes accordingly: the log starts at `snapshotIndex + 1`, and term
   monotonicity starts from `snapshotTerm`.

Still open for M3: where the snapshot bytes live while the leader streams them (held by the core vs.
read by the driver), and whether to chunk them.

## E1. Effect protocol: `step(event) -> Ready`

**Decision.** `RaftCore.step(event)` updates the core's in-memory state and returns a `Ready`.
The driver executes the `Ready` before it feeds the next event, in this order:
1. persist all ops, then fsync
2. send messages
3. apply committed entries
4. notify clients

Group commit (M6) keeps the same guarantee: the driver joins several `Ready`s, fsyncs once, and only
then sends their messages.

**Rejected.** An etcd-style `Ready()` / `Advance()` handshake. It allows sending while an fsync is
still in progress (thesis §10.2.1), but it has more states and the driver can misuse it.

**Why.** It's the simplest protocol in which "nothing leaves the node before it is durable" holds by
construction. Its one requirement is that the driver is single-threaded.

### E1a. Driver rules

1. **One mailbox.** Network threads, timers and client handlers never call the core. They only enqueue
   events into the event loop's FIFO queue. While a `Ready` is executing, new events wait in the queue,
   because nothing else can reach `step()`.
2. **A failed fsync stops the node.** If an fsync fails, the node stops processing (fail-stop: it exits,
   or crashes in the simulator) and recovers from disk on restart. It does not "skip sending and carry
   on", for two reasons:
   - The core has already advanced its in-memory state (it believes `votedFor = X` is durable), so it
     now disagrees with the disk.
   - After a failed fsync, Linux may discard the dirty pages, so a retried fsync can report success
     without the data being written ("fsyncgate", PostgreSQL 2018).

   The only safe recovery is to reload from what is actually on disk.
3. **Every message waits for the fsync**, including heartbeats that don't depend on new state. Sending
   them earlier would be an optimisation; it is not done in v1.
4. **The state machine is deterministic.** `apply` may depend only on the entry and the current state:
   - no wall clock, no randomness, no local configuration
   - anything time-based, such as session expiry, uses data carried in the entry
   - snapshot serialisation is canonical (sorted keys, e.g. TreeMap, never HashMap iteration order)

   Then two nodes at the same `lastApplied` produce byte-identical snapshots, and the simulator can
   assert exactly that.
5. **A snapshot is taken at exactly `lastApplied`,** between two `Ready`s, and includes the client
   session table. In v1 it is taken synchronously on the loop thread. That blocks the loop, which only
   affects liveness, so it will be marked `// SHORTCUT:`; copy-on-write snapshots are a possible M6 item.

## M2. Log replication decisions (approved 2026-11-22)

### M2-A. Flow control: send on proposal and on heartbeat; `nextIndex` moves only on replies
The leader sends AppendEntries (up to `maxEntriesPerAppend` entries from `nextIndex[p]`) to every peer
when a proposal is appended and on every heartbeat. `nextIndex` changes only when a reply arrives:
success raises it to `matchIndex + 1`, a rejection that answers the latest probe lowers it.
**Rejected:** etcd-style optimistic pipelining (Probe/Replicate modes, in-flight windows): much more
state and many more edge cases. Heartbeat-only sending: adds up to one heartbeat interval to every commit.
**Cost:** a slow follower can receive the same batch several times until it acks; throughput is about
one batch per round trip. Revisit in the M6 benchmarks.

### M2-B. Command encoding: hand-written binary codec
`DataOutputStream`/`DataInputStream`: a type byte, then fixed-width numbers and length-prefixed byte
strings. No dependencies, canonical (equal values give identical bytes, needed for M3 snapshot
comparison), and every byte is visible. Property-tested: `decode(encode(x)) == x`.
**Rejected:** protobuf now (build complexity before M5, and its encoding is not guaranteed canonical);
Java serialization (security risk, fragile, not a stable format).

### M2-C. Mutation hooks: a test-only `Set<ProtocolMutation>`
`RaftNodeCore.withMutationsForTesting(config, state, rng, mutations)` builds a core that breaks the named
rules. The public constructor always passes an empty set. Each mutation is one marked `if` at the
rule it breaks. **Rejected:** subclass overrides (non-final classes, `this-escape` lint); PIT
(slow, untargeted; possible M6 extra).

### M2-D. Simulated clients: open loop, no retries
Simulated clients propose random PUT/DELETE/CAS commands every 20–100 ms to the node they believe is
leader, follow rejection hints, and never retry an unknown outcome. M2 checks that replicas agree
(invariants 2–5); what clients observe (linearizability) is M4, together with retries and sessions.

### M2-E. Proposal bookkeeping in the driver
The core emits `ProposalAccepted(proposalId, index, term)`. The driver remembers `index → (proposalId,
term)`. When the entry at `index` is applied: same term means the proposal committed and gets the state
machine's result; a different term means it was overwritten by another leader and is answered
"lost, retry" (thesis §6.3). The command envelope carries `clientId` and `seq` from day one so that M4's
dedup does not change the log format.

## M3. Durability decisions (approved 2026-11-24)

### M3-A. One write-ahead log with typed records
Each node has one WAL: segment files `wal-<seq>.log` holding records
`[length u32][crc32c u32][type u8][payload]` of type HARD_STATE, ENTRIES, TRUNCATE, SNAPSHOT_MARK and
RESET (the first record of a checkpoint segment). Hard state lives in the same log as entries, so the
one fsync per `Ready` (E1) covers everything. Truncation is a logical record; bytes are never rewritten
in place, except to cut a torn tail. Recovery replays all segments: a bad record in the **last**
segment is a torn tail (cut it, fsync, stop); a bad record anywhere else is corruption (fail-stop).
After a snapshot, a checkpoint segment (RESET + HARD_STATE + retained entries) is written to a temp
file, fsynced, renamed, and the directory fsynced; only then are older segments deleted.
**Rejected:** a separate hard-state file (two fsyncs and a directory fsync per Ready); an embedded
database (hides what we are learning).

### M3-B. Crash testing: the real WAL on a simulated disk
`Disk` is a small file API (append, read, truncate, fsync, rename, delete, list, fsyncDir).
`RealDisk` uses `FileChannel`; `SimDisk` models a page cache: unsynced bytes are lost on crash, or a
random prefix of them survives (a torn write); a file name created, renamed or deleted becomes durable
only after `fsyncDir`; "crash during fsync" persists a random prefix of the pending bytes and fails.
The simulator runs the real `FileWalStorage` on `SimDisk`, so every seed exercises recovery.
**Rejected (for now):** only real files and kill -9 (slow and nondeterministic; that is M5's compose
test).

### M3-C. The core holds the latest snapshot's bytes
The core receives snapshot bytes through `SnapshotCreated`, a completed InstallSnapshot, or recovery,
and builds InstallSnapshot chunks from them. Pure and deterministic; the cost is one snapshot copy in
memory (a scale limit, listed in DEVIATIONS.md).
**Rejected:** the driver reading the snapshot file on demand (scales better, but message building leaks
into the driver).

### M3-D. Chunked InstallSnapshot (Figure 13)
Chunks carry `offset`, `data`, `done`; one chunk is in flight per follower and the reply carries how
many bytes the follower holds. 64 KB chunks in production, 64 bytes in the simulator so every install
is multi-chunk. The follower assembles chunks in volatile memory and restarts on an offset mismatch.

### M3-E. Snapshot every N applied entries, decided by the driver
Between two `Ready`s, when `lastApplied - snapshotIndex >= snapshotEvery`, the driver serializes the
state machine at exactly `lastApplied`, saves the snapshot file atomically (tmp, fsync, rename, fsync
dir), and only then tells the core (`SnapshotCreated`), which compacts its log and emits `CompactLog`.
Production 10,000; simulator 30-100.

### M3-F. Mutations that must be caught
Lying fsync (on the WAL now), recovery that does not cut the torn tail, a snapshot renamed into place
without fsyncing its data first, and a naive follower InstallSnapshot (no stale check, always discards
the log). Each one must be caught by a checker, with measured rates and a clean control run.
