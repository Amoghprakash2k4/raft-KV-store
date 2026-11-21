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
