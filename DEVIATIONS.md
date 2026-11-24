# Deviations

Every shortcut or departure from the Raft paper / thesis or from the project brief is listed here,
and the code carries a matching `// SHORTCUT:` comment where it applies.

## From the Raft protocol

| # | Where | Shortcut | Removed in |
|---|-------|----------|------------|
| P3 | `RaftNodeCore.step`, `NodeDriver.handle` | `ReadRequested` throws `UnsupportedOperationException` (and the driver throws on `ReadReady`/`ReadRejected`). It fails loudly rather than being silently ignored. Until then, reads go through the log as `KvCommand.Get`, which is linearizable but costs a log entry per read. | M4 |

Removed in M2: **P1** (heartbeats with `prevLogIndex = 0`, now real `nextIndex`/`prevLogTerm` with entries), **P2**
(no no-op on election, now appended in `becomeLeader`, §8), and the M2 half of **P3** (proposals and non-empty
AppendEntries are handled). `grep -rnE "M1 only|arrives? in M2" raft-*/src/main` returns nothing.

## From the project brief

| # | Brief said | We do | Why |
|---|------------|-------|-----|
| B1 | Java 17 | Java 21 | Owner decision. Also gives final pattern-matching `switch` over sealed types, which the exhaustive-dispatch rule needs. |
| B2 | `SimulatedNetwork` in the transport layer | Lives in `raft-sim` | It depends on the virtual clock; `raft-transport` stays free of simulation code. |
| B3 | `Transport.send(nodeId, Message)` | Messages carry `to()` | A `Ready` is self-describing; the transport reads the destination from the message. |

## Test-harness simplifications (the simulator, not the protocol)

| # | What | Why it is acceptable now | Revisit |
|---|------|--------------------------|---------|
| H1 | Simulated client requests reach nodes over a reliable 1–5 ms channel (no drops or duplicates); only node crashes and pauses affect them. Outcomes are delivered back directly. | M2 checks replica agreement, not client-observed behaviour. Lossy client links only matter together with retries and dedup. | M4 (sessions, retries, linearizability checker) |
| H2 | `RaftNodeCore.withMutationsForTesting` puts test-only `if`s into production code (DESIGN.md M2-C), now four; `WalOptions`/`StorageMutation` add two in storage (M3-F). | Production paths always use no mutations; each `if` is marked `// MUTATION (test only)`. | Kept |
| H3 | `SimDisk` models one directory, whole-byte torn writes and a simple page cache; no reordering of writes *within* a file beyond "a prefix survives". | Matches what ext4/xfs with `data=ordered` give for appends; the WAL relies only on prefix survival plus CRCs. | M5 (real `kill -9` on Docker) |

## Durability limits (M3)

| # | What | Why it is acceptable now | Revisit |
|---|------|--------------------------|---------|
| S1 | On macOS, `FileChannel.force` is `fsync(2)`, which doesn't flush the drive's write cache (`F_FULLFSYNC` does), and `RealDisk.fsyncDir` silently does nothing if the directory can't be opened as a channel. | Linux in Docker is the deployment target; the simulator tests the protocol's use of fsync independently. | M5 |
| S2 | Memory: the core holds the latest snapshot's bytes (M3-C), and `FileWalStorage` keeps its own copy of the log after the base (`LogImage`, for checkpoints). | The KV store lives in RAM anyway; both copies are bounded by snapshotting. | Kept (a production system would stream snapshots from disk) |
| S3 | Serializing a snapshot runs on the driver thread and blocks the event loop. | Fine at this size; etcd/TiKV use copy-on-write state and background snapshots. | Later, if measured to matter |
| S4 | `ProposalOutcome.Unknown`: a pending proposal covered by an installed snapshot can't be resolved, and the client must retry. | Retries become safe with M4 sessions and dedup. | M4 |

## Open (to decide at the named milestone)

- **M5:** `compose.yaml` is written but can't run yet. The node `main` and `raft-node/Dockerfile` must honour its env contract (`RAFT_NODE_ID`, `RAFT_PEERS`, `RAFT_PEER_PORT`, `RAFT_CLIENT_PORT`, `RAFT_DATA_DIR`).
- **M4:** client sessions never expire (unbounded table) vs. deterministic expiry (thesis §6.3).
