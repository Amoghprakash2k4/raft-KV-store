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
| H2 | `RaftNodeCore.withMutationsForTesting` puts three test-only `if`s into production code (DESIGN.md M2-C). | The public constructor always uses no mutations; each `if` is marked `// MUTATION (test only)`. | Kept |

## Open (to decide at the named milestone)

- **M3:** replace the `SnapshotInstalled` event with `SnapshotCreated` (local compaction); decide where snapshot bytes live and whether InstallSnapshot is chunked.
- **M5:** `compose.yaml` is written but can't run yet. The node `main` and `raft-node/Dockerfile` must honour its env contract (`RAFT_NODE_ID`, `RAFT_PEERS`, `RAFT_PEER_PORT`, `RAFT_CLIENT_PORT`, `RAFT_DATA_DIR`).
- **M4:** client sessions never expire (unbounded table) vs. deterministic expiry (thesis §6.3).
