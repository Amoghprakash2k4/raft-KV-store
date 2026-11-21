# Deviations

Every shortcut or departure from the Raft paper / thesis or from the project brief is listed here,
and the code carries a matching `// SHORTCUT:` comment where it applies.

## From the Raft protocol

| # | Where | Shortcut | Removed in |
|---|-------|----------|------------|
| P1 | `RaftNodeCore.broadcastHeartbeat` | Heartbeats always send `prevLogIndex = prevLogTerm = 0` and no entries, because the leader doesn't track `nextIndex` yet. This is valid under Figure 2 (index 0 always matches), but nothing gets replicated. | M2 |
| P2 | `RaftNodeCore.becomeLeader` | A new leader doesn't append a no-op entry in its term (§8). It doesn't matter yet, since nothing commits in M1. | M2 |
| P3 | `RaftNodeCore.step` | `ProposeCommand` and `ReadRequested` throw `UnsupportedOperationException`, and a non-empty AppendEntries also throws. They fail loudly rather than being silently ignored. | M2 / M4 |

## From the project brief

| # | Brief said | We do | Why |
|---|------------|-------|-----|
| B1 | Java 17 | Java 21 | Owner decision. Also gives final pattern-matching `switch` over sealed types, which the exhaustive-dispatch rule needs. |
| B2 | `SimulatedNetwork` in the transport layer | Lives in `raft-sim` | It depends on the virtual clock; `raft-transport` stays free of simulation code. |
| B3 | `Transport.send(nodeId, Message)` | Messages carry `to()` | A `Ready` is self-describing; the transport reads the destination from the message. |

## Open (to decide at the named milestone)

- **M3:** replace the `SnapshotInstalled` event with `SnapshotCreated` (local compaction); decide where snapshot bytes live and whether InstallSnapshot is chunked.
- **M5:** `compose.yaml` is written but can't run yet. The node `main` and `raft-node/Dockerfile` must honour its env contract (`RAFT_NODE_ID`, `RAFT_PEERS`, `RAFT_PEER_PORT`, `RAFT_CLIENT_PORT`, `RAFT_DATA_DIR`).
- **M4:** client sessions never expire (unbounded table) vs. deterministic expiry (thesis §6.3).
