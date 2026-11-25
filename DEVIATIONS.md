# Deviations

Every shortcut or departure from the Raft paper / thesis or from the project brief is listed here,
and the code carries a matching `// SHORTCUT:` comment where it applies.

## From the Raft protocol

None open. `grep -rn "SHORTCUT:\|arrives in M" raft-*/src/main` returns nothing.

Removed in M2: **P1** (heartbeats with `prevLogIndex = 0`, now real `nextIndex`/`prevLogTerm` with entries), **P2**
(no no-op on election, now appended in `becomeLeader`, §8), and the M2 half of **P3** (proposals and non-empty
AppendEntries are handled). Removed in M4: the rest of **P3** (`ReadRequested` threw; it is now ReadIndex,
thesis §6.4, DESIGN.md M4-B).

## Skipped in M4 on purpose (owner decision 2026-11-24)

Full reasons, diagrams and interview answers for each: EXPLANATION.md M4 §2 (what and why) and §3.2
(follow-up questions with answers).

None of these affects safety: no write is lost or applied twice, no read is stale. Each is explained in
detail, with diagrams, in EXPLANATION.md M4 §2.

| # | Thesis feature | What we do instead | Cost | Effort to add |
|---|----------------|--------------------|------|---------------|
| K1 | Session expiry (§6.3.2) | The session table is unbounded. | Memory grows with the number of distinct clients ever seen. | High: leader timestamps in entries, expiry at apply time, a "session expired" error. |
| K2 | RegisterClient (§6.3) | Clients choose their own id (1..5 in the simulator; a random 64-bit id in production). | Ids unique by randomness, not by construction. | Low. |
| K3 | Lease-based reads (§6.4.1) | Every read batch costs one ReadIndex round. | About one round trip per read batch. | Medium, and it would make safety depend on clocks (the brief forbids that). |
| K4 | Reads served by followers (§6.4) | Followers reject reads with a leader hint. | The leader serves every read. | Low: reuses ReadIndex plus one message. |
| K5 | Several requests in flight per client | One operation in flight per client; dedup keeps only the last seq. | Per-client throughput. | High: a window of seqs and replies, plus client acks. |

## From the project brief

| # | Brief said | We do | Why |
|---|------------|-------|-----|
| B1 | Java 17 | Java 21 | Owner decision. Also gives final pattern-matching `switch` over sealed types, which the exhaustive-dispatch rule needs. |
| B2 | `SimulatedNetwork` in the transport layer | Lives in `raft-sim` | It depends on the virtual clock; `raft-transport` stays free of simulation code. |
| B3 | `Transport.send(nodeId, Message)` | Messages carry `to()` | A `Ready` is self-describing; the transport reads the destination from the message. |

## Test-harness simplifications (the simulator, not the protocol)

| # | What | Why it is acceptable now | Revisit |
|---|------|--------------------------|---------|
| H2 | `RaftNodeCore.withMutationsForTesting` puts test-only `if`s into production code (DESIGN.md M2-C), now six; `WalOptions`/`StorageMutation` add two in storage (M3-F); `KvStateMachine.withoutDedupForTesting` adds one (M4-F). | Production paths always use no mutations; each `if` is marked `// MUTATION (test only)`. | Kept |
| H3 | `SimDisk` models one directory, whole-byte torn writes and a simple page cache; no reordering of writes *within* a file beyond "a prefix survives". | Matches what ext4/xfs with `data=ordered` give for appends; the WAL relies only on prefix survival plus CRCs. M5 adds real `kill -9` runs (`KillNineIntegrationTest`, Docker). A SIGKILL keeps the OS page cache, so it tests process crashes, not power loss. | Kept (power loss needs a VM or a fault-injecting filesystem) |
| H4 | The two ReadIndex mutations are measured under a leader-targeted fault mix (`FaultConfig.leaderHunting`) and a read-heavy workload, not the standard chaos mix. | A bad ReadIndex is stale only in the short window around a leadership change; the standard mix catches it in 7–14% of seeds, the targeted one in 20–75%. Same idea as batch size 1 for Figure 8. Control runs use the same configs. | Kept |
| H5 | Invariant 7 is checked once, at the end of each run, not after every event. | It needs the whole history; the other invariants still run after every event. | Kept |
| H6 | Client liveness ("every client completed something in the last N ms") uses N = 2 s; the cluster liveness checks keep 1 s. | Client links keep losing 5% of messages in the quiet period, so one client can be unlucky for a while. Measured over 4 × 5,000 seeds: the slowest client's idle time at the end has p99.9 ≈ 790 ms and max 1,089 ms, the same with PreVote/CheckQuorum on or off. The M5 gate failed once at 1 s (chaos, 5 nodes, seed 3064: five lost round trips in a row to a stable leader, idle 1,089 ms). Pinned in `RegressionSeedsTest`. | Kept |

Removed in M4: **H1** (client links were reliable; they are now lossy, DESIGN.md M4-A).

## Durability limits (M3)

| # | What | Why it is acceptable now | Revisit |
|---|------|--------------------------|---------|
| S1 | On macOS, `FileChannel.force` is `fsync(2)`, which doesn't flush the drive's write cache (`F_FULLFSYNC` does), and `RealDisk.fsyncDir` silently does nothing if the directory can't be opened as a channel. | Linux in Docker is the deployment target (compose uses named volumes on the Linux VM's disk, not macOS bind mounts); the simulator tests the protocol's use of fsync independently. | Kept |
| S2 | Memory: the core holds the latest snapshot's bytes (M3-C), and `FileWalStorage` keeps its own copy of the log after the base (`LogImage`, for checkpoints). | The KV store lives in RAM anyway; both copies are bounded by snapshotting. | Kept (a production system would stream snapshots from disk) |
| S3 | Serializing a snapshot runs on the driver thread and blocks the event loop. | Fine at this size; etcd/TiKV use copy-on-write state and background snapshots. | Later, if measured to matter |

Removed in M4: **S4** (`ProposalOutcome.Unknown` could not be retried safely; with sessions the client retries
with the same seq and dedup makes it exactly-once).

## Not built in M5 (production concerns outside the brief)

| # | What | What we do instead | Cost |
|---|------|--------------------|------|
| R1 | TLS and authentication between nodes and for clients | Plaintext gRPC and HTTP on a private Docker network | Anyone on the network can read or send messages, including fake Raft messages |
| R2 | Membership changes (joint consensus or single-server changes, thesis ch. 4) | Membership is fixed by `RAFT_PEERS` at start | Replacing a machine means a full cluster restart with a new config |
| R3 | Back-pressure to clients beyond "mailbox full → 503" | Bounded mailbox and per-peer queues; clients retry | Under overload, latency is not shaped; requests are refused |
| R4 | Leadership transfer (thesis §3.10) | A leader keeps its role until it fails or loses quorum | Planned restarts of the leader cost one election timeout of unavailability |

## Open

Nothing. M6 (benchmarks and report) is next.
