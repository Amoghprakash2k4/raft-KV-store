# CLAUDE.md: context for continuing this project

Read this first in every new conversation. Then read `PROJECT_BRIEF.md` (the owner's full instructions),
`MILESTONES.md` (status and the next checklist), `DESIGN.md` (decisions) and `DEVIATIONS.md` (shortcuts).

## What this is
A Raft-based replicated key-value store in plain Java 21, built from scratch as a **learning project**
for the owner: a new grad with about 6 months of Java/Spring experience who wants to deeply understand
consensus and end up with an interview-grade project. You act as a **senior distributed-systems mentor**.

## Status (last updated 2026-11-25)
- **M1 Leader Election: DONE.** Gate passed: 0 Election Safety violations over 5,000 seeds × {3, 5} nodes.
- **M2 Log Replication: DONE** (accepted by the owner 2026-11-23). 0 violations of invariants 1–5 over
  5,000 seeds × {3, 5} nodes; all mutation checks caught (rates in MILESTONES.md).
- **M3 Durability: DONE** (accepted by the owner 2026-11-24). 0 violations of invariants 1–6 over
  5,000 seeds × {3, 5} × {chaos, crash loop}; all M3-F mutations caught (rates in MILESTONES.md).
- **M4 Clients and reads: DONE** (accepted by the owner 2026-11-25, committed on `m4`, merged to `main`, pushed).
  The gate found a real ReadIndex bug (seed 4811: a stale-term rejection echoed a read round); fixed.
  Sessions + dedup, `ClientSession` (raft-client), ReadIndex, linearizability checker (invariant 7),
  lossy client links, three mutations caught. Gate numbers in MILESTONES.md.
- **M5 Hardening: built and verified, NOT yet accepted or committed** (branch `m5`). PreVote + CheckQuorum
  + stickiness (flags), gRPC transport, `raft-server` (event loop, HTTP API, metrics), `raft-cli`,
  kill -9 integration test (6 runs, 162 kills, all linearizable), Docker compose verified by hand. Gate
  passed with hardening on after seed 3064 (client-liveness window too tight → H6, 2 s, measured).
- **Git:** history rewritten 2026-11-24 (author → "Amogh P"): M1 `a645fa8`, docs `069cc15`, M2 `ec88c75`, M3 `be439fc`. M2 committed and pushed to `origin/main` on 2026-11-23, M3 on
  2026-11-24 (the owner asked each time). M4 work happens on the `m4` branch. Never commit or push without an explicit yes from the owner.

## How the owner wants to work (learned from the conversation)
1. **One milestone at a time.** Don't start the next one until the owner says the current one is done.
2. **Ask before design decisions and before choosing algorithms or approaches for a milestone.** The
   owner said explicitly: "before starting election concepts and choosing algorithms ask me". Present
   2–4 options with a recommendation first (AskUserQuestion works well). The owner often replies
   "accept all".
3. **The owner asks lots of conceptual follow-up questions** ("how is a duplicate vote detected?", "what
   does the vote checker do?"). Answer in plain language, with concrete examples, real traces from the
   simulator when possible, and `file:line` references. Correct misconceptions directly (for example,
   the owner thought "higher term steps down" meant two quorum winners comparing terms).
4. **Measure, don't guess.** When stating a number, run it. Once I wrote "nearly every run", and the
   owner caught that it was unmeasured.
5. **Keep `EXPLANATION.md` updated, with diagrams.** The owner asked for neat, self-explanatory diagrams:
   Mermaid for flows/sequences/states (GitHub renders them), aligned ASCII for log layouts. At the end of each milestone, append a section
   (`# M<n>: <title>`) explaining in detail everything taught in that phase: concepts, design, code
   walkthrough, tests, results, shortcuts, study questions with answers, glossary additions. The owner
   uses it to re-learn later.
6. Follow the response format in `PROJECT_BRIEF.md` when building: why → invariants → design → tests
   first → implementation → gaps/deviations → what to study.
7. **Java 21**, not 17 (the owner overrode the brief, which is recorded in DEVIATIONS.md B1).

## Decisions already made (don't re-ask)
- **D1:** each node persists its hard state, log and (from M3) snapshots through `Storage`. The core
  keeps an in-memory copy of the log and never reads storage while running.
- **E1:** `RaftCore.step(Event) → Ready`. The driver executes it in this order: persist + one fsync →
  send → apply → notify, before the next event. A failed fsync means fail-stop (`NodeHaltedException`).
- **D1a / E1a:** snapshot boundary rules for M3, and driver rules (single mailbox, fail-stop, canonical
  serialization). See DESIGN.md.
- **M1 choices:** sealed `RoleState` objects per role; the simulator runs the real `NodeDriver` +
  `InMemoryStorage`; a new random election timeout on every reset; network = uniform delay + 1% long
  delays (10–50×) + 5% drop + 2% duplicate + directed (one-way) partitions.
- **compose.yaml:** a 3-node cluster of our own nodes (target for M5). No external database: the Raft
  store is the database.
- **M2 choices (approved):** M2-A send on proposal + heartbeat, nextIndex moves only on replies;
  M2-B hand-written binary codec; M2-C test-only `Set<ProtocolMutation>`; M2-D open-loop sim clients
  without retries; M2-E driver matches `ProposalAccepted(index, term)` at apply time. Fast backup is
  added after the basic version passes.
- **M4 choices (approved 2026-11-24):** scope = only the important parts; skipped on purpose (DEVIATIONS
  K1–K5): session expiry (table unbounded), RegisterClient, lease reads, follower reads, pipelining.
  M4-A pure `ClientSession` + closed-loop sim clients + lossy client links; M4-B ReadIndex with a
  `readRound` echoed on AppendEntries/InstallSnapshot responses (one round in flight, batching); M4-C
  sessions in `KvStateMachine` (snapshot v2, in `stateHash`, client 0 = no session); M4-D Wing & Gong +
  Lowe memo, per key, unknown ops optional, 20M-step budget; M4-E one record per op, unique values;
  M4-F mutations SKIP_DEDUP / READ_WITHOUT_QUORUM_CHECK / READ_BEFORE_CURRENT_TERM_COMMIT under
  `FaultConfig.leaderHunting`.
- **M3 choices (approved):** M3-A one WAL with typed records + CRC32C, torn-tail cut, checkpoint
  segments; M3-B real WAL on `SimDisk` in the simulator; M3-C core holds the latest snapshot bytes;
  M3-D Figure 13 chunks (64 B in sim); M3-E driver snapshots every N applied entries
  (`SnapshotCreated` replaces the brief's `SnapshotInstalled`); M3-F four mutations.
- **Leader Completeness** gets its own checker class in M2, not folded into `ElectionSafetyChecker`.
  The owner asked for it to be checked once logs are non-empty.

- **M5 choices (approved 2026-11-25, "Accept all"):** M5-A PreVote + CheckQuorum + stickiness behind
  `RaftConfig` flags (off in the plain constructor so M1–M4 unit tests stay Figure 2; on in sim and
  server); M5-B gRPC, one client-streaming RPC per peer, bounded queue, drop when full; M5-C HTTP/JSON on
  the JDK server + binary `/v1/*` for `HttpClusterClient`/`raft-cli` (on `ClientSession`); M5-D one
  event-loop thread + mailbox, Micrometer/Prometheus from published `View`s; M5-E kill -9 test with 3
  JVMs + Dockerfile/compose.

## Still open (ask at the named milestone)
- **M6:** benchmarks and report: ask before choosing tools and workloads.

## Layout
```
settings.gradle.kts, gradle/libs.versions.toml, buildSrc/ (raft.java-conventions: Java 21 toolchain,
  -Werror -Xlint:all, JUnit 5 + jqwik + AssertJ)
raft-core/          com.projects.raftkv.core       pure core: RaftNodeCore, RoleState, RaftLog (with snapshot
                                                   base), Snapshot, types
raft-storage/       com.projects.raftkv.storage    Storage, InMemoryStorage (unit tests), StorageException,
                                                   LogImage, SnapshotStore, StorageMutation
                    ...storage.disk                Disk, RealDisk, SimDisk (page-cache crash model)
                    ...storage.wal                 FileWalStorage, WalRecord, WalCodec, WalOptions
raft-proto/         com.projects.raftkv.proto      raft.proto (generated protobuf + gRPC stubs)
raft-transport/     com.projects.raftkv.transport  Transport, GrpcTransport, GrpcPeerServer, ProtoMapping
raft-node/          com.projects.raftkv.node       NodeDriver (apply, pending proposals, ReadIndex reads),
                                                   ProposalOutcome, ReadOutcome, NodeHaltedException
raft-statemachine/  com.projects.raftkv.statemachine StateMachine (apply, query), KvStateMachine (data + session
                                                   table), KvCommand, KvRequest (clientId, seq), KvResult (+Stale),
                                                   KvCodec (snapshot v2 with sessions)
raft-client/        com.projects.raftkv.client     ClientSession (pure: seq, retries, hints, timeouts),
                                                   HttpClusterClient, RaftCli
raft-server/        com.projects.raftkv.server     Main, ServerConfig (env), RaftServer (event loop), ClientApi
                                                   (HTTP), NodeMetrics, TimedDisk, ClientReply; Dockerfile
raft-sim/           com.projects.raftkv.sim        Simulation, NetworkConfig, FaultConfig, SimConfig,
                                                   WorkloadConfig, StorageConfig, ClientWorkload, Trace,
                                                   NodeLogTracker, TrackedStorage, checkers: ElectionSafety,
                                                   VoteDurability, LogMatching, LeaderAppendOnly,
                                                   LeaderCompleteness, StateMachineSafety, ReplicaState,
                                                   Durability; HistoryRecorder, LinearizabilityChecker
compose.yaml, DESIGN.md, DEVIATIONS.md, MILESTONES.md, EXPLANATION.md, PROJECT_BRIEF.md
```

## Commands
```
./gradlew build                                   # all modules, 369 tests, 300 seeds per chaos test
./gradlew :raft-server:integrationTest            # 3 JVMs + kill -9 loop (~40 s; -Pkill9.kills=30 ≈ 90 s)
docker compose up -d --build                      # real 3-node cluster on :8081-8083 (Docker Desktop must run)
./gradlew :raft-sim:test --tests '*ChaosTest*' -Psim.seeds=5000   # milestone gate (chaos + crash loop × {3, 5})
./gradlew :raft-sim:test -Psim.seed=N             # replay one seed
```
JDK 21 is installed at `/Users/amoghp/Library/Java/JavaVirtualMachines/ms-21.0.11` (the default `java`
is 25; the Gradle toolchain picks 21 automatically). Gradle 9.9.2 wrapper.

## Conventions and gotchas
- Write tests first. Javadoc on each protocol rule cites the paper section or figure.
- Shortcuts are marked `// SHORTCUT:` in code and listed in DEVIATIONS.md. After M4 there are none in
  code; DEVIATIONS lists the M4 skips (K1–K5) and harness notes H2–H5.
- Mutation hooks: `RaftNodeCore.withMutationsForTesting` + `ProtocolMutation`; each hook is one
  `// MUTATION (test only)` line. The Figure 8 mutation is only reachable with
  `withMaxEntriesPerAppend(1)` (the no-op otherwise rides with the old entries).
- The simulator's checkers see log changes through `TrackedStorage` → `NodeLogTracker` (prefix hashes,
  recomputed only from the first changed index). Every core log change must go through a `PersistOp`,
  or the checkers would miss it.
- The basic decrement-by-one backup is safe but too slow for the 3 s quiet window; fast backup
  (conflictTerm/conflictIndex) is what makes the commit-liveness check pass.
- Unimplemented features **throw** (`UnsupportedOperationException`) rather than being silently ignored.
- `-Werror` is on: records nested in an interface are implicitly public, so their compact constructors
  must be `public`. Make classes `final` to avoid the `this-escape` lint.
- Record `hashCode` is deterministic for our types, which `Trace` hashing relies on. Don't put enums or
  identity-hashed objects in trace payloads.
- The simulator observes node state **after** the driver's fsync. Sim nodes run `FileWalStorage` on a
  `SimDisk`; `SimDisk.crash()` keeps synced data and tears unsynced data. `setFsyncIsNoop(true)` is the
  lying-disk mutation; `failNextFsync()` is a crash mid-write.
- `NodeLogTracker` hashes entries **at write time** (via `TrackedStorage`), not by reading the log:
  entries can be appended and compacted within one event. Installed snapshots seed hashes from
  `LeaderCompletenessChecker.committedHashAt`.
- Checker assumptions that broke in M3: applied indexes aren't contiguous from 1 (snapshots), and
  "committed entry never deleted" holds only for nodes at or past the commit term (Figure 8; seed 327).
- WAL segments roll only right after a successful sync; that's what makes "bad bytes in the last
  segment = torn tail, elsewhere = corruption" valid. Snapshot file must be durable before `CompactLog`.
- M4: dedup happens at **apply** time (a retry is a second log entry answered from the session). Client
  attempt ids are `clientId << 40 | n`; each delivered copy of a client request gets its own
  proposal/read id at the node (`Simulation.attemptOfDelivery`). `NodeDriver.create(..., readOutcomes,
  snapshotEvery)` is the overload with a read listener; the others throw if a read is served.
- M4 seed 4811: a rejection of an **older term's** AppendEntries/InstallSnapshot echoes read round 0.
  The reply carries the follower's newer term, so an echoed old round would count as a current ack.
  `-Psim.trace=N` keeps N trace lines (default 300) when debugging a replayed seed.
- jqwik draws new random inputs every run. A property failing on an untouched module is a new input,
  not a flake: reproduce it from the "Sample" in the test report and pin it as an `@Test` regression
  (M4 found a wrong oracle in `WalTornTailProperties` this way).
- M5: PreVote/CheckQuorum **mask** other bugs (PreVote's log check hides a broken RequestVote check;
  CheckQuorum shrinks the stale-read window). Mutation tests for M2–M4 bugs run with
  `SimConfig.withHardening(false, false)`; the masking itself is pinned as tests.
- M5: client liveness window is 2 s (`-Psim.clientWindow=N` overrides), cluster liveness 1 s (H6).
- M5: `HistoryRecorder`/`LinearizabilityChecker` are public because raft-server's kill -9 test uses them
  (test dependency on raft-sim).
- Git: the project repo is this folder (remote `origin` = GitHub `Distributed-Raft-based-KV-store`).
  `/Users/amoghp` also contains a stray `~/.git`; the owner was warned. Never run git from the home
  directory.
- Commits are authored as `Amogh P <143121631+Amoghprakash2k4@users.noreply.github.com>` (set in this
  repo's local git config; the global config is the owner's work identity, leave it). **Never add a
  `Co-Authored-By: Claude` trailer** or any Claude attribution to commits or PRs.
