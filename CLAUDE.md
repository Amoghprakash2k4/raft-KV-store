# CLAUDE.md: context for continuing this project

Read this first in every new conversation. Then read `PROJECT_BRIEF.md` (the owner's full instructions),
`MILESTONES.md` (status and the next checklist), `DESIGN.md` (decisions) and `DEVIATIONS.md` (shortcuts).

## What this is
A Raft-based replicated key-value store in plain Java 21, built from scratch as a **learning project**
for the owner: a new grad with about 6 months of Java/Spring experience who wants to deeply understand
consensus and end up with an interview-grade project. You act as a **senior distributed-systems mentor**.

## Status (last updated 2026-11-22)
- **M1 Leader Election: DONE.** Gate passed: 0 Election Safety violations over 5,000 seeds × {3, 5} nodes.
- **M2 Log Replication: DONE** (accepted by the owner 2026-11-23). 0 violations of invariants 1–5 over
  5,000 seeds × {3, 5} nodes; all mutation checks caught (rates in MILESTONES.md).
- **M3 Durability: NOT STARTED.** Before coding, ask the owner the open M3 design questions (below).
- **Git:** M1 `b0420ae`, docs `eb85e73`, M2 committed and pushed to `origin/main` on 2026-11-23 (the
  owner asked for it). Never commit or push without an explicit yes from the owner.

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
- **Leader Completeness** gets its own checker class in M2, not folded into `ElectionSafetyChecker`.
  The owner asked for it to be checked once logs are non-empty.

## Still open (ask at the named milestone)
- **M3:** where snapshot bytes live while being sent (core vs. driver), chunked InstallSnapshot, and
  replacing the `SnapshotInstalled` event with `SnapshotCreated`.
- **M4:** client session expiry (unbounded table vs. deterministic expiry, thesis §6.3).

## Layout
```
settings.gradle.kts, gradle/libs.versions.toml, buildSrc/ (raft.java-conventions: Java 21 toolchain,
  -Werror -Xlint:all, JUnit 5 + jqwik + AssertJ)
raft-core/          com.projects.raftkv.core       pure core: RaftNodeCore, RoleState, RaftLog, types
raft-storage/       com.projects.raftkv.storage    Storage, InMemoryStorage, StorageException
raft-transport/     com.projects.raftkv.transport  Transport
raft-node/          com.projects.raftkv.node       NodeDriver (apply + pending proposals), ProposalOutcome,
                                                   NodeHaltedException
raft-statemachine/  com.projects.raftkv.statemachine StateMachine, KvStateMachine (TreeMap), KvCommand,
                                                   KvRequest (clientId, seq), KvResult, KvCodec
raft-client/        (empty: M4)
raft-sim/           com.projects.raftkv.sim        Simulation, NetworkConfig, FaultConfig, SimConfig,
                                                   WorkloadConfig, ClientWorkload, Trace, NodeLogTracker,
                                                   TrackedStorage, checkers: ElectionSafety, VoteDurability,
                                                   LogMatching, LeaderAppendOnly, LeaderCompleteness,
                                                   StateMachineSafety, ReplicaState
compose.yaml, DESIGN.md, DEVIATIONS.md, MILESTONES.md, EXPLANATION.md, PROJECT_BRIEF.md
```

## Commands
```
./gradlew build                                   # all modules, 149 tests (~16 s), 300 seeds per chaos test
./gradlew :raft-sim:test -Psim.seeds=5000         # milestone gate (~90 s)
./gradlew :raft-sim:test -Psim.seed=N             # replay one seed
```
JDK 21 is installed at `/Users/amoghp/Library/Java/JavaVirtualMachines/ms-21.0.11` (the default `java`
is 25; the Gradle toolchain picks 21 automatically). Gradle 9.9.2 wrapper.

## Conventions and gotchas
- Write tests first. Javadoc on each protocol rule cites the paper section or figure.
- Shortcuts are marked `// SHORTCUT:` in code and listed in DEVIATIONS.md. After M2 there are none in
  code; only P3 (`ReadRequested` throws until M4) and harness notes H1/H2 remain.
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
- The simulator observes node state **after** the driver's fsync. `InMemoryStorage.crash()` keeps only
  synced data. `setFsyncIsNoop(true)` is the lying-disk mutation.
- Git: the project repo is this folder (remote `origin` = GitHub `Distributed-Raft-based-KV-store`).
  `/Users/amoghp` also contains a stray `~/.git`; the owner was warned. Never run git from the home
  directory.
