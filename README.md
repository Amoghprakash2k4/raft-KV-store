# Distributed-Raft-based-KV-store

A replicated key-value store built on Raft, written from scratch in plain Java 21 with no Raft library
and no framework. The protocol core is a pure, deterministic state machine. A deterministic simulator
runs thousands of seeded chaos runs against it (dropped, duplicated and reordered messages, partitions,
crashes in the middle of an fsync, process pauses) and checks every Raft safety property after every
event, plus linearizability of what clients saw.

## Architecture

```mermaid
flowchart TB
    CL["raft-client<br/>ClientSession: clientId, seq,<br/>retries, leader redirect"]
    subgraph NODE["one node (single-threaded)"]
        DRV["raft-node: NodeDriver<br/>persist + fsync → send → apply → notify"]
        CORE["raft-core: RaftNodeCore (pure)<br/>election, replication, snapshots, ReadIndex"]
        SM["raft-statemachine: KvStateMachine<br/>PUT / GET / DELETE / CAS + client sessions"]
        ST["raft-storage: FileWalStorage<br/>CRC32C WAL, torn-tail cut, atomic snapshots"]
    end
    NET["raft-transport<br/>(gRPC in M5)"]
    SIM["raft-sim: deterministic simulator<br/>virtual time, faults, 8 invariant checkers,<br/>linearizability checker"]
    CL -- "requests / answers" --> DRV
    DRV -- "events" --> CORE
    CORE -- "Ready: effects" --> DRV
    DRV --> ST
    DRV --> SM
    DRV <--> NET
    SIM -. "drives N nodes on a simulated disk and network" .-> NODE
```

## Status

| Milestone | What | State |
|---|---|---|
| M1 | Leader election | done |
| M2 | Log replication, KV state machine, fast log backup | done |
| M3 | Write-ahead log, crash recovery, snapshots, InstallSnapshot | done |
| M4 | Client sessions (exactly-once writes), ReadIndex reads, linearizability checker | done |
| M5 | PreVote, CheckQuorum, gRPC, metrics, CLI | next |
| M6 | Benchmarks and report | later |

Each milestone ends with a gate of 5,000 seeds × {3, 5} nodes × {chaos, crash loop} with zero
violations, plus deliberately injected bugs ("mutations") that the checkers must catch. Numbers are in
[MILESTONES.md](MILESTONES.md).

## Build and test

```
./gradlew build                                                  # all modules and tests
./gradlew :raft-sim:test --tests '*ChaosTest*' -Psim.seeds=5000  # a milestone gate
./gradlew :raft-sim:test -Psim.seed=1234                         # replay one seed
```

Requires JDK 21 (Gradle's toolchain finds or downloads it).

## Documents

- [EXPLANATION.md](EXPLANATION.md): learning notes for every milestone, with diagrams and study questions
- [DESIGN.md](DESIGN.md): decisions and rejected alternatives
- [DEVIATIONS.md](DEVIATIONS.md): every shortcut, and what was skipped on purpose
- [MILESTONES.md](MILESTONES.md): exit criteria, results, mutation catch rates
- [PROJECT_BRIEF.md](PROJECT_BRIEF.md): the original brief
