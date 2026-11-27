# Raft KV Store

**A fault-tolerant, linearizable key-value store built on the Raft consensus algorithm, written from scratch
in Java 21: no consensus library, no framework.**

![Java 21](https://img.shields.io/badge/Java-21-orange)
![Gradle](https://img.shields.io/badge/build-Gradle-02303A)
![Tests](https://img.shields.io/badge/tests-411%20passing-brightgreen)
![Gate](https://img.shields.io/badge/simulation%20gate-40%2C000%20runs%2C%200%20violations-blue)

Three or five nodes keep identical copies of the data. When the leader crashes, the others elect a new one
in about 200 ms and no acknowledged write is lost. Correctness is checked, not assumed: a deterministic
simulator runs tens of thousands of seeded disaster scenarios against the real protocol code and checks every
Raft safety property after every event, plus linearizability of everything clients observed.

---

## Highlights

| | |
|---|---|
| **Consensus** | Leader election, log replication with fast backup, Figure 8 commit rule, PreVote, CheckQuorum, leader stickiness |
| **Durability** | Write-ahead log with CRC32C records, torn-write detection, crash recovery, snapshots, chunked InstallSnapshot |
| **Clients** | Exactly-once writes (client sessions + deduplication), linearizable reads via ReadIndex, leader redirects and retries |
| **Runtime** | Single-threaded event loop per node, gRPC between nodes, HTTP/JSON API, CLI, Prometheus metrics, Docker Compose |
| **Performance** | Group commit: **14× write throughput** (148 → 2,075 writes/s) and p99 latency **541 → 50 ms**, every write fsynced on a majority |
| **Verification** | Deterministic simulation with fault injection, 8 invariant checkers, a linearizability checker, 11 planted bugs that must be caught, `kill -9` tests on real processes |
| **Tooling** | A localhost dashboard to run simulations, blind bug-detection tests and live benchmarks |

## Architecture

```mermaid
flowchart TB
    CL["raft-client<br/>ClientSession: clientId, seq,<br/>retries, leader redirect"]
    subgraph NODE["one node (single-threaded event loop)"]
        DRV["raft-node: NodeDriver<br/>persist + fsync → send → apply → notify"]
        CORE["raft-core: RaftNodeCore (pure)<br/>election, replication, snapshots, ReadIndex"]
        SM["raft-statemachine: KvStateMachine<br/>PUT / GET / DELETE / CAS + client sessions"]
        ST["raft-storage: FileWalStorage<br/>CRC32C WAL, torn-tail cut, atomic snapshots"]
    end
    NET["raft-transport<br/>gRPC streams, one per peer"]
    SIM["raft-sim: deterministic simulator<br/>virtual time, faults, invariant checkers,<br/>linearizability checker"]
    CL -- "requests / answers" --> DRV
    DRV -- "events" --> CORE
    CORE -- "Ready: effects" --> DRV
    DRV --> ST
    DRV --> SM
    DRV <--> NET
    SIM -. "drives N nodes on a simulated disk and network" .-> NODE
```

The protocol core is a **pure function**: `step(event) → Ready` (what to persist, send and apply). It never
touches threads, clocks, sockets or files. A driver executes each `Ready` in a fixed order (persist and fsync,
then send, then apply, then answer). Because of that split, the exact same core runs inside the real server
and inside the deterministic simulator, so every failure the simulator finds is reproducible from one seed.

## Results

Measured on one MacBook (Apple silicon), every write durably flushed on a majority before it is acknowledged.
Full method, charts and limits: [BENCHMARKS.md](BENCHMARKS.md).

| Measurement | Result |
|---|---|
| Write throughput, 3 nodes, 64 clients | **2,075 writes/s** with group commit (148 without) |
| Write latency p99, same | **50 ms** (541 ms without group commit) |
| Linearizable read, idle | **0.4 ms** p50 (ReadIndex: no log entry, no disk write) |
| Leader failover, 100 × `kill -9`, 3 nodes | election **p50 199 ms, p99 386 ms** |
| Leader failover, 100 × `kill -9`, 5 nodes | election **p50 186 ms, p99 247 ms** |
| Simulation gate | 5,000 seeds × 8 configurations = **40,000 runs, 0 safety violations** |
| Planted bugs | **11 of 11 caught**, 0 false alarms |

## Quick start

**Requirements:** JDK 21 (the Gradle toolchain finds or downloads it). Docker is optional.

```bash
./gradlew build                          # compile everything and run the 411 tests (~1.5 min)
```

### Run a 3-node cluster

```bash
docker compose up -d --build             # clients on :8081, :8082, :8083

curl -X PUT localhost:8081/kv/x -d '{"value":"1"}'   # a follower answers 421 {"leaderId":N}
curl -X PUT localhost:808N/kv/x -d '{"value":"1"}'   # the leader answers {"previous":null}
curl localhost:8081/kv/x                             # linearizable read
curl -X POST localhost:808N/kv/x/cas -d '{"expected":"1","value":"2"}'
curl localhost:8081/status                           # role, term, commitIndex
curl localhost:8081/metrics                          # Prometheus metrics

docker compose kill node2                # SIGKILL a node...
docker compose start node2               # ...and it recovers from its write-ahead log
docker compose down -v                   # stop and delete the data volumes
```

The CLI follows leader redirects and retries with the same sequence number (exactly-once):

```bash
./gradlew :raft-server:installDist
CLI="raft-server/build/install/raft-server/bin/raft-cli --cluster 1=http://localhost:8081,2=http://localhost:8082,3=http://localhost:8083"
$CLI put demo hello
$CLI get demo
$CLI cas demo hello world
$CLI status
```

Without Docker, start `raft-server/build/install/raft-server/bin/raft-server` three times with
`RAFT_NODE_ID`, `RAFT_PEERS`, `RAFT_PEER_PORT`, `RAFT_CLIENT_PORT` and `RAFT_DATA_DIR` set (see `ServerConfig`).

### Dashboard

```bash
./gradlew :raft-dashboard:run            # opens http://localhost:8090
```

| Tab | What it does |
|---|---|
| **Simulator** | Run thousands of seeded chaos scenarios; plant a bug and click a failing seed to see which checker caught it |
| **Blind bug test** | Hide chosen bugs among clean seeds; the answer key (caught, missed, false alarms) is revealed at the end |
| **Throughput & latency** | Benchmark a real 3- or 5-node cluster live |
| **Leader failover** | `kill -9` the leader repeatedly and measure the time to recover |
| **Compare runs** | Put saved runs side by side |
| **Benchmarks** | The measured results, explained |

## How correctness is verified

1. **Unit tests** on the pure core: one event in, check the effects out.
2. **Deterministic simulation.** All nodes run in one thread on virtual time, a simulated network (loss,
   duplication, long delays, one-way partitions) and a simulated disk (crashes in the middle of an fsync, torn
   writes). After every event, checkers verify Election Safety, vote durability, Log Matching, Leader
   Append-Only, Leader Completeness, State Machine Safety and durability of acknowledged writes. At the end of
   each run a Wing & Gong search verifies the client history is linearizable, and liveness checks verify the
   cluster recovers.
3. **Mutation testing.** Eleven deliberate one-line bugs (the Figure 8 commit rule, a lying fsync, skipped
   deduplication, reads without a leadership check, and more) are switched on in test runs. Each must be caught.
4. **Real processes.** Three JVMs on real files, nodes killed with `kill -9` in a loop while clients write;
   the history is then checked for linearizability.

```bash
./gradlew :raft-sim:test --tests '*ChaosTest*' -Psim.seeds=5000   # the full gate (~8 min)
./gradlew :raft-sim:test -Psim.seed=1234                          # replay one seed exactly
./gradlew :raft-server:integrationTest                            # kill -9 test on real processes
```

## Project layout

| Module | Responsibility |
|---|---|
| `raft-core` | The pure Raft protocol: roles, log, election, replication, snapshots, ReadIndex, PreVote, CheckQuorum |
| `raft-storage` | Write-ahead log (typed records, CRC32C, torn-tail recovery), snapshot files, real and simulated disks |
| `raft-statemachine` | The key-value state machine with client sessions and a canonical binary codec |
| `raft-node` | `NodeDriver`: executes the core's effects in order; group commit |
| `raft-proto`, `raft-transport` | Protobuf schema and gRPC transport between nodes |
| `raft-client` | `ClientSession` (retries, sequence numbers, redirects), HTTP client, CLI |
| `raft-server` | The runnable node: event loop, HTTP API, metrics, Dockerfile |
| `raft-sim` | Deterministic simulator, fault injection, invariant and linearizability checkers |
| `raft-bench` | Benchmarks on real processes: load, failover, the full matrix |
| `raft-dashboard` | The localhost dashboard |

## Documentation

Each document is also available as a PDF with rendered diagrams in [`docs/`](docs/).

| Document | Contents | PDF |
|---|---|---|
| [EXPLANATION.md](EXPLANATION.md) | Learning notes for every milestone: concepts, diagrams, study questions | [PDF](docs/EXPLANATION.pdf) |
| [INTERVIEW_PREP.md](INTERVIEW_PREP.md) | Likely interview questions with answers, bug stories, a demo script | [PDF](docs/INTERVIEW_PREP.pdf) |
| [BENCHMARKS.md](BENCHMARKS.md) | Throughput, latency and failover numbers, with method and limits | [PDF](docs/BENCHMARKS.pdf) |
| [DESIGN.md](DESIGN.md) | Every design decision and the alternatives rejected | [PDF](docs/DESIGN.pdf) |
| [MILESTONES.md](MILESTONES.md) | Exit criteria, gate results and mutation catch rates per milestone | [PDF](docs/MILESTONES.pdf) |
| [DEVIATIONS.md](DEVIATIONS.md) | Every shortcut and every feature skipped on purpose | [PDF](docs/DEVIATIONS.pdf) |
| [PROJECT_BRIEF.md](PROJECT_BRIEF.md) | The original brief | [PDF](docs/PROJECT_BRIEF.pdf) |

## Milestones

| | Milestone | Delivered |
|---|---|---|
| M1 | Leader election | Terms, randomized timeouts, durable votes, the simulator and first checkers |
| M2 | Log replication | AppendEntries, commit rules (Figure 8), fast backup, the KV state machine |
| M3 | Durability | Write-ahead log, crash recovery, snapshots, InstallSnapshot |
| M4 | Clients and reads | Sessions and exactly-once writes, ReadIndex, the linearizability checker |
| M5 | Hardening | PreVote, CheckQuorum, gRPC, the real server, HTTP API, CLI, metrics, `kill -9` tests, Docker |
| M6 | Benchmarks | Group commit, the benchmark suite, the dashboard |

## Known limitations

Fixed cluster membership (no add/remove of nodes at runtime), an unbounded client-session table, reads served
only by the leader, and benchmarks run on a single machine. Each is documented with its cost and how it would
be added in [DEVIATIONS.md](DEVIATIONS.md).

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014
- Diego Ongaro, *Consensus: Bridging Theory and Practice*, PhD thesis, Stanford University, 2014
