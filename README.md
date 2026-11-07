# raft-kv

A fault-tolerant, replicated key-value store in C++17, built on a from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf).

The cluster keeps serving `GET` / `PUT` / `DELETE` while nodes crash, restart, and the network drops, delays, duplicates, or partitions messages. Everything runs on a **deterministic simulator**: a virtual clock plus a seeded random number generator, so any failure found by a randomized test can be replayed exactly from its seed.

> **Status:** work in progress. See the roadmap below.

## Why a simulator?

Distributed-systems bugs are timing bugs, and timing bugs are hard to reproduce. In this project no code reads the wall clock or uses threads. Time advances only when the simulator pops the next event, and all randomness (network delay, election timeouts, message loss) comes from one seeded generator. Same seed, same history, every time.

## Architecture

```
            +---------------------------+
            |   KV store + client API   |   (milestone 4)
            +-------------+-------------+
                          |
            +-------------v-------------+
            |        Raft node          |   election, replication, commit
            +------+-------------+------+
                   |             |
        +----------v---+   +-----v--------+
        |   Storage    |   |   Network    |   delay / drop / duplicate / partition
        | (term, vote, |   +-----+--------+
        |     log)     |         |
        +--------------+   +-----v--------+
                           |  Simulator   |   virtual clock + seeded RNG
                           +--------------+
```

| Path | Purpose |
|---|---|
| `include/raft/types.h` | Ids, log entries, the four Raft RPC messages |
| `include/raft/sim.h`, `src/sim.cpp` | Discrete-event simulator with virtual time |
| `include/raft/network.h`, `src/network.cpp` | Simulated unreliable network and partitions |
| `include/raft/storage.h` | Persistent state interface (term, vote, log) |
| `include/raft/node.h`, `src/node.cpp` | A single Raft server |
| `tests/` | Dependency-free test suite |

## Build and test

Requires a C++17 compiler and CMake 3.16+.

```bash
cmake -S . -B build
cmake --build build
./build/test_foundations
```

The build enables AddressSanitizer and UBSan by default. Turn them off with `-DRAFT_SANITIZE=OFF`.

## Roadmap

- [x] **1. Foundations:** deterministic simulator, simulated network, storage interface, node skeleton (14 tests)
- [x] **2. Leader election:** randomized timeouts, `RequestVote`, heartbeats, step-down
- [ ] **3. Log replication:** `AppendEntries` consistency check, commit index, apply to state machine
- [ ] **4. KV layer and client API:** linearizable reads and writes, leader redirect, duplicate-request detection
- [ ] **5. Persistence and recovery:** file-backed storage with fsync, crash and restart
- [ ] **6. Log compaction:** snapshots and `InstallSnapshot`
- [ ] **7. Chaos tests and benchmarks:** kill leaders, partition the network, check linearizability, measure throughput and latency

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [The Raft visualization](https://raft.github.io/)

## License

MIT. See [LICENSE](LICENSE).
