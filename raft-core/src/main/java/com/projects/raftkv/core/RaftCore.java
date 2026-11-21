package com.projects.raftkv.core;

/**
 * The pure Raft state machine: no threads, no I/O, no clock. Randomness only via the
 * RandomGenerator passed at construction, so (config, recovered state, seed, event sequence)
 * always produces the same sequence of Readys.
 * Not thread-safe by design: exactly one driver thread calls it.
 */
public interface RaftCore {
    Ready step(Event event);

    RaftStatus status();

    LogView log();
}
