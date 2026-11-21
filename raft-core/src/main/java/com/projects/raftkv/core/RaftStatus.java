package com.projects.raftkv.core;

/** Snapshot of the core's state for assertions and invariant checkers. Never used for decisions. */
public record RaftStatus(NodeId id, SoftState soft, HardState hard, long commitIndex, long lastApplied) {}
