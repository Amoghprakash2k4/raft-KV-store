package com.projects.raftkv.core;

/** Identity of a cluster member. Fixed at startup (no membership changes in v1). */
public record NodeId(int value) implements Comparable<NodeId> {
    public NodeId {
        if (value < 0) throw new IllegalArgumentException("NodeId must be >= 0: " + value);
    }
    @Override public int compareTo(NodeId o) { return Integer.compare(value, o.value); }
    @Override public String toString() { return "n" + value; }
}
