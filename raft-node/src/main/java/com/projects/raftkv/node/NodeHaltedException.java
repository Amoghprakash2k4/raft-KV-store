package com.projects.raftkv.node;

/** The node stopped after a durability failure and must be restarted from disk (DESIGN.md E1a). */
public final class NodeHaltedException extends RuntimeException {
    public NodeHaltedException(String message, Throwable cause) { super(message, cause); }
}
