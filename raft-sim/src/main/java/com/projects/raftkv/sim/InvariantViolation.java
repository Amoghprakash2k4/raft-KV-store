package com.projects.raftkv.sim;

/** A safety or liveness property was violated during a simulation run. */
public final class InvariantViolation extends RuntimeException {
    public InvariantViolation(String message) { super(message); }
}
