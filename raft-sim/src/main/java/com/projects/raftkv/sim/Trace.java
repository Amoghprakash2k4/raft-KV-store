package com.projects.raftkv.sim;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Bounded event trace for failure reports, plus a running hash over every event for determinism checks.
 * Lines are formatted lazily, only when a run fails.
 */
final class Trace {
    private record Line(long timeMs, String who, Object what) {}

    private final int capacity;
    private final Deque<Line> lines = new ArrayDeque<>();
    private long hash = 17;

    Trace(int capacity) { this.capacity = capacity; }

    void add(long timeMs, String who, Object what) {
        hash = 31 * hash + timeMs;
        hash = 31 * hash + who.hashCode();
        hash = 31 * hash + what.hashCode();
        lines.addLast(new Line(timeMs, who, what));
        if (lines.size() > capacity) lines.removeFirst();
    }

    long hash() { return hash; }

    String format() {
        StringBuilder sb = new StringBuilder();
        for (Line l : lines) {
            sb.append(String.format("    %7dms %-4s %s%n", l.timeMs(), l.who(), l.what()));
        }
        return sb.toString();
    }
}
