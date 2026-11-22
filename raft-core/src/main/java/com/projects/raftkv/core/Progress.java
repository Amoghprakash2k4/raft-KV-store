package com.projects.raftkv.core;

/**
 * The leader's view of one follower (Figure 2, volatile state on leaders).
 * {@code nextIndex}: index of the next entry to send. {@code matchIndex}: highest index known to be
 * replicated on that follower. Invariant: matchIndex < nextIndex.
 */
public record Progress(long nextIndex, long matchIndex) {
    public Progress {
        if (matchIndex < 0 || nextIndex <= matchIndex)
            throw new IllegalArgumentException("need 0 <= matchIndex < nextIndex: " + matchIndex + ", " + nextIndex);
    }
}
