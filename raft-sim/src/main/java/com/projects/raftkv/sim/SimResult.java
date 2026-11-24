package com.projects.raftkv.sim;

/**
 * Summary of a successful run. Two runs with the same seed and config must be equal (incl. traceHash).
 * Client side (M4): operations started, completed writes and reads, attempts sent (first tries plus
 * retries), timeouts, redirects (rejections), retry replies (Lost/Unknown), operations still unanswered at
 * the end, proposals overwritten (lost) or covered by a snapshot (unknown) as reported by nodes,
 * duplicate entries answered from a session (summed over replicas), request/reply copies the client links
 * duplicated, and the linearizability search size.
 */
public record SimResult(long seed, long eventsProcessed, long traceHash,
                        int leadersElected, long maxTerm, long firstLeaderAtMs, long lastNewLeaderAtMs,
                        int crashes, int partitions, int pauses,
                        long opsStarted, long writesCompleted, long readsCompleted, long attempts, long timeouts,
                        long redirects, long retryReplies, long unansweredAtEnd, long lost, long unknown,
                        long duplicatesAnswered, long linkDuplicates, long linearizabilitySteps, long maxCommitIndex,
                        int midWriteCrashes, int tornTailsCut, long restartsChecked,
                        long snapshotsTaken, long snapshotsInstalled, long maxLogEntries) {}
