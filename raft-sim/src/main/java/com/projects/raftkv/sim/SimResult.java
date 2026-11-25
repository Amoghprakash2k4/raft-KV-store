package com.projects.raftkv.sim;

/**
 * Summary of a successful run. Two runs with the same seed and config must be equal (incl. traceHash).
 * Client side (M4): operations started, completed writes and reads, attempts sent (first tries plus
 * retries), timeouts, redirects (rejections), retry replies (Lost/Unknown), operations still unanswered at
 * the end, proposals overwritten (lost) or covered by a snapshot (unknown) as reported by nodes,
 * duplicate entries answered from a session (summed over replicas), request/reply copies the client links
 * duplicated, and the linearizability search size.
 * Scenario (M5): the highest term when the scripted fault started, leaders elected from then on, and how
 * long an isolated leader kept its role (-1 = never stepped down, or not that scenario).
 * {@code slowestClientIdleMs}: at the end, how long ago the least recently successful client completed
 * its last operation.
 * {@code batches}/{@code batchedEvents}/{@code maxBatch}: group commit (M6-A), 0 when batching is off.
 */
public record SimResult(long seed, long eventsProcessed, long traceHash,
                        int leadersElected, long maxTerm, long firstLeaderAtMs, long lastNewLeaderAtMs,
                        int crashes, int partitions, int pauses,
                        long opsStarted, long writesCompleted, long readsCompleted, long attempts, long timeouts,
                        long redirects, long retryReplies, long unansweredAtEnd, long lost, long unknown,
                        long duplicatesAnswered, long linkDuplicates, long linearizabilitySteps, long maxCommitIndex,
                        int midWriteCrashes, int tornTailsCut, long restartsChecked,
                        long snapshotsTaken, long snapshotsInstalled, long maxLogEntries,
                        long termAtScenarioStart, int newLeadersSinceScenario, long scenarioStepDownMs,
                        long slowestClientIdleMs, long batches, long batchedEvents, long maxBatch) {}
