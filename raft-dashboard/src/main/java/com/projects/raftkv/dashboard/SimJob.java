package com.projects.raftkv.dashboard;

import com.projects.raftkv.sim.InvariantViolation;
import com.projects.raftkv.sim.SimConfig;
import com.projects.raftkv.sim.SimResult;
import com.projects.raftkv.sim.Simulation;
import com.projects.raftkv.sim.SimulationFailure;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Runs a {@link SimSettings} over many seeds, in parallel, into one {@link Result}. */
final class SimJob {

    /**
     * One seed. {@code outcome} is "passed" or the checker that failed (as in MutationSanityTest). For a
     * failed seed the counters cover the run up to the failure, at {@code failedAtMs} of simulated time (-1
     * when it passed).
     */
    record Row(long seed, String outcome, String message, int elections, long maxTerm, int crashes, int midWriteCrashes,
               int partitions, int pauses, long ops, long writes, long reads, long timeouts, long redirects,
               long dedupHits, long unknownAtEnd, long linSteps, long maxCommitIndex, long snapshotsTaken,
               long snapshotsInstalled, int newLeadersAfterScenario, long stepDownMs, long slowestClientIdleMs,
               double avgBatch, long maxBatch, long events, long failedAtMs) {}

    record Result(SimSettings settings, int seeds, int passed, Map<String, Integer> outcomes, Map<String, Long> totals,
                  List<Row> rows, long wallMs) {}

    record Progress(int done, int of, int passed, int failed) {}

    /** Every checker's message prefix (the page lists them all, with 0 or the seeds each one failed). */
    static final List<String> CHECKERS = List.of("Election Safety", "Vote durability", "Leader Append-Only", "Log Matching",
            "Leader Completeness", "State Machine Safety", "Replica state divergence", "Durability", "Linearizability",
            "Liveness");

    private SimJob() {}

    static Result run(SimSettings s, AtomicBoolean cancel, Consumer<Progress> progress) throws InterruptedException {
        SimConfig cfg = s.toConfig();
        long start = System.nanoTime();
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        AtomicInteger done = new AtomicInteger(), passed = new AtomicInteger();
        List<Row> rows = java.util.Collections.synchronizedList(new ArrayList<>());
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> futures = new ArrayList<>();
            for (long seed = s.firstSeed(); seed < s.firstSeed() + s.seeds(); seed++) {
                long sd = seed;
                futures.add(pool.submit(() -> {
                    if (cancel.get()) return;
                    Row row = runOne(sd, cfg);
                    rows.add(row);
                    if (row.outcome().equals("passed")) passed.incrementAndGet();
                    int d = done.incrementAndGet();
                    if (d % 10 == 0 || d == s.seeds()) progress.accept(new Progress(d, s.seeds(), passed.get(), d - passed.get()));
                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new IllegalStateException(e.getCause());
                }
            }
        }
        List<Row> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparingLong(Row::seed));
        Map<String, Integer> outcomes = new TreeMap<>();
        for (Row r : sorted) outcomes.merge(r.outcome(), 1, Integer::sum);
        Map<String, Long> totals = new java.util.LinkedHashMap<>();
        totals.put("elections", sorted.stream().mapToLong(Row::elections).sum());
        totals.put("maxTerm", sorted.stream().mapToLong(Row::maxTerm).max().orElse(0));
        totals.put("crashes", sorted.stream().mapToLong(Row::crashes).sum());
        totals.put("midWriteCrashes", sorted.stream().mapToLong(Row::midWriteCrashes).sum());
        totals.put("partitions", sorted.stream().mapToLong(Row::partitions).sum());
        totals.put("pauses", sorted.stream().mapToLong(Row::pauses).sum());
        totals.put("ops", sorted.stream().mapToLong(Row::ops).sum());
        totals.put("writes", sorted.stream().mapToLong(Row::writes).sum());
        totals.put("reads", sorted.stream().mapToLong(Row::reads).sum());
        totals.put("timeouts", sorted.stream().mapToLong(Row::timeouts).sum());
        totals.put("dedupHits", sorted.stream().mapToLong(Row::dedupHits).sum());
        totals.put("snapshotsTaken", sorted.stream().mapToLong(Row::snapshotsTaken).sum());
        totals.put("snapshotsInstalled", sorted.stream().mapToLong(Row::snapshotsInstalled).sum());
        totals.put("events", sorted.stream().mapToLong(Row::events).sum());
        return new Result(s, sorted.size(), (int) sorted.stream().filter(r -> r.outcome().equals("passed")).count(),
                outcomes, totals, sorted, (System.nanoTime() - start) / 1_000_000);
    }

    static Row runOne(long seed, SimConfig cfg) {
        try {
            return row(seed, "passed", "", Simulation.run(seed, cfg), -1);
        } catch (SimulationFailure f) {
            String msg = f.getMessage();
            if (msg.length() > 60_000) msg = msg.substring(0, 60_000) + "\n...";
            if (f.partial() == null) {
                return new Row(seed, category(f.getCause()), msg, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1,
                        0, 0, 0, 0, f.failedAtMs());
            }
            return row(seed, category(f.getCause()), msg, f.partial(), f.failedAtMs());
        }
    }

    private static Row row(long seed, String outcome, String msg, SimResult r, long failedAtMs) {
        {
            return new Row(seed, outcome, msg, r.leadersElected(), r.maxTerm(), r.crashes(), r.midWriteCrashes(), r.partitions(),
                    r.pauses(), r.opsStarted(), r.writesCompleted(), r.readsCompleted(), r.timeouts(), r.redirects(),
                    r.duplicatesAnswered(), r.unansweredAtEnd(), r.linearizabilitySteps(), r.maxCommitIndex(),
                    r.snapshotsTaken(), r.snapshotsInstalled(), r.newLeadersSinceScenario(), r.scenarioStepDownMs(),
                    r.slowestClientIdleMs(), r.batches() == 0 ? 0 : Math.round(100.0 * r.batchedEvents() / r.batches()) / 100.0,
                    r.maxBatch(), r.eventsProcessed(), failedAtMs);
        }
    }

    /** The checker's name for an invariant violation; "assertion: <type>" for a guard inside production code. */
    static String category(Throwable cause) {
        String msg = String.valueOf(cause.getMessage());
        if (cause instanceof InvariantViolation) {
            for (String c : CHECKERS) if (msg.startsWith(c)) return c;
            return "Invariant";
        }
        return "assertion: " + cause.getClass().getSimpleName();
    }
}
