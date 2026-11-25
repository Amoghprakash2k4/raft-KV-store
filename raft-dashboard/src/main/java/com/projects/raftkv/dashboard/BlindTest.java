package com.projects.raftkv.dashboard;

import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.sim.FaultConfig;
import com.projects.raftkv.sim.SimConfig;
import com.projects.raftkv.sim.WorkloadConfig;
import com.projects.raftkv.storage.StorageMutation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * The blind bug test (DESIGN.md M6-C). The owner picks bug types and a count for each; the page plants
 * them in randomly chosen seeds, leaves the other seeds clean, runs everything, and only then compares the
 * hidden plan with what the checkers flagged:
 * <pre>
 *                    flagged            not flagged
 *   bug planted      caught (good)      missed
 *   clean            FALSE ALARM (bad)  clean pass (good)
 * </pre>
 * Each bug runs under the {@link Profile} that exposes it (the settings MutationSanityTest uses); every
 * clean seed gets a random profile from those in play, so the settings don't give the plan away.
 */
final class BlindTest {

    /** Base settings. A bug's profile is the condition under which it can show at all. */
    enum Profile {
        CHAOS("chaos"),
        CHAOS_NO_HARDENING("chaos, PreVote and CheckQuorum off"),
        FIGURE8("chaos, no hardening, 1 entry per AppendEntries"),
        CRASH_LOOP("crash loop"),
        CRASH_LOOP_GROUP_COMMIT("crash loop + group commit"),
        LEADER_HUNT_NO_CQ("leader hunting, 80% reads, CheckQuorum off"),
        LEADER_HUNT_ONE_KEY("leader hunting, 50% reads, 1 key");

        final String label;

        Profile(String label) {
            this.label = label;
        }

        SimConfig config(int nodes) {
            return switch (this) {
                case CHAOS -> SimConfig.chaos(nodes);
                case CHAOS_NO_HARDENING -> SimConfig.chaos(nodes).withHardening(false, false);
                case FIGURE8 -> SimConfig.chaos(nodes).withHardening(false, false).withMaxEntriesPerAppend(1);
                case CRASH_LOOP -> SimConfig.crashLoop(nodes);
                case CRASH_LOOP_GROUP_COMMIT -> SimConfig.crashLoop(nodes).withBatching(2);
                case LEADER_HUNT_NO_CQ -> hunting(nodes, 80, 2).withHardening(true, false);
                case LEADER_HUNT_ONE_KEY -> hunting(nodes, 50, 1);
            };
        }

        private static SimConfig hunting(int nodes, int readPercent, int keys) {
            return SimConfig.chaos(nodes).withFaults(FaultConfig.leaderHunting())
                    .withWorkload(new WorkloadConfig(5, 5, 30, keys, 150, 10, readPercent));
        }
    }

    /** A plantable bug: where it shows up, how to switch it on, and what it breaks. */
    record Bug(String name, Profile profile, UnaryOperator<SimConfig> plant, String what) {}

    /**
     * Real bugs only: PREVOTE_SKIPS_LOG_CHECK is a control (still safe) and PREVOTE_BUMPS_TERM hurts only
     * liveness, so neither is a bug a safety checker should flag.
     */
    static final List<Bug> BUGS = List.of(
            new Bug("LYING_DISK", Profile.CHAOS, SimConfig::withLyingDisks, "fsync returns without saving anything"),
            new Bug("SKIP_DEDUP", Profile.CHAOS, SimConfig::withoutDedup, "retried requests are executed again"),
            new Bug("TRUNCATE_ON_MATCH", Profile.CHAOS, c -> c.withMutation(ProtocolMutation.TRUNCATE_ON_MATCH),
                    "followers delete matching entries after an AppendEntries"),
            new Bug("NAIVE_INSTALL_SNAPSHOT", Profile.CHAOS, c -> c.withMutation(ProtocolMutation.NAIVE_INSTALL_SNAPSHOT),
                    "stale snapshots are installed and the log thrown away"),
            new Bug("SKIP_UP_TO_DATE_CHECK", Profile.CHAOS_NO_HARDENING, c -> c.withMutation(ProtocolMutation.SKIP_UP_TO_DATE_CHECK),
                    "votes go to candidates with out-of-date logs (§5.4.1)"),
            new Bug("COMMIT_OLD_TERM_BY_COUNT", Profile.FIGURE8, c -> c.withMutation(ProtocolMutation.COMMIT_OLD_TERM_BY_COUNT),
                    "old-term entries committed by counting replicas (Figure 8); needs a rare sequence"),
            new Bug("SKIP_TORN_TAIL_CUT", Profile.CRASH_LOOP, c -> c.withMutation(StorageMutation.SKIP_TORN_TAIL_CUT),
                    "recovery keeps a half-written WAL record"),
            new Bug("SKIP_SNAPSHOT_DATA_FSYNC", Profile.CRASH_LOOP, c -> c.withMutation(StorageMutation.SKIP_SNAPSHOT_DATA_FSYNC),
                    "a snapshot is renamed into place before its bytes are durable"),
            new Bug("SEND_BEFORE_SYNC", Profile.CRASH_LOOP_GROUP_COMMIT, c -> c.withMutation(ProtocolMutation.SEND_BEFORE_SYNC),
                    "messages leave before the batch's fsync"),
            new Bug("READ_WITHOUT_QUORUM_CHECK", Profile.LEADER_HUNT_NO_CQ, c -> c.withMutation(ProtocolMutation.READ_WITHOUT_QUORUM_CHECK),
                    "ReadIndex skips the heartbeat round: a deposed leader answers reads"),
            new Bug("READ_BEFORE_CURRENT_TERM_COMMIT", Profile.LEADER_HUNT_ONE_KEY,
                    c -> c.withMutation(ProtocolMutation.READ_BEFORE_CURRENT_TERM_COMMIT),
                    "ReadIndex before the new leader's no-op commits; needs a read in a short window"));

    static Bug bug(String name) {
        for (Bug b : BUGS) if (b.name().equals(name)) return b;
        throw new IllegalArgumentException("unknown bug " + name);
    }

    /**
     * @param bugs      bug name → how many seeds get it (the rest stay clean)
     * @param planSeed  shuffles which seeds get which bug; 0 = pick one at random (it is saved, so a plan
     *                  can be replayed exactly)
     * @param settings  null = each bug runs under its own {@link Profile}; otherwise every seed, bugged or
     *                  clean, runs under these settings (its mutation field is ignored)
     */
    record Spec(int nodes, int seeds, long firstSeed, long planSeed, Map<String, Integer> bugs, Fixed settings) {
        Spec {
            if (settings != null) settings.base(nodes);   // validates the mix
            if (nodes != 3 && nodes != 5) throw new IllegalArgumentException("nodes must be 3 or 5");
            if (seeds < 1 || seeds > 20_000) throw new IllegalArgumentException("seeds must be 1..20000");
            if (firstSeed < 1) throw new IllegalArgumentException("firstSeed must be >= 1");
            if (bugs == null || bugs.isEmpty()) throw new IllegalArgumentException("pick at least one bug");
            bugs = Collections.unmodifiableMap(new TreeMap<>(bugs));
            int planted = 0;
            for (Map.Entry<String, Integer> e : bugs.entrySet()) {
                bug(e.getKey());
                if (e.getValue() == null || e.getValue() < 0) throw new IllegalArgumentException("bad count for " + e.getKey());
                planted += e.getValue();
            }
            if (planted == 0) throw new IllegalArgumentException("plant at least one bug");
            if (planted > seeds) throw new IllegalArgumentException(planted + " bugs do not fit in " + seeds + " seeds");
        }

        int planted() {
            return bugs.values().stream().mapToInt(Integer::intValue).sum();
        }

        String label() {
            return "Blind test: " + planted() + " bugs (" + bugs.entrySet().stream().filter(e -> e.getValue() > 0).count()
                    + " types) in " + seeds + " seeds, " + nodes + " nodes" + (settings == null ? "" : ", " + settings.label());
        }
    }

    /** The owner's own settings for every seed (the Simulator tab's fault mix and toggles). */
    record Fixed(String mix, boolean preVote, boolean checkQuorum, int batchWindowMs) {
        SimConfig base(int nodes) {
            return new SimSettings(nodes, 1, 1, mix, preVote, checkQuorum, batchWindowMs, "none").toConfig();
        }

        String label() {
            return mix + (preVote ? "" : ", no PreVote") + (checkQuorum ? "" : ", no CheckQuorum") + (batchWindowMs > 0 ? ", group commit" : "");
        }
    }

    /** One seed of the plan. {@code bug} is "none" for a clean seed. */
    record Planned(long seed, String bug, Profile profile) {}

    /** One seed, after the run: the plan next to what the checkers said, and the run's counters. */
    record Row(long seed, String bug, String profile, boolean flagged, String outcome, String message, String verdict,
               SimJob.Row stats) {}

    /** Per bug type: how many planted, how many caught, and by which checker. */
    record PerBug(String bug, String what, String profile, int planted, int caught, Map<String, Integer> caughtBy,
                  long medianCaughtAtMs, long firstCaughtAtMs, long lastCaughtAtMs) {}

    record Result(Spec spec, long planSeed, int seeds, int planted, int clean, int caught, int missed, int falseAlarms,
                  int cleanPasses, List<PerBug> perBug, List<Row> rows, long wallMs) {}

    private BlindTest() {}

    /** Deterministic for a given (spec, planSeed): shuffle the seeds, deal out the bugs, profile the rest. */
    static List<Planned> plan(Spec s, long planSeed) {
        List<Long> seeds = new ArrayList<>();
        for (long x = s.firstSeed(); x < s.firstSeed() + s.seeds(); x++) seeds.add(x);
        Random rnd = new Random(planSeed);
        Collections.shuffle(seeds, rnd);
        List<Planned> out = new ArrayList<>();
        List<Profile> inPlay = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, Integer> e : s.bugs().entrySet()) {
            Bug b = bug(e.getKey());
            if (e.getValue() > 0 && !inPlay.contains(b.profile())) inPlay.add(b.profile());
            for (int k = 0; k < e.getValue(); k++) out.add(new Planned(seeds.get(i++), b.name(), b.profile()));
        }
        while (i < seeds.size()) out.add(new Planned(seeds.get(i++), "none", inPlay.get(rnd.nextInt(inPlay.size()))));
        out.sort((a, b) -> Long.compare(a.seed(), b.seed()));
        return out;
    }

    /**
     * Runs the plan. Progress shows only how many seeds were flagged, never which bug a seed carried: the
     * answer key is revealed in the result.
     */
    static Result run(Spec s, AtomicBoolean cancel, Consumer<SimJob.Progress> progress) throws InterruptedException {
        long planSeed = s.planSeed() != 0 ? s.planSeed() : ThreadLocalRandom.current().nextLong(1, 1_000_000_000L);
        List<Planned> plan = plan(s, planSeed);
        long start = System.nanoTime();
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        AtomicInteger done = new AtomicInteger(), flagged = new AtomicInteger();
        List<Row> rows = Collections.synchronizedList(new ArrayList<>());
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> futures = new ArrayList<>();
            for (Planned p : plan) {
                futures.add(pool.submit(() -> {
                    if (cancel.get()) return;
                    SimConfig cfg = s.settings() == null ? p.profile().config(s.nodes()) : s.settings().base(s.nodes());
                    String settingsLabel = s.settings() == null ? p.profile().label : s.settings().label();
                    if (!p.bug().equals("none")) cfg = bug(p.bug()).plant().apply(cfg);
                    SimJob.Row r = SimJob.runOne(p.seed(), cfg);
                    boolean f = !r.outcome().equals("passed");
                    boolean planted = !p.bug().equals("none");
                    String verdict = planted ? (f ? "caught" : "missed") : (f ? "false alarm" : "clean pass");
                    rows.add(new Row(p.seed(), p.bug(), settingsLabel, f, r.outcome(), r.message(), verdict, r));
                    if (f) flagged.incrementAndGet();
                    int d = done.incrementAndGet();
                    if (d % 10 == 0 || d == plan.size()) progress.accept(new SimJob.Progress(d, plan.size(), d - flagged.get(), flagged.get()));
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
        sorted.sort((a, b) -> Long.compare(a.seed(), b.seed()));
        List<PerBug> perBug = new ArrayList<>();
        for (Map.Entry<String, Integer> e : s.bugs().entrySet()) {
            if (e.getValue() == 0) continue;
            Bug b = bug(e.getKey());
            Map<String, Integer> by = new TreeMap<>();
            int planted = 0, caught = 0;
            List<Long> at = new ArrayList<>();
            for (Row r : sorted) {
                if (!r.bug().equals(b.name())) continue;
                planted++;
                if (r.flagged()) {
                    caught++;
                    by.merge(r.outcome(), 1, Integer::sum);
                    if (r.stats().failedAtMs() >= 0) at.add(r.stats().failedAtMs());
                }
            }
            Collections.sort(at);
            perBug.add(new PerBug(b.name(), b.what(), s.settings() == null ? b.profile().label : s.settings().label(), planted, caught, by,
                    at.isEmpty() ? -1 : at.get(at.size() / 2), at.isEmpty() ? -1 : at.get(0),
                    at.isEmpty() ? -1 : at.get(at.size() - 1)));
        }
        Map<String, Integer> v = new LinkedHashMap<>();
        for (Row r : sorted) v.merge(r.verdict(), 1, Integer::sum);
        int planted = v.getOrDefault("caught", 0) + v.getOrDefault("missed", 0);
        return new Result(s, planSeed, sorted.size(), planted, sorted.size() - planted, v.getOrDefault("caught", 0),
                v.getOrDefault("missed", 0), v.getOrDefault("false alarm", 0), v.getOrDefault("clean pass", 0), perBug,
                sorted, (System.nanoTime() - start) / 1_000_000);
    }
}
