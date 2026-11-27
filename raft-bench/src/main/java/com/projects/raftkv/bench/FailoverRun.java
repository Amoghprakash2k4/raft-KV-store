package com.projects.raftkv.bench;

import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.HttpClusterClient;
import com.projects.raftkv.statemachine.KvCommand;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.HdrHistogram.Histogram;

/**
 * Leader failover time (DESIGN.md M6-B): SIGKILL the leader, again and again, while a probe client writes
 * continuously. For each kill two numbers:
 * <ul>
 *   <li><b>election</b>: kill → another node reports itself leader of a newer term (polled every 5 ms);</li>
 *   <li><b>client gap</b>: the longest time between two consecutive completed probe writes, from 1 s before
 *       the kill until the first write completed after the new leader appeared. What a user notices. (Not
 *       simply "kill → next success": a write committed just before the kill can be answered just after
 *       it and would hide the real gap.)</li>
 * </ul>
 * After each kill the old leader is restarted from its files and allowed to catch up.
 */
public final class FailoverRun {

    public record Spec(int nodes, boolean fsyncBatching, int kills) {
        public Spec {
            if (nodes != 3 && nodes != 5) throw new IllegalArgumentException("nodes must be 3 or 5");
            if (kills < 1 || kills > 1_000) throw new IllegalArgumentException("kills must be 1..1000");
        }

        public String label() {
            return nodes + " nodes, " + kills + " leader kills, batching " + (fsyncBatching ? "on" : "off");
        }
    }

    public record Kill(int n, int oldLeader, int newLeader, long newTerm, double electionMs, double clientGapMs) {}

    public record Result(Spec spec, List<Kill> kills, Latency election, Latency clientGap) {}

    public record Progress(int done, int of, Optional<Kill> last) {}

    private FailoverRun() {}

    public static Result run(Spec spec, java.nio.file.Path workDir, Consumer<Progress> progress) throws InterruptedException {
        try (ProcessCluster cluster = ProcessCluster.start(spec.nodes(), spec.fsyncBatching(), workDir)) {
            ConcurrentSkipListSet<Long> successes = new ConcurrentSkipListSet<>();
            AtomicBoolean stop = new AtomicBoolean();
            Thread probe = Thread.ofPlatform().daemon().name("failover-probe").start(() -> {
                HttpClusterClient c = new HttpClusterClient(cluster.clientUrls(), 77, new ClientSession.Config(100, 5), 30_000);
                long n = 0;
                while (!stop.get()) {
                    try {
                        c.execute(new KvCommand.Put("probe", String.valueOf(++n)));
                        successes.add(System.nanoTime());
                    } catch (TimeoutException ignored) {
                        // keep probing
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            List<Kill> kills = new ArrayList<>();
            Histogram election = new Histogram(3), gap = new Histogram(3);
            try {
                for (int k = 1; k <= spec.kills(); k++) {
                    cluster.awaitCaughtUp(Duration.ofSeconds(20));
                    Thread.sleep(300);
                    ProcessCluster.Status old = cluster.awaitLeader(Duration.ofSeconds(20));
                    long t0 = System.nanoTime();
                    cluster.kill(old.id());
                    ProcessCluster.Status next = awaitNewLeader(cluster, old.term(), Duration.ofSeconds(20));
                    long elected = System.nanoTime();
                    double electionMs = (elected - t0) / 1e6;
                    long firstAfter = awaitSuccessAfter(successes, elected, Duration.ofSeconds(20));
                    double gapMs = longestGap(successes.subSet(t0 - 1_000_000_000L, true, firstAfter, true), t0) / 1e6;
                    Kill kill = new Kill(k, old.id(), next.id(), next.term(), Latency.round(electionMs), Latency.round(gapMs));
                    kills.add(kill);
                    election.recordValue(Math.max(1, (long) (electionMs * 1000)));
                    gap.recordValue(Math.max(1, (long) (gapMs * 1000)));
                    cluster.launch(old.id());
                    successes.headSet(t0).clear();
                    progress.accept(new Progress(k, spec.kills(), Optional.of(kill)));
                }
            } finally {
                stop.set(true);
                probe.interrupt();
            }
            return new Result(spec, kills, Latency.of(election), Latency.of(gap));
        }
    }

    private static ProcessCluster.Status awaitNewLeader(ProcessCluster cluster, long oldTerm, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<ProcessCluster.Status> l = cluster.leader();
            if (l.isPresent() && l.get().term() > oldTerm) return l.get();
            Thread.sleep(5);
        }
        throw new IllegalStateException("no new leader within " + timeout);
    }

    /** Longest distance between consecutive success times (or from t0, if nothing succeeded before it). */
    static long longestGap(java.util.SortedSet<Long> times, long t0) {
        long prev = times.isEmpty() || times.first() > t0 ? t0 : times.first();
        long max = 0;
        for (long t : times) {
            max = Math.max(max, t - prev);
            prev = t;
        }
        return max;
    }

    private static long awaitSuccessAfter(ConcurrentSkipListSet<Long> successes, long t0, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Long s = successes.higher(t0);
            if (s != null) return s;
            Thread.sleep(2);
        }
        throw new IllegalStateException("the probe client wrote nothing within " + timeout + " of the kill");
    }
}
