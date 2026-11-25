package com.projects.raftkv.bench;

import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.HttpClusterClient;
import com.projects.raftkv.statemachine.KvCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Throughput and latency (DESIGN.md M6-B). {@code clients} closed-loop threads, each a real
 * {@link HttpClusterClient} (sessions, redirects, retries), run for warm-up + duration; only the
 * measured part counts. Latency is from the client's call to its answer, retries included: what a user
 * would see. Every second a sample (throughput, p50, p99) is taken for the time-series chart.
 */
public final class LoadRun {

    /** One second of the run. */
    public record Sample(int second, long ops, double p50Ms, double p99Ms) {}

    public record Result(LoadSpec spec, double opsPerSec, double writesPerSec, double readsPerSec, long ops,
                         long timeouts, Latency writes, Latency reads, List<Sample> perSecond, ServerStats server) {}

    /** Read from the leader's /metrics at the end: fsyncs and how many events each batch carried. */
    public record ServerStats(long fsyncs, double fsyncMeanMs, double fsyncP99Ms, double batchMean, double batchP99) {}

    /** Live progress: phase ("starting", "warm-up", "measuring", "done"), seconds so far, ops/s last second. */
    public record Progress(String phase, int second, int ofSeconds, double opsPerSec, double p99Ms) {}

    private LoadRun() {}

    /** A unique prefix padded with 'x' to {@code bytes} (or the prefix alone, if it is already longer). */
    static String value(String prefix, int bytes) {
        return prefix.length() >= bytes ? prefix : prefix + "x".repeat(bytes - prefix.length());
    }

    public static Result run(LoadSpec spec, java.nio.file.Path workDir, Consumer<Progress> progress) throws InterruptedException {
        progress.accept(new Progress("starting " + spec.nodes() + " servers", 0, spec.warmupSec() + spec.durationSec(), 0, 0));
        try (ProcessCluster cluster = ProcessCluster.start(spec.nodes(), spec.fsyncBatching(), workDir)) {
            return run(spec, cluster, progress);
        }
    }

    public static Result run(LoadSpec spec, ProcessCluster cluster, Consumer<Progress> progress) throws InterruptedException {
        Recorder writeRec = new Recorder(3);
        Recorder readRec = new Recorder(3);
        Histogram writes = new Histogram(3), reads = new Histogram(3);
        AtomicBoolean measuring = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong timeouts = new AtomicLong();

        List<Thread> threads = new ArrayList<>();
        for (int c = 1; c <= spec.clients(); c++) {
            long clientId = 1_000 + c;
            threads.add(Thread.ofPlatform().daemon().name("bench-client-" + c).start(() -> {
                HttpClusterClient client = new HttpClusterClient(cluster.clientUrls(), clientId,
                        new ClientSession.Config(1_000, 20), 10_000);
                SplittableRandom rng = new SplittableRandom(clientId);
                long n = 0;
                while (!stop.get()) {
                    String key = "k" + rng.nextInt(spec.keys());
                    boolean write = rng.nextInt(100) < spec.writePercent();
                    KvCommand cmd = write ? new KvCommand.Put(key, value(clientId + "-" + (++n) + "-", spec.valueBytes()))
                            : new KvCommand.Get(key);
                    long start = System.nanoTime();
                    try {
                        client.execute(cmd);
                    } catch (TimeoutException e) {
                        if (measuring.get()) timeouts.incrementAndGet();
                        continue;
                    } catch (InterruptedException e) {
                        return;
                    }
                    long micros = Math.max(1, (System.nanoTime() - start) / 1_000);
                    if (measuring.get()) (write ? writeRec : readRec).recordValue(micros);
                }
            }));
        }

        int total = spec.warmupSec() + spec.durationSec();
        List<Sample> samples = new ArrayList<>();
        long t0 = System.nanoTime();
        for (int s = 1; s <= total; s++) {
            long wake = t0 + s * 1_000_000_000L;
            while (System.nanoTime() < wake) Thread.sleep(Math.max(1, (wake - System.nanoTime()) / 1_000_000));
            if (s == spec.warmupSec()) {
                measuring.set(true);
                writeRec.reset();
                readRec.reset();
                progress.accept(new Progress("measuring", s, total, 0, 0));
                continue;
            }
            if (s <= spec.warmupSec()) {
                progress.accept(new Progress("warm-up", s, total, 0, 0));
                continue;
            }
            if (spec.warmupSec() == 0 && s == 1) measuring.set(true);
            Histogram w = writeRec.getIntervalHistogram(), r = readRec.getIntervalHistogram();
            writes.add(w);
            reads.add(r);
            Histogram both = w.copy();
            both.add(r);
            Sample sample = new Sample(s - spec.warmupSec(), both.getTotalCount(), Latency.round(both.getValueAtPercentile(50) / 1000.0),
                    Latency.round(both.getValueAtPercentile(99) / 1000.0));
            samples.add(sample);
            progress.accept(new Progress("measuring", s, total, sample.ops(), sample.p99Ms()));
        }
        stop.set(true);
        for (Thread t : threads) t.join(15_000);
        writes.add(writeRec.getIntervalHistogram());
        reads.add(readRec.getIntervalHistogram());

        double secs = spec.durationSec();
        long ops = writes.getTotalCount() + reads.getTotalCount();
        ServerStats server = ServerMetrics.leaderStats(cluster);
        progress.accept(new Progress("done", total, total, ops / secs, 0));
        return new Result(spec, Latency.round(ops / secs), Latency.round(writes.getTotalCount() / secs),
                Latency.round(reads.getTotalCount() / secs), ops, timeouts.get(), Latency.of(writes), Latency.of(reads),
                samples, server);
    }
}
