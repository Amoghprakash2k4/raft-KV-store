package com.projects.raftkv.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command line (DESIGN.md M6-B). Results go to {@code results/} as JSON (see {@link ResultStore}).
 * <pre>
 *   raft-bench load     [--nodes 3] [--clients 16] [--writes 100] [--batching true] [--duration 15] [--warmup 3]
 *                       [--keys 1000] [--value 100] [--repeat 1]
 *   raft-bench failover [--nodes 3] [--kills 50] [--batching true]
 *   raft-bench matrix   [--duration 15] [--repeat 3]   nodes {3,5} x batching {off,on} x clients {1,4,16,64}
 *                                                      x writes {100,50}
 * </pre>
 */
public final class BenchMain {
    private BenchMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: raft-bench load|failover|matrix [--option value]...");
            System.exit(1);
        }
        Map<String, String> o = new HashMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        ResultStore store = new ResultStore(Path.of(o.getOrDefault("results", "results")));
        switch (args[0]) {
            case "load" -> {
                LoadSpec spec = new LoadSpec(i(o, "nodes", 3), b(o, "batching", true), i(o, "clients", 16),
                        i(o, "writes", 100), i(o, "keys", 1_000), i(o, "value", 100), i(o, "warmup", 3), i(o, "duration", 15));
                for (int r = 0; r < i(o, "repeat", 1); r++) load(store, spec);
            }
            case "failover" -> {
                FailoverRun.Spec spec = new FailoverRun.Spec(i(o, "nodes", 3), b(o, "batching", true), i(o, "kills", 50));
                FailoverRun.Result r = FailoverRun.run(spec, Files.createTempDirectory("raft-bench"),
                        p -> p.last().ifPresent(k -> System.out.printf("kill %d/%d: n%d -> n%d (term %d) election %.1f ms, client gap %.1f ms%n",
                                p.done(), p.of(), k.oldLeader(), k.newLeader(), k.newTerm(), k.electionMs(), k.clientGapMs())));
                RunRecord rec = store.save("failover", spec.label(), spec, r);
                System.out.printf("%s: election p50 %.1f p99 %.1f max %.1f ms | client gap p50 %.1f p99 %.1f max %.1f ms -> %s%n",
                        spec.label(), r.election().p50Ms(), r.election().p99Ms(), r.election().maxMs(),
                        r.clientGap().p50Ms(), r.clientGap().p99Ms(), r.clientGap().maxMs(), rec.id());
            }
            case "matrix" -> {
                List<LoadSpec> specs = new ArrayList<>();
                for (int nodes : new int[] {3, 5})
                    for (boolean batching : new boolean[] {false, true})
                        for (int writes : new int[] {100, 50})
                            for (int clients : new int[] {1, 4, 16, 64})
                                specs.add(new LoadSpec(nodes, batching, clients, writes, 1_000, 100, i(o, "warmup", 3), i(o, "duration", 15)));
                int repeat = i(o, "repeat", 3);
                int n = 0;
                for (LoadSpec spec : specs) {
                    for (int r = 0; r < repeat; r++) {
                        System.out.printf("[%d/%d] ", ++n, specs.size() * repeat);
                        load(store, spec);
                    }
                }
            }
            default -> {
                System.err.println("unknown command " + args[0]);
                System.exit(1);
            }
        }
        System.exit(0);
    }

    private static void load(ResultStore store, LoadSpec spec) throws Exception {
        LoadRun.Result r = LoadRun.run(spec, Files.createTempDirectory("raft-bench"), p -> { });
        RunRecord rec = store.save("load", spec.label(), spec, r);
        System.out.printf("%s: %.0f ops/s | write p50 %.2f p99 %.2f ms | read p50 %.2f p99 %.2f ms | fsync mean %.2f ms,"
                        + " batch mean %.1f | timeouts %d -> %s%n",
                spec.label(), r.opsPerSec(), r.writes().p50Ms(), r.writes().p99Ms(), r.reads().p50Ms(), r.reads().p99Ms(),
                r.server().fsyncMeanMs(), r.server().batchMean(), r.timeouts(), rec.id());
    }

    private static int i(Map<String, String> o, String k, int d) { return Integer.parseInt(o.getOrDefault(k, String.valueOf(d))); }

    private static boolean b(Map<String, String> o, String k, boolean d) { return Boolean.parseBoolean(o.getOrDefault(k, String.valueOf(d))); }
}
