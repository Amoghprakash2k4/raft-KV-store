package com.projects.raftkv.bench;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Reads a few numbers from a node's Prometheus text (/metrics). */
final class ServerMetrics {
    private ServerMetrics() {}

    static LoadRun.ServerStats leaderStats(ProcessCluster cluster) throws InterruptedException {
        Optional<ProcessCluster.Status> leader = cluster.leader();
        if (leader.isEmpty()) return new LoadRun.ServerStats(0, 0, 0, 0, 0);
        Map<String, Double> m = scrape(cluster.clientUrls().get(new com.projects.raftkv.core.NodeId(leader.get().id())));
        double fsyncs = m.getOrDefault("raft_fsync_seconds_count", 0.0);
        double fsyncSum = m.getOrDefault("raft_fsync_seconds_sum", 0.0);
        double batches = m.getOrDefault("raft_batch_size_count", 0.0);
        double events = m.getOrDefault("raft_batch_size_sum", 0.0);
        return new LoadRun.ServerStats((long) fsyncs, Latency.round(fsyncs == 0 ? 0 : fsyncSum / fsyncs * 1000),
                Latency.round(bucketPercentile(m, "raft_fsync_seconds", 0.99) * 1000),
                Latency.round(batches == 0 ? 0 : events / batches), m.getOrDefault("raft_batch_size{quantile=\"0.99\"}", 0.0));
    }

    /**
     * A percentile from a Prometheus histogram's cumulative buckets ({@code name_bucket{le="x"}}): the
     * upper bound of the first bucket holding at least {@code p} of the observations. The fsync timer is
     * published as a histogram, which has no quantile lines. Resolution is the bucket width; the
     * {@code +Inf} bucket falls back to the observed maximum.
     */
    static double bucketPercentile(Map<String, Double> m, String name, double p) {
        double count = m.getOrDefault(name + "_count", 0.0);
        if (count == 0) return 0;
        TreeMap<Double, Double> buckets = new TreeMap<>();
        String prefix = name + "_bucket{le=\"";
        for (Map.Entry<String, Double> e : m.entrySet()) {
            if (!e.getKey().startsWith(prefix)) continue;
            String le = e.getKey().substring(prefix.length(), e.getKey().length() - 2);
            buckets.put(le.equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(le), e.getValue());
        }
        for (Map.Entry<Double, Double> b : buckets.entrySet()) {
            if (b.getValue() >= p * count) {
                return b.getKey().isInfinite() ? m.getOrDefault(name + "_max", 0.0) : b.getKey();
            }
        }
        return m.getOrDefault(name + "_max", 0.0);
    }

    /** One line of Prometheus text → [key, value]; key keeps a quantile or le label, drops the node label. */
    static Map.Entry<String, Double> parseLine(String line) {
        int sp = line.lastIndexOf(' ');
        String name = line.substring(0, sp);
        String key = name.contains("{") ? name.substring(0, name.indexOf('{')) : name;
        int q = name.indexOf("quantile=");
        if (q >= 0) key += "{" + name.substring(q, name.indexOf('"', q + 10) + 1) + "}";
        int le = name.indexOf("{le=");                // a whole label named le, not the tail of "quantile="
        if (le < 0) le = name.indexOf(",le=");
        if (le >= 0) key += "{" + name.substring(le + 1, name.indexOf('"', le + 5) + 1) + "}";
        try {
            return Map.entry(key, Double.parseDouble(line.substring(sp + 1)));
        } catch (NumberFormatException e) {
            return null;   // NaN etc.
        }
    }

    /** "name{node=..,quantile=..|le=..} value" → key "name", "name{quantile=..}" or "name_bucket{le=..}". */
    static Map<String, Double> scrape(URI base) throws InterruptedException {
        Map<String, Double> out = new HashMap<>();
        try {
            HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(base.resolve("/metrics"))
                    .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
            for (String line : r.body().split("\n")) {
                if (line.startsWith("#") || line.isBlank()) continue;
                Map.Entry<String, Double> e = parseLine(line);
                if (e != null) out.put(e.getKey(), e.getValue());
            }
        } catch (IOException e) {
            // leave empty
        }
        return out;
    }
}
