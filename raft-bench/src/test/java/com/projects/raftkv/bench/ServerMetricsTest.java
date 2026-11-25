package com.projects.raftkv.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The Prometheus text parser behind the benchmark's server-side numbers (fsync time, batch size). */
class ServerMetricsTest {

    private static Map<String, Double> parse(String... lines) {
        Map<String, Double> m = new HashMap<>();
        for (String l : lines) {
            Map.Entry<String, Double> e = ServerMetrics.parseLine(l);
            if (e != null) m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    @Test
    void dropsTheNodeLabelAndKeepsQuantileAndBucketLabels() {
        Map<String, Double> m = parse(
                "raft_fsync_seconds_count{node=\"n1\"} 701",
                "raft_batch_size{node=\"n1\",quantile=\"0.99\"} 23.0",
                "raft_fsync_seconds_bucket{le=\"0.005\",node=\"n1\"} 650",
                "raft_fsync_seconds_bucket{node=\"n1\",le=\"+Inf\"} 701");
        assertThat(m).containsOnlyKeys("raft_fsync_seconds_count", "raft_batch_size{quantile=\"0.99\"}",
                "raft_fsync_seconds_bucket{le=\"0.005\"}", "raft_fsync_seconds_bucket{le=\"+Inf\"}");
        assertThat(m.get("raft_batch_size{quantile=\"0.99\"}")).isEqualTo(23.0);
    }

    /** "quantile=" ends in "le=": a naive search for the le label would mangle every quantile key. */
    @Test
    void aQuantileLabelIsNotMistakenForABucketBound() {
        assertThat(parse("raft_batch_size{node=\"n2\",quantile=\"0.5\"} 4.0")).containsOnlyKeys("raft_batch_size{quantile=\"0.5\"}");
    }

    @Test
    void unparseableValuesAreSkipped() {
        assertThat(parse("raft_batch_size{node=\"n1\",quantile=\"0.99\"} NaN-ish")).isEmpty();
    }

    @Test
    void percentileComesFromTheFirstBucketCoveringIt() {
        Map<String, Double> m = parse(
                "raft_fsync_seconds_count{node=\"n1\"} 100",
                "raft_fsync_seconds_max{node=\"n1\"} 0.042",
                "raft_fsync_seconds_bucket{le=\"0.001\",node=\"n1\"} 10",
                "raft_fsync_seconds_bucket{le=\"0.005\",node=\"n1\"} 90",
                "raft_fsync_seconds_bucket{le=\"0.01\",node=\"n1\"} 99",
                "raft_fsync_seconds_bucket{le=\"0.05\",node=\"n1\"} 100",
                "raft_fsync_seconds_bucket{le=\"+Inf\",node=\"n1\"} 100");
        assertThat(ServerMetrics.bucketPercentile(m, "raft_fsync_seconds", 0.50)).isEqualTo(0.005);
        assertThat(ServerMetrics.bucketPercentile(m, "raft_fsync_seconds", 0.99)).isEqualTo(0.01);
        assertThat(ServerMetrics.bucketPercentile(m, "raft_fsync_seconds", 1.00)).isEqualTo(0.05);
    }

    @Test
    void onlyTheInfiniteBucketFallsBackToTheMaximum() {
        Map<String, Double> m = parse(
                "raft_fsync_seconds_count{node=\"n1\"} 3",
                "raft_fsync_seconds_max{node=\"n1\"} 7.5",
                "raft_fsync_seconds_bucket{le=\"1.0\",node=\"n1\"} 0",
                "raft_fsync_seconds_bucket{le=\"+Inf\",node=\"n1\"} 3");
        assertThat(ServerMetrics.bucketPercentile(m, "raft_fsync_seconds", 0.99)).isEqualTo(7.5);
    }

    @Test
    void noObservationsMeansZero() {
        assertThat(ServerMetrics.bucketPercentile(parse("raft_fsync_seconds_count{node=\"n1\"} 0"), "raft_fsync_seconds", 0.99))
                .isZero();
    }
}
