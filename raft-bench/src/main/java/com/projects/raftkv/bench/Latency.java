package com.projects.raftkv.bench;

import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;

/** Latency summary in milliseconds, from an HdrHistogram recorded in microseconds. */
public record Latency(long count, double meanMs, double p50Ms, double p90Ms, double p99Ms, double p999Ms, double maxMs,
                      List<double[]> distribution) {

    /** {@code distribution}: [upper bound ms, count] buckets on a log scale, for the histogram chart. */
    public static Latency of(Histogram h) {
        List<double[]> buckets = new ArrayList<>();
        if (h.getTotalCount() > 0) {
            for (HistogramIterationValue v : h.logarithmicBucketValues(100, 1.25)) {   // from 0.1 ms, x1.25 per bucket
                if (v.getCountAddedInThisIterationStep() > 0) {
                    buckets.add(new double[] {round(v.getValueIteratedTo() / 1000.0), v.getCountAddedInThisIterationStep()});
                }
            }
        }
        return new Latency(h.getTotalCount(), round(h.getMean() / 1000.0), ms(h, 50), ms(h, 90), ms(h, 99), ms(h, 99.9),
                round(h.getMaxValue() / 1000.0), buckets);
    }

    private static double ms(Histogram h, double p) {
        return h.getTotalCount() == 0 ? 0 : round(h.getValueAtPercentile(p) / 1000.0);
    }

    static double round(double v) { return Math.round(v * 1000) / 1000.0; }
}
