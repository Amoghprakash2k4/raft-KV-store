package com.projects.raftkv.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

/** The latency summary: recorded in microseconds, reported in milliseconds. */
class LatencyTest {

    @Test
    void percentilesOfAUniformSpread() {
        Histogram h = new Histogram(3);
        for (int ms = 1; ms <= 100; ms++) h.recordValue(ms * 1000L);   // 1..100 ms, once each

        Latency l = Latency.of(h);
        assertThat(l.count()).isEqualTo(100);
        assertThat(l.p50Ms()).isCloseTo(50, within(0.1));
        assertThat(l.p90Ms()).isCloseTo(90, within(0.1));
        assertThat(l.p99Ms()).isCloseTo(99, within(0.1));
        assertThat(l.maxMs()).isCloseTo(100, within(0.1));
        assertThat(l.meanMs()).isCloseTo(50.5, within(0.1));
    }

    @Test
    void distributionBucketsAddUpToTheCount() {
        Histogram h = new Histogram(3);
        for (int i = 0; i < 1000; i++) h.recordValue(200 + (i * 37L) % 40_000);   // 0.2 .. 40 ms
        Latency l = Latency.of(h);
        assertThat(l.distribution().stream().mapToDouble(b -> b[1]).sum()).isEqualTo(1000);
        double previous = 0;
        for (double[] b : l.distribution()) {
            assertThat(b[0]).as("bucket bounds rise").isGreaterThan(previous);
            previous = b[0];
        }
    }

    @Test
    void anEmptyHistogramIsAllZeros() {
        Latency l = Latency.of(new Histogram(3));
        assertThat(l.count()).isZero();
        assertThat(l.p99Ms()).isZero();
        assertThat(l.distribution()).isEmpty();
    }
}
