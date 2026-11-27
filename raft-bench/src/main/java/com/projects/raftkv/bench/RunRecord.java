package com.projects.raftkv.bench;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;

/**
 * One saved run (DESIGN.md M6-C): what was run ({@code settings}), on what ({@code gitCommit},
 * {@code machine}), and what came out ({@code result}). Stored as {@code results/<id>.json}; the dashboard
 * lists, shows and overlays these, and BENCHMARKS.md is written from them.
 *
 * @param kind "sim", "load" or "failover"
 */
public record RunRecord(String id, String kind, String label, String createdAt, String gitCommit,
                        Map<String, String> machine, JsonNode settings, JsonNode result) {
    public RunRecord {
        Objects.requireNonNull(id);
        Objects.requireNonNull(kind);
        Objects.requireNonNull(settings);
        Objects.requireNonNull(result);
    }
}
