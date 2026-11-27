package com.projects.raftkv.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Saves and loads {@link RunRecord}s as pretty JSON files in one directory ({@code results/}). */
public final class ResultStore {
    public static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final DateTimeFormatter ID = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS");

    private final Path dir;

    public ResultStore(Path dir) {
        this.dir = dir;
    }

    public Path dir() { return dir; }

    /** Wraps a result with its settings, commit and machine, writes it, and returns the record. */
    public synchronized RunRecord save(String kind, String label, Object settings, Object result) {
        LocalDateTime now = LocalDateTime.now();
        String id = kind + "-" + now.format(ID);
        RunRecord r = new RunRecord(id, kind, label, now.toString(), gitCommit(), machine(),
                JSON.valueToTree(settings), JSON.valueToTree(result));
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(id + ".json"), JSON.writeValueAsString(r), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot save " + id, e);
        }
        return r;
    }

    /** Every saved run, newest first. Files that don't parse are skipped. */
    public List<RunRecord> list() {
        List<RunRecord> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) return out;
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                try {
                    out.add(JSON.readValue(f.toFile(), RunRecord.class));
                } catch (IOException skip) {
                    // not one of ours
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        out.sort(Comparator.comparing(RunRecord::createdAt).reversed());
        return out;
    }

    public Optional<RunRecord> get(String id) {
        if (!id.matches("[a-z]+-[0-9-]+")) return Optional.empty();   // no path tricks
        Path f = dir.resolve(id + ".json");
        if (!Files.exists(f)) return Optional.empty();
        try {
            return Optional.of(JSON.readValue(f.toFile(), RunRecord.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean delete(String id) {
        if (!id.matches("[a-z]+-[0-9-]+")) return false;
        try {
            return Files.deleteIfExists(dir.resolve(id + ".json"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode tree(Object o) { return JSON.valueToTree(o); }

    static String gitCommit() {
        try {
            Process p = new ProcessBuilder("git", "describe", "--always", "--dirty").redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0 ? out : "unknown";
        } catch (IOException e) {
            return "unknown";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unknown";
        }
    }

    static Map<String, String> machine() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
        m.put("cpus", String.valueOf(Runtime.getRuntime().availableProcessors()));
        m.put("java", System.getProperty("java.version"));
        return m;
    }
}
