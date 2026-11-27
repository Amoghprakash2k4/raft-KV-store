package com.projects.raftkv.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.raftkv.core.NodeId;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * N real {@code raft-server} processes on localhost (one JVM each, own data directory, own ports), the
 * thing every benchmark measures. Nodes can be SIGKILLed and restarted from their files.
 */
public final class ProcessCluster implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    /** One node's /status. */
    public record Status(int id, String role, long term, int leaderId, long commitIndex, long lastApplied) {
        public boolean leader() { return role.equals("LEADER"); }
    }

    private final int size;
    private final boolean fsyncBatching;
    private final Path dir;
    private final Map<Integer, Integer> peerPorts = new LinkedHashMap<>();
    private final Map<Integer, Integer> clientPorts = new LinkedHashMap<>();
    private final Map<Integer, Process> procs = new LinkedHashMap<>();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(200)).build();

    private ProcessCluster(int size, boolean fsyncBatching, Path dir) {
        this.size = size;
        this.fsyncBatching = fsyncBatching;
        this.dir = dir;
    }

    /** Starts {@code size} fresh nodes (empty data directories under {@code dir}) and waits for a leader. */
    public static ProcessCluster start(int size, boolean fsyncBatching, Path dir) throws InterruptedException {
        if (size != 3 && size != 5) throw new IllegalArgumentException("3 or 5 nodes");
        ProcessCluster c = new ProcessCluster(size, fsyncBatching, dir);
        for (int id = 1; id <= size; id++) {
            c.peerPorts.put(id, freePort());
            c.clientPorts.put(id, freePort());
        }
        try {
            for (int id = 1; id <= size; id++) c.launch(id);
            c.awaitLeader(Duration.ofSeconds(20));
        } catch (RuntimeException | InterruptedException e) {
            c.close();
            throw e;
        }
        return c;
    }

    /**
     * An already-running cluster reached by its client ports on localhost, for example
     * {@code docker compose} on 8081..8083. Nothing is started, killed or deleted: {@link #close()} only
     * lets go of it.
     */
    public static ProcessCluster attach(Map<Integer, Integer> clientPorts) throws InterruptedException {
        ProcessCluster c = new ProcessCluster(clientPorts.size(), true, null);
        c.clientPorts.putAll(clientPorts);
        c.awaitLeader(Duration.ofSeconds(20));
        return c;
    }

    public int size() { return size; }

    public Map<NodeId, URI> clientUrls() {
        Map<NodeId, URI> out = new LinkedHashMap<>();
        clientPorts.forEach((id, port) -> out.put(new NodeId(id), URI.create("http://localhost:" + port)));
        return out;
    }

    public void launch(int id) throws InterruptedException {
        String peers = String.join(",", peerPorts.entrySet().stream().map(e -> e.getKey() + "=localhost:" + e.getValue()).toList());
        ProcessBuilder pb = new ProcessBuilder(ProcessHandle.current().info().command().orElse("java"),
                "-Xmx512m", "-cp", System.getProperty("java.class.path"), "com.projects.raftkv.server.Main");
        Map<String, String> env = pb.environment();
        env.put("RAFT_NODE_ID", String.valueOf(id));
        env.put("RAFT_PEERS", peers);
        env.put("RAFT_PEER_PORT", String.valueOf(peerPorts.get(id)));
        env.put("RAFT_CLIENT_PORT", String.valueOf(clientPorts.get(id)));
        env.put("RAFT_DATA_DIR", dir.resolve("data-" + id).toString());
        env.put("RAFT_FSYNC_BATCH", String.valueOf(fsyncBatching));
        File log = dir.resolve("node-" + id + ".log").toFile();
        try {
            Files.createDirectories(dir);
            procs.put(id, pb.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(log)).start());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (!procs.get(id).isAlive()) throw new IllegalStateException("node " + id + " exited; see " + log);
            if (status(id).isPresent()) return;
            Thread.sleep(25);
        }
        throw new IllegalStateException("node " + id + " did not start; see " + log);
    }

    /** SIGKILL: no shutdown hook, nothing flushed. */
    public void kill(int id) throws InterruptedException {
        Process p = procs.remove(id);
        if (p != null) p.destroyForcibly().waitFor();
    }

    public Optional<Status> status(int id) throws InterruptedException {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + clientPorts.get(id) + "/status"))
                    .timeout(Duration.ofMillis(300)).build(), HttpResponse.BodyHandlers.ofString());
            JsonNode j = JSON.readTree(r.body());
            return Optional.of(new Status(j.get("id").asInt(), j.get("role").asText(), j.get("term").asLong(),
                    j.get("leaderId").isNull() ? 0 : j.get("leaderId").asInt(), j.get("commitIndex").asLong(),
                    j.get("lastApplied").asLong()));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<Status> leader() throws InterruptedException {
        for (int id : clientPorts.keySet()) {   // a killed node simply has no status
            Optional<Status> s = status(id);
            if (s.isPresent() && s.get().leader()) return s;
        }
        return Optional.empty();
    }

    public Status awaitLeader(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Status> l = leader();
            if (l.isPresent()) return l.get();
            Thread.sleep(20);
        }
        throw new IllegalStateException("no leader within " + timeout);
    }

    /** Waits until every running node has applied at least the leader's current commit index. */
    public void awaitCaughtUp(Duration timeout) throws InterruptedException {
        long target = awaitLeader(timeout).commitIndex();
        long deadline = System.nanoTime() + timeout.toNanos();
        for (int id : procs.keySet()) {
            while (status(id).map(s -> s.lastApplied() < target).orElse(true)) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("node " + id + " did not catch up");
                Thread.sleep(20);
            }
        }
    }

    public boolean running(int id) { return procs.containsKey(id); }

    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Kills every node and deletes the data directories. */
    @Override
    public void close() {
        for (Process p : procs.values()) p.destroyForcibly();
        for (Process p : procs.values()) {
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        procs.clear();
        if (dir == null) return;   // attached: not ours to delete
        try (Stream<Path> files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
        } catch (IOException ignored) {
            // best effort: it is a temp directory
        }
    }
}
