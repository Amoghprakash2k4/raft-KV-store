package com.projects.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.HttpClusterClient;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.sim.HistoryRecorder;
import com.projects.raftkv.sim.LinearizabilityChecker;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * DESIGN.md M5-E: three real server processes on real disks. Clients run the whole time while a chaos
 * thread SIGKILLs nodes (often the leader) and restarts them from their files. At the end the history the
 * clients saw must be linearizable (invariant 7, the simulator's own checker), and every node must have
 * caught up. Run with {@code ./gradlew :raft-server:integrationTest}.
 */
@Tag("integration")
class KillNineIntegrationTest {
    private static final int NODES = 3;
    private static final int CLIENTS = 4;
    private static final int KILLS = Integer.getInteger("kill9.kills", 12);
    private static final List<String> KEYS = List.of("a", "b", "c");
    private static final Pattern NUM = Pattern.compile("\"(\\w+)\":(\\d+|null|\"\\w+\")");

    @TempDir Path dir;
    private final Map<NodeId, Process> procs = new LinkedHashMap<>();
    private final Map<NodeId, Integer> peerPorts = new LinkedHashMap<>();
    private final Map<NodeId, Integer> clientPorts = new LinkedHashMap<>();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
    private final long t0 = System.nanoTime();

    private long nowMs() { return (System.nanoTime() - t0) / 1_000_000; }

    @Test
    @Timeout(300)
    void historyStaysLinearizableWhileNodesAreKilledAndRestarted() throws Exception {
        for (int i = 1; i <= NODES; i++) {
            peerPorts.put(new NodeId(i), LocalCluster.freePort());
            clientPorts.put(new NodeId(i), LocalCluster.freePort());
        }
        try {
            for (NodeId id : peerPorts.keySet()) launch(id);
            awaitLeader(Duration.ofSeconds(20));

            List<HistoryRecorder.Operation> history = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger opIds = new AtomicInteger();
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> clients = new ArrayList<>();
            for (int c = 1; c <= CLIENTS; c++) {
                int clientNo = c;
                clients.add(Thread.ofPlatform().name("client-" + c).start(() -> runClient(clientNo, history, opIds, stop)));
            }

            Random rng = new Random(42);
            int leaderKills = 0;
            for (int k = 0; k < KILLS; k++) {
                Thread.sleep(800 + rng.nextInt(700));
                Optional<NodeId> leader = currentLeader();
                NodeId victim = leader.isPresent() && rng.nextBoolean() ? leader.get()
                        : new NodeId(1 + rng.nextInt(NODES));
                if (leader.equals(Optional.of(victim))) leaderKills++;
                procs.get(victim).destroyForcibly().waitFor();   // SIGKILL: no shutdown hook, no flush
                log("kill -9 " + victim + (leader.equals(Optional.of(victim)) ? " (leader)" : ""));
                Thread.sleep(300 + rng.nextInt(900));
                launch(victim);
                log("restart " + victim);
            }
            Thread.sleep(4_000);   // quiet period: everything up, clients still running
            stop.set(true);
            for (Thread t : clients) t.join(20_000);

            NodeId leader = awaitLeader(Duration.ofSeconds(20));
            long commit = status(leader).get("commitIndex");
            for (NodeId id : peerPorts.keySet()) awaitApplied(id, commit, Duration.ofSeconds(20));

            long completed = history.stream().filter(o -> o.output().isPresent()).count();
            long unknown = history.size() - completed;
            LinearizabilityChecker.Result r = LinearizabilityChecker.check(List.copyOf(history), 20_000_000);
            log("ops=" + history.size() + " completed=" + completed + " unknown=" + unknown + " kills=" + KILLS
                    + " (leader " + leaderKills + ") commitIndex=" + commit + " linSteps=" + r.steps() + " -> " + r.describe());
            assertThat(r.linearizable()).as(r.describe()).isTrue();
            assertThat(history.stream().filter(o -> o.output().orElse(null) instanceof KvResult.Stale)).isEmpty();
            assertThat(completed).isGreaterThan(200);
            assertThat(leaderKills).isPositive();
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
        }
    }

    private void runClient(int clientNo, List<HistoryRecorder.Operation> history, AtomicInteger opIds, AtomicBoolean stop) {
        Map<NodeId, URI> urls = new LinkedHashMap<>();
        clientPorts.forEach((id, port) -> urls.put(id, URI.create("http://localhost:" + port)));
        HttpClusterClient client = new HttpClusterClient(urls, 1000 + clientNo, new ClientSession.Config(400, 50), 4_000);
        Random rng = new Random(clientNo);
        int n = 0;
        while (!stop.get()) {
            String key = KEYS.get(rng.nextInt(KEYS.size()));
            String value = "c" + clientNo + "-" + (++n);   // unique values (M4-E)
            int dice = rng.nextInt(10);
            KvCommand cmd = dice < 4 ? new KvCommand.Put(key, value)
                    : dice < 7 ? new KvCommand.Get(key)
                    : dice < 9 ? new KvCommand.Cas(key, Optional.of("c" + (1 + rng.nextInt(CLIENTS)) + "-" + Math.max(1, n - rng.nextInt(3))), value)
                    : new KvCommand.Delete(key);
            long invoke = nowMs();
            try {
                KvResult result = client.execute(cmd);
                long ret = Math.max(invoke, nowMs());
                history.add(new HistoryRecorder.Operation(opIds.incrementAndGet(), "c" + clientNo, cmd, invoke, ret, Optional.of(result)));
            } catch (TimeoutException e) {
                history.add(new HistoryRecorder.Operation(opIds.incrementAndGet(), "c" + clientNo, cmd, invoke,
                        HistoryRecorder.UNKNOWN_RETURN, Optional.empty()));
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // ---------------------------------------------------------------- processes

    private void launch(NodeId id) throws Exception {
        String peers = String.join(",", peerPorts.entrySet().stream()
                .map(e -> e.getKey().value() + "=localhost:" + e.getValue()).toList());
        ProcessBuilder pb = new ProcessBuilder(ProcessHandle.current().info().command().orElse("java"),
                "-Xmx256m", "-cp", System.getProperty("raft.server.classpath"), "com.projects.raftkv.server.Main");
        pb.environment().put("RAFT_NODE_ID", String.valueOf(id.value()));
        pb.environment().put("RAFT_PEERS", peers);
        pb.environment().put("RAFT_PEER_PORT", String.valueOf(peerPorts.get(id)));
        pb.environment().put("RAFT_CLIENT_PORT", String.valueOf(clientPorts.get(id)));
        pb.environment().put("RAFT_DATA_DIR", dir.resolve("data-" + id.value()).toString());
        pb.environment().put("RAFT_SNAPSHOT_EVERY", "200");
        File out = dir.resolve("node-" + id.value() + ".log").toFile();
        pb.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(out));
        procs.put(id, pb.start());
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (!procs.get(id).isAlive()) throw new AssertionError(id + " exited; see " + out);
            try {
                status(id);
                return;
            } catch (java.io.IOException notYet) {
                Thread.sleep(50);
            }
        }
        throw new AssertionError(id + " did not come up; see " + out);
    }

    private Map<String, Long> status(NodeId id) throws java.io.IOException, InterruptedException {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + clientPorts.get(id) + "/status"))
                .timeout(Duration.ofMillis(500)).build(), HttpResponse.BodyHandlers.ofString());
        Map<String, Long> out = new LinkedHashMap<>();
        Matcher m = NUM.matcher(r.body());
        while (m.find()) {
            String v = m.group(2);
            out.put(m.group(1), v.equals("null") ? -1 : v.startsWith("\"") ? (v.equals("\"LEADER\"") ? 1 : 0) : Long.parseLong(v));
        }
        return out;
    }

    private Optional<NodeId> currentLeader() throws InterruptedException {
        for (NodeId id : peerPorts.keySet()) {
            try {
                if (status(id).get("role") == 1) return Optional.of(id);
            } catch (java.io.IOException down) {
                // not up
            }
        }
        return Optional.empty();
    }

    private NodeId awaitLeader(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<NodeId> l = currentLeader();
            if (l.isPresent()) return l.get();
            Thread.sleep(50);
        }
        throw new AssertionError("no leader within " + timeout);
    }

    private void awaitApplied(NodeId id, long index, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        long applied = -1;
        while (System.nanoTime() < deadline) {
            applied = status(id).get("lastApplied");
            if (applied >= index) return;
            Thread.sleep(50);
        }
        throw new AssertionError(id + " applied only " + applied + " of " + index);
    }

    private void log(String s) {
        System.out.println("[kill9 " + nowMs() + "ms] " + s);
    }
}
