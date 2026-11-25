package com.projects.raftkv.server;

import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.HttpClusterClient;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.Role;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** N real nodes in one JVM: real gRPC on localhost, real files, real threads (only the process is shared). */
final class LocalCluster implements AutoCloseable {
    final List<NodeId> ids = new ArrayList<>();
    final Map<NodeId, Integer> peerPorts = new LinkedHashMap<>();
    final Map<NodeId, Integer> clientPorts = new LinkedHashMap<>();
    final Map<NodeId, RaftServer> servers = new LinkedHashMap<>();
    final Map<NodeId, ClientApi> apis = new LinkedHashMap<>();
    final AtomicInteger halts = new AtomicInteger();
    private final Path dir;

    LocalCluster(int n, Path dir) {
        this.dir = dir;
        for (int i = 1; i <= n; i++) {
            NodeId id = new NodeId(i);
            ids.add(id);
            peerPorts.put(id, freePort());
            clientPorts.put(id, freePort());
        }
        for (NodeId id : ids) start(id);
    }

    ServerConfig config(NodeId id) {
        Map<NodeId, InetSocketAddress> members = new LinkedHashMap<>();
        for (NodeId m : ids) members.put(m, InetSocketAddress.createUnresolved("localhost", peerPorts.get(m)));
        return new ServerConfig(id, members, peerPorts.get(id), clientPorts.get(id), dir.resolve("n" + id.value()),
                10, 15, 5, true, true, 50, 3_000);
    }

    void start(NodeId id) {
        RaftServer s = RaftServer.start(config(id), halts::incrementAndGet);
        servers.put(id, s);
        apis.put(id, ClientApi.start(s, clientPorts.get(id)));
    }

    void stop(NodeId id) {
        ClientApi api = apis.remove(id);
        if (api != null) api.close();
        RaftServer s = servers.remove(id);
        if (s != null) s.close();
    }

    URI url(NodeId id) { return URI.create("http://localhost:" + clientPorts.get(id)); }

    Map<NodeId, URI> urls() {
        Map<NodeId, URI> out = new LinkedHashMap<>();
        for (NodeId id : ids) out.put(id, url(id));
        return out;
    }

    HttpClusterClient client(long clientId) {
        return new HttpClusterClient(urls(), clientId, new ClientSession.Config(500, 50), 15_000);
    }

    /** Waits until exactly one running node is leader and every running node knows it. */
    NodeId awaitLeader(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<NodeId> leader = Optional.empty();
            int leaders = 0;
            for (Map.Entry<NodeId, RaftServer> e : servers.entrySet()) {
                if (e.getValue().view().role() == Role.LEADER) {
                    leaders++;
                    leader = Optional.of(e.getKey());
                }
            }
            if (leaders == 1) {
                NodeId l = leader.get();
                boolean allKnow = servers.values().stream()
                        .allMatch(s -> s.view().status().soft().leader().equals(Optional.of(l)));
                if (allKnow) return l;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no stable leader within " + timeout);
    }

    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        for (NodeId id : List.copyOf(servers.keySet())) stop(id);
    }
}
