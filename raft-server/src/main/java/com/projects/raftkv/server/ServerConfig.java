package com.projects.raftkv.server;

import com.projects.raftkv.core.NodeId;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One node's settings, normally from environment variables (the contract in compose.yaml):
 * <pre>
 *   RAFT_NODE_ID=2
 *   RAFT_PEERS=1=node1:7000,2=node2:7000,3=node3:7000   every member, this node included
 *   RAFT_PEER_PORT=7000        node-to-node gRPC
 *   RAFT_CLIENT_PORT=8080      HTTP client API and /metrics
 *   RAFT_DATA_DIR=/data        WAL segments and snapshots
 * optional:
 *   RAFT_TICK_MS=10  RAFT_ELECTION_TICKS=15  RAFT_HEARTBEAT_TICKS=5   (election timeout 150-300 ms)
 *   RAFT_PREVOTE=true  RAFT_CHECK_QUORUM=true  RAFT_SNAPSHOT_EVERY=10000  RAFT_REQUEST_TIMEOUT_MS=3000
 * </pre>
 */
public record ServerConfig(NodeId self, Map<NodeId, InetSocketAddress> members, int peerPort, int clientPort,
                           Path dataDir, int tickMs, int electionTicks, int heartbeatTicks, boolean preVote,
                           boolean checkQuorum, long snapshotEvery, long requestTimeoutMs) {

    public ServerConfig {
        Objects.requireNonNull(self);
        members = Map.copyOf(members);
        Objects.requireNonNull(dataDir);
        if (!members.containsKey(self)) throw new IllegalArgumentException(self + " is not in RAFT_PEERS " + members);
        if (tickMs < 1 || electionTicks < 1 || heartbeatTicks < 1 || requestTimeoutMs < 1 || snapshotEvery < 0)
            throw new IllegalArgumentException("timing settings must be positive");
    }

    public List<NodeId> memberIds() {
        return members.keySet().stream().sorted().toList();
    }

    /** The other members and where their peer servers listen. */
    public Map<NodeId, InetSocketAddress> peers() {
        Map<NodeId, InetSocketAddress> out = new LinkedHashMap<>();
        for (NodeId id : memberIds()) if (!id.equals(self)) out.put(id, members.get(id));
        return out;
    }

    public static ServerConfig fromEnv(Map<String, String> env) {
        NodeId self = new NodeId(Integer.parseInt(required(env, "RAFT_NODE_ID")));
        return new ServerConfig(self, parsePeers(required(env, "RAFT_PEERS")),
                Integer.parseInt(env.getOrDefault("RAFT_PEER_PORT", "7000")),
                Integer.parseInt(env.getOrDefault("RAFT_CLIENT_PORT", "8080")),
                Path.of(required(env, "RAFT_DATA_DIR")),
                Integer.parseInt(env.getOrDefault("RAFT_TICK_MS", "10")),
                Integer.parseInt(env.getOrDefault("RAFT_ELECTION_TICKS", "15")),
                Integer.parseInt(env.getOrDefault("RAFT_HEARTBEAT_TICKS", "5")),
                Boolean.parseBoolean(env.getOrDefault("RAFT_PREVOTE", "true")),
                Boolean.parseBoolean(env.getOrDefault("RAFT_CHECK_QUORUM", "true")),
                Long.parseLong(env.getOrDefault("RAFT_SNAPSHOT_EVERY", "10000")),
                Long.parseLong(env.getOrDefault("RAFT_REQUEST_TIMEOUT_MS", "3000")));
    }

    /** "1=node1:7000,2=node2:7000" → {n1 → node1:7000, n2 → node2:7000}. */
    static Map<NodeId, InetSocketAddress> parsePeers(String spec) {
        Map<NodeId, InetSocketAddress> out = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] idAndAddr = part.trim().split("=", 2);
            if (idAndAddr.length != 2) throw new IllegalArgumentException("bad RAFT_PEERS entry: " + part);
            int colon = idAndAddr[1].lastIndexOf(':');
            if (colon < 0) throw new IllegalArgumentException("missing port in RAFT_PEERS entry: " + part);
            NodeId id = new NodeId(Integer.parseInt(idAndAddr[0].trim()));
            InetSocketAddress addr = InetSocketAddress.createUnresolved(idAndAddr[1].substring(0, colon).trim(),
                    Integer.parseInt(idAndAddr[1].substring(colon + 1).trim()));
            if (out.put(id, addr) != null) throw new IllegalArgumentException("duplicate id in RAFT_PEERS: " + id);
        }
        return out;
    }

    private static String required(Map<String, String> env, String key) {
        String v = env.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException(key + " is not set");
        return v;
    }
}
