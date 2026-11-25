package com.projects.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.NodeId;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

    private static final Map<String, String> ENV = Map.of(
            "RAFT_NODE_ID", "2",
            "RAFT_PEERS", "1=node1:7000, 2=node2:7000,3=node3:7001",
            "RAFT_DATA_DIR", "/data");

    @Test
    void readsTheComposeContractWithDefaults() {
        ServerConfig c = ServerConfig.fromEnv(ENV);
        assertThat(c.self()).isEqualTo(new NodeId(2));
        assertThat(c.memberIds()).containsExactly(new NodeId(1), new NodeId(2), new NodeId(3));
        assertThat(c.peers()).containsOnlyKeys(new NodeId(1), new NodeId(3));
        assertThat(c.peers().get(new NodeId(3)).getHostString()).isEqualTo("node3");
        assertThat(c.peers().get(new NodeId(3)).getPort()).isEqualTo(7001);
        assertThat(c.dataDir()).isEqualTo(Path.of("/data"));
        assertThat(c.peerPort()).isEqualTo(7000);
        assertThat(c.clientPort()).isEqualTo(8080);
        assertThat(c.preVote()).isTrue();
        assertThat(c.checkQuorum()).isTrue();
        assertThat(c.tickMs() * c.electionTicks()).isEqualTo(150);
        assertThat(c.fsyncBatching()).isTrue();
        assertThat(c.maxBatch()).isEqualTo(256);
    }

    @Test
    void selfMustBeAMember() {
        Map<String, String> env = new java.util.HashMap<>(ENV);
        env.put("RAFT_NODE_ID", "9");
        assertThatThrownBy(() -> ServerConfig.fromEnv(env)).hasMessageContaining("not in RAFT_PEERS");
    }

    @Test
    void rejectsMissingAndMalformedSettings() {
        assertThatThrownBy(() -> ServerConfig.fromEnv(Map.of("RAFT_PEERS", "1=a:1", "RAFT_DATA_DIR", "/d")))
                .hasMessageContaining("RAFT_NODE_ID");
        assertThatThrownBy(() -> ServerConfig.parsePeers("1=a:1,1=b:2")).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> ServerConfig.parsePeers("1=nohost")).hasMessageContaining("missing port");
        assertThatThrownBy(() -> ServerConfig.parsePeers("garbage")).hasMessageContaining("bad RAFT_PEERS");
    }
}
