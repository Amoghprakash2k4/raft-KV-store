package com.projects.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.client.HttpClusterClient;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.Role;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import com.projects.raftkv.client.RaftCli;
import java.io.PrintStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three real nodes in this JVM (gRPC on localhost, WAL on real files): the client API, sessions over HTTP,
 * metrics, leader failover and recovery from disk. Process kills are in {@code KillNineIntegrationTest}.
 */
@Timeout(60)
class RaftServerTest {

    @TempDir Path dir;
    private LocalCluster cluster;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() {
        cluster = new LocalCluster(3, dir);
    }

    @AfterEach
    void stop() {
        cluster.close();
    }

    private HttpResponse<String> call(NodeId node, String method, String path, String body, String... headers)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(cluster.url(node).resolve(path)).timeout(Duration.ofSeconds(5))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (headers.length > 0) b.headers(headers);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private NodeId follower(NodeId leader) {
        return cluster.ids.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
    }

    @Test
    void electsALeaderAndTheClientWritesAndReadsThroughAnyNode() throws Exception {
        cluster.awaitLeader(Duration.ofSeconds(10));
        HttpClusterClient c = cluster.client(11);
        assertThat(c.execute(new KvCommand.Put("x", "1"))).isEqualTo(new KvResult.Value(Optional.empty()));
        assertThat(c.execute(new KvCommand.Put("x", "2"))).isEqualTo(new KvResult.Value(Optional.of("1")));
        assertThat(c.execute(new KvCommand.Cas("x", Optional.of("2"), "3")))
                .isEqualTo(new KvResult.CasOutcome(true, Optional.of("2")));
        assertThat(c.execute(new KvCommand.Get("x"))).isEqualTo(new KvResult.Value(Optional.of("3")));
        assertThat(c.execute(new KvCommand.Delete("x"))).isEqualTo(new KvResult.Value(Optional.of("3")));
        assertThat(c.execute(new KvCommand.Get("x"))).isEqualTo(new KvResult.Value(Optional.empty()));
    }

    @Test
    void jsonApiAndAFollowerRedirectsWithTheLeaderId() throws Exception {
        NodeId leader = cluster.awaitLeader(Duration.ofSeconds(10));
        HttpResponse<String> put = call(leader, "PUT", "/kv/greeting", "{\"value\":\"hello\"}");
        assertThat(put.statusCode()).isEqualTo(200);
        assertThat(put.body()).isEqualTo("{\"previous\":null}");

        HttpResponse<String> get = call(leader, "GET", "/kv/greeting", null);
        assertThat(get.body()).isEqualTo("{\"value\":\"hello\"}");

        HttpResponse<String> cas = call(leader, "POST", "/kv/greeting/cas", "{\"expected\":\"nope\",\"value\":\"x\"}");
        assertThat(cas.body()).isEqualTo("{\"swapped\":false,\"previous\":\"hello\"}");

        HttpResponse<String> redirect = call(follower(leader), "GET", "/kv/greeting", null);
        assertThat(redirect.statusCode()).isEqualTo(421);
        assertThat(redirect.body()).contains("\"leaderId\":" + leader.value());

        assertThat(call(leader, "PATCH", "/kv/greeting", null).statusCode()).isEqualTo(405);
        assertThat(call(leader, "PUT", "/kv/greeting", "{}").statusCode()).isEqualTo(400);
    }

    /** Thesis §6.3 over HTTP: the same (Client-Id, Seq) runs once; a retry gets the saved answer. */
    @Test
    void aRetryWithTheSameClientIdAndSeqIsExecutedOnce() throws Exception {
        NodeId leader = cluster.awaitLeader(Duration.ofSeconds(10));
        assertThat(call(leader, "PUT", "/kv/k", "{\"value\":\"a\"}", "Client-Id", "77", "Seq", "1").body())
                .isEqualTo("{\"previous\":null}");
        call(leader, "PUT", "/kv/k", "{\"value\":\"b\"}");   // someone else writes in between

        HttpResponse<String> retry = call(leader, "PUT", "/kv/k", "{\"value\":\"a\"}", "Client-Id", "77", "Seq", "1");
        assertThat(retry.body()).isEqualTo("{\"previous\":null}");                  // the saved reply
        assertThat(call(leader, "GET", "/kv/k", null).body()).isEqualTo("{\"value\":\"b\"}");   // not undone

        call(leader, "PUT", "/kv/k", "{\"value\":\"c\"}", "Client-Id", "77", "Seq", "2");
        HttpResponse<String> old = call(leader, "PUT", "/kv/k", "{\"value\":\"a\"}", "Client-Id", "77", "Seq", "1");
        assertThat(old.statusCode()).isEqualTo(409);
        assertThat(call(leader, "PUT", "/kv/k", "{\"value\":\"a\"}", "Client-Id", "77").statusCode()).isEqualTo(400);
    }

    @Test
    void statusAndPrometheusMetrics() throws Exception {
        NodeId leader = cluster.awaitLeader(Duration.ofSeconds(10));
        cluster.client(12).execute(new KvCommand.Put("m", "1"));

        String status = call(leader, "GET", "/status", null).body();
        assertThat(status).contains("\"role\":\"LEADER\"", "\"leaderId\":" + leader.value());

        String metrics = call(leader, "GET", "/metrics", null).body();
        assertThat(metrics).contains("raft_term{", "raft_is_leader{node=\"" + leader + "\"} 1.0",
                "raft_commit_index{", "raft_replication_lag{", "raft_fsync_seconds_count{",
                "raft_transport_sent_total{", "raft_elections_total{");
        assertThat(cluster.servers.get(leader).metrics().fsyncCount()).isPositive();
    }

    @Test
    void whenTheLeaderStopsANewOneTakesOverWithEveryAcknowledgedWrite() throws Exception {
        NodeId first = cluster.awaitLeader(Duration.ofSeconds(10));
        HttpClusterClient c = cluster.client(13);
        for (int i = 0; i < 20; i++) c.execute(new KvCommand.Put("k" + i, "v" + i));

        cluster.stop(first);
        NodeId second = cluster.awaitLeader(Duration.ofSeconds(10));
        assertThat(second).isNotEqualTo(first);
        assertThat(cluster.servers.get(second).view().status().hard().currentTerm())
                .isGreaterThan(0);
        for (int i = 0; i < 20; i++) {
            assertThat(c.execute(new KvCommand.Get("k" + i))).isEqualTo(new KvResult.Value(Optional.of("v" + i)));
        }

        // the old leader restarts from its files and catches up (snapshotEvery=50 → also via snapshots)
        for (int i = 20; i < 120; i++) c.execute(new KvCommand.Put("k" + i, "v" + i));
        cluster.start(first);
        long target = cluster.servers.get(second).view().status().commitIndex();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (cluster.servers.get(first).view().status().lastApplied() < target && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(cluster.servers.get(first).view().status().lastApplied()).isGreaterThanOrEqualTo(target);
        assertThat(cluster.servers.get(first).view().role()).isNotEqualTo(Role.LEADER);
        assertThat(cluster.halts.get()).isZero();
    }

    @Test
    void theCliTalksToTheCluster() throws Exception {
        cluster.awaitLeader(Duration.ofSeconds(10));
        String spec = "1=" + cluster.url(new NodeId(1)) + ",2=" + cluster.url(new NodeId(2)) + ",3=" + cluster.url(new NodeId(3));
        var out = new java.io.ByteArrayOutputStream();
        var err = new java.io.ByteArrayOutputStream();
        assertThat(RaftCli.run(new String[] {"--cluster", spec, "put", "a", "1"}, new PrintStream(out), new PrintStream(err))).isZero();
        assertThat(RaftCli.run(new String[] {"--cluster", spec, "get", "a"}, new PrintStream(out), new PrintStream(err))).isZero();
        assertThat(out.toString()).isEqualTo("(none)\n1\n");
    }
}
