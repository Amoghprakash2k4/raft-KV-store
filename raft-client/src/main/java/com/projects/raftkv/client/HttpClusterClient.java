package com.projects.raftkv.client;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A blocking client for a real cluster (DESIGN.md M5-C): the simulator-tested {@link ClientSession} (same
 * seq on every retry, leader hints, timeouts) driven over HTTP. Each attempt is one
 * {@code POST /v1/write|read} with the KvCodec bytes; the HTTP status becomes a {@link ClientSession.Reply}.
 * Not thread-safe: one operation at a time, like the session (no pipelining, DEVIATIONS.md K5).
 */
public final class HttpClusterClient {
    private static final Pattern LEADER_ID = Pattern.compile("\"leaderId\"\\s*:\\s*(\\d+)");

    private final Map<NodeId, URI> nodes;
    private final ClientSession session;
    private final HttpClient http;
    private final long operationTimeoutMs;

    /**
     * @param nodes      every member's client URL, e.g. {@code n1 → http://localhost:8081}
     * @param clientId   1 .. 2^23-1, unique among live clients (random in practice, DEVIATIONS.md K2)
     * @param operationTimeoutMs give up on one operation after this long (its outcome is then unknown)
     */
    public HttpClusterClient(Map<NodeId, URI> nodes, long clientId, ClientSession.Config config, long operationTimeoutMs) {
        this.nodes = new LinkedHashMap<>(nodes);
        this.session = new ClientSession(clientId, this.nodes.keySet().stream().sorted().toList(), new SplittableRandom(clientId), config);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(config.timeoutMs())).build();
        this.operationTimeoutMs = operationTimeoutMs;
    }

    public long clientId() { return session.clientId(); }

    /**
     * Runs one command to completion: a write (exactly once, thanks to the session) or a linearizable read.
     * @throws TimeoutException if no attempt succeeded within the operation timeout. A write's outcome is
     *         then unknown: it may still take effect (once).
     */
    public KvResult execute(KvCommand command) throws TimeoutException, InterruptedException {
        Objects.requireNonNull(command);
        long deadline = System.nanoTime() + operationTimeoutMs * 1_000_000;
        ClientSession.Attempt a = command instanceof KvCommand.Get g ? session.read(g.key()) : session.write(command);
        while (true) {
            if (System.nanoTime() > deadline) {
                abandon(a);
                throw new TimeoutException("no answer for " + command + " within " + operationTimeoutMs + " ms");
            }
            if (a.delayMs() > 0) Thread.sleep(a.delayMs());
            Optional<ClientSession.Reply> reply = send(a);
            ClientSession.Decision d = reply.isPresent() ? session.onReply(a.attemptId(), reply.get())
                    : session.onTimeout(a.attemptId());
            switch (d) {
                case ClientSession.Decision.Completed c -> {
                    return KvCodec.decodeResult(c.result());
                }
                case ClientSession.Decision.Send s -> a = s.attempt();
                case ClientSession.Decision.Ignore i -> {
                    ClientSession.Decision t = session.onTimeout(a.attemptId());
                    if (t instanceof ClientSession.Decision.Send s) a = s.attempt();
                    else throw new IllegalStateException("session has nothing to send: " + t);
                }
            }
        }
    }

    /** One HTTP round trip; empty = no usable answer (timeout, connection refused, 504, 5xx...). */
    private Optional<ClientSession.Reply> send(ClientSession.Attempt a) throws InterruptedException {
        URI base = nodes.get(a.target());
        HttpRequest req = HttpRequest.newBuilder(base.resolve(a.read() ? "/v1/read" : "/v1/write"))
                .timeout(Duration.ofMillis(session.config().timeoutMs()))
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(a.payload().toByteArray()))
                .build();
        HttpResponse<byte[]> resp;
        try {
            resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.ofNullable(switch (resp.statusCode()) {
            case 200 -> new ClientSession.Reply.Ok(Bytes.copyOf(resp.body()));
            case 421 -> new ClientSession.Reply.NotLeader(leaderOf(resp.body()));
            case 503 -> new String(resp.body(), java.nio.charset.StandardCharsets.UTF_8).contains("\"retry\"")
                    ? new ClientSession.Reply.Retry() : null;
            default -> null;
        });
    }

    private Optional<NodeId> leaderOf(byte[] body) {
        Matcher m = LEADER_ID.matcher(new String(body, java.nio.charset.StandardCharsets.UTF_8));
        if (!m.find()) return Optional.empty();
        NodeId id = new NodeId(Integer.parseInt(m.group(1)));
        return nodes.containsKey(id) ? Optional.of(id) : Optional.empty();
    }

    /** Lets the session forget an operation we gave up on, so the next one can start. */
    private void abandon(ClientSession.Attempt a) {
        session.abandon(a.attemptId());
    }
}
