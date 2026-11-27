package com.projects.raftkv.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import com.projects.raftkv.statemachine.KvResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The client API over HTTP (DESIGN.md M5-C), on the JDK's built-in server, one virtual thread per request.
 *
 * <p>For people ({@code curl}), JSON:
 * <pre>
 *   PUT    /kv/{key}       {"value":"v"}                    → 200 {"previous":"old"|null}
 *   GET    /kv/{key}                                         → 200 {"value":"v"|null}   (linearizable, ReadIndex)
 *   DELETE /kv/{key}                                         → 200 {"previous":...}
 *   POST   /kv/{key}/cas   {"expected":"a"|null,"value":"b"} → 200 {"swapped":true,"previous":"a"}
 *   GET    /status, GET /metrics (Prometheus)
 * </pre>
 * Writes may carry {@code Client-Id} and {@code Seq} headers; a retry with the same pair is answered from the
 * session table instead of running twice (thesis §6.3). Without them there is no session (client 0).
 *
 * <p>For programs ({@code HttpClusterClient}, built on the simulator-tested {@code ClientSession}): the
 * KvCodec request bytes, as {@code POST /v1/write} and {@code POST /v1/read}, answered with the KvCodec
 * result bytes.
 *
 * <p>Errors: 421 + {"leaderId":n|null} not the leader (try that node); 503 + {"error":"retry"} outcome
 * unknown, resend the same request; 503 + {"error":"unavailable"} try another node; 504 no answer in time
 * (outcome unknown); 409 stale seq; 400 bad request.
 */
public final class ClientApi implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * TCP_NODELAY on client connections. The JDK server writes a response's headers and body separately.
     * With Nagle's algorithm the body waits for the headers' ACK, and a Linux peer delays that ACK by up to
     * 40 ms, so every request on a kept-alive connection stalled about 40 ms. Measured through Docker in M6:
     * 42 ms → 1.1–1.8 ms per request, and 20 → several hundred ops/s for one client (BENCHMARKS.md).
     * macOS loopback never showed it. The JDK reads this property once, when its HTTP server classes load,
     * so it is set before the first {@link HttpServer#create}. An explicit -D on the command line wins.
     */
    static final String NODELAY = "sun.net.httpserver.nodelay";

    static {
        if (System.getProperty(NODELAY) == null) System.setProperty(NODELAY, "true");
    }

    private final RaftServer node;
    private final HttpServer http;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private ClientApi(RaftServer node, int port) {
        this.node = node;
        try {
            this.http = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open the client port " + port, e);
        }
        http.setExecutor(executor);
        http.createContext("/kv/", this::kv);
        http.createContext("/v1/write", ex -> binary(ex, true));
        http.createContext("/v1/read", ex -> binary(ex, false));
        http.createContext("/status", this::status);
        http.createContext("/metrics", this::metrics);
    }

    public static ClientApi start(RaftServer node, int port) {
        ClientApi api = new ClientApi(node, port);
        api.http.start();
        return api;
    }

    public int port() { return http.getAddress().getPort(); }

    // ---------------------------------------------------------------- JSON for people

    private void kv(HttpExchange ex) throws IOException {
        try (ex) {
            String rest = ex.getRequestURI().getRawPath().substring("/kv/".length());
            boolean cas = rest.endsWith("/cas");
            String key = URLDecoder.decode(cas ? rest.substring(0, rest.length() - 4) : rest, StandardCharsets.UTF_8);
            if (key.isEmpty() || key.contains("/")) {
                json(ex, 400, Map.of("error", "bad key"));
                return;
            }
            String method = ex.getRequestMethod();
            if (cas && method.equals("POST")) {
                JsonNode body = JSON.readTree(ex.getRequestBody());
                if (body == null || !body.hasNonNull("value")) {
                    json(ex, 400, Map.of("error", "body must be {\"expected\": ...|null, \"value\": ...}"));
                    return;
                }
                Optional<String> expected = body.hasNonNull("expected") ? Optional.of(body.get("expected").asText()) : Optional.empty();
                writeJson(ex, new KvCommand.Cas(key, expected, body.get("value").asText()));
            } else if (!cas && method.equals("PUT")) {
                JsonNode body = JSON.readTree(ex.getRequestBody());
                if (body == null || !body.hasNonNull("value")) {
                    json(ex, 400, Map.of("error", "body must be {\"value\": \"...\"}"));
                    return;
                }
                writeJson(ex, new KvCommand.Put(key, body.get("value").asText()));
            } else if (!cas && method.equals("DELETE")) {
                writeJson(ex, new KvCommand.Delete(key));
            } else if (!cas && method.equals("GET")) {
                Bytes query = KvCodec.encode(new KvRequest(0, 0, new KvCommand.Get(key)));
                answerJson(ex, await(node.read(query)));
            } else {
                json(ex, 405, Map.of("error", "method not allowed"));
            }
        } catch (IOException | RuntimeException e) {
            json(ex, 400, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    private void writeJson(HttpExchange ex, KvCommand command) throws IOException {
        long clientId = header(ex, "Client-Id");
        long seq = header(ex, "Seq");
        if ((clientId == 0) != (seq == 0)) {
            json(ex, 400, Map.of("error", "send both Client-Id and Seq, or neither"));
            return;
        }
        answerJson(ex, await(node.write(KvCodec.encode(new KvRequest(clientId, seq, command)))));
    }

    private void answerJson(HttpExchange ex, ClientReply reply) throws IOException {
        if (!(reply instanceof ClientReply.Ok ok)) {
            error(ex, reply);
            return;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        int status = 200;
        switch (KvCodec.decodeResult(ok.result())) {
            case KvResult.Value v -> body.put(ex.getRequestMethod().equals("GET") ? "value" : "previous", v.value().orElse(null));
            case KvResult.CasOutcome c -> {
                body.put("swapped", c.swapped());
                body.put("previous", c.previous().orElse(null));
            }
            case KvResult.Stale s -> {
                status = 409;
                body.put("error", "stale request: this client's session already executed seq " + s.lastSeq());
            }
            case KvResult.Invalid i -> {
                status = 400;
                body.put("error", i.reason());
            }
        }
        json(ex, status, body);
    }

    // ---------------------------------------------------------------- KvCodec bytes for programs

    private void binary(HttpExchange ex, boolean write) throws IOException {
        try (ex) {
            if (!ex.getRequestMethod().equals("POST")) {
                json(ex, 405, Map.of("error", "POST only"));
                return;
            }
            Bytes body = Bytes.copyOf(ex.getRequestBody().readAllBytes());
            ClientReply reply = await(write ? node.write(body) : node.read(body));
            if (reply instanceof ClientReply.Ok ok) {
                byte[] out = ok.result().toByteArray();
                ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                ex.sendResponseHeaders(200, out.length);
                ex.getResponseBody().write(out);
            } else {
                error(ex, reply);
            }
        }
    }

    // ---------------------------------------------------------------- status and metrics

    private void status(HttpExchange ex) throws IOException {
        try (ex) {
            RaftServer.View v = node.view();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("id", node.config().self().value());
            body.put("role", v.role().name());
            body.put("term", v.status().hard().currentTerm());
            body.put("leaderId", v.status().soft().leader().map(NodeId::value).orElse(null));
            body.put("votedFor", v.status().hard().votedFor().map(NodeId::value).orElse(null));
            body.put("commitIndex", v.status().commitIndex());
            body.put("lastApplied", v.status().lastApplied());
            body.put("lastLogIndex", v.lastLogIndex());
            body.put("halted", v.halted());
            json(ex, 200, body);
        }
    }

    private void metrics(HttpExchange ex) throws IOException {
        try (ex) {
            byte[] out = node.metrics().scrape().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
        }
    }

    // ---------------------------------------------------------------- helpers

    private ClientReply await(CompletableFuture<ClientReply> f) {
        try {
            return f.get(node.config().requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;   // 504: the outcome is unknown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ClientReply.Unavailable("interrupted");
        } catch (ExecutionException e) {
            return new ClientReply.Unavailable(String.valueOf(e.getCause()));
        }
    }

    private static void error(HttpExchange ex, ClientReply reply) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        int status;
        switch (reply) {
            case null -> {
                status = 504;
                body.put("error", "timeout");
            }
            case ClientReply.NotLeader nl -> {
                status = 421;
                body.put("error", "not_leader");
                body.put("leaderId", nl.leader().map(NodeId::value).orElse(null));
            }
            case ClientReply.Retry r -> {
                status = 503;
                body.put("error", "retry");
                body.put("why", r.why());
            }
            case ClientReply.Unavailable u -> {
                status = 503;
                body.put("error", "unavailable");
                body.put("why", u.why());
            }
            case ClientReply.Ok ok -> throw new IllegalArgumentException("not an error");
        }
        json(ex, status, body);
    }

    private static long header(HttpExchange ex, String name) {
        String v = ex.getRequestHeaders().getFirst(name);
        if (v == null) return 0;
        long n = Long.parseLong(v.trim());
        if (n < 1) throw new IllegalArgumentException(name + " must be positive");
        return n;
    }

    private static void json(HttpExchange ex, int status, Map<String, ?> body) throws IOException {
        byte[] out = JSON.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, out.length);
        ex.getResponseBody().write(out);
    }

    @Override
    public void close() {
        http.stop(0);
        executor.close();
    }
}
