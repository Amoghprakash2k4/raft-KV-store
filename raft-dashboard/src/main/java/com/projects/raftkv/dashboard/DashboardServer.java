package com.projects.raftkv.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.raftkv.bench.FailoverRun;
import com.projects.raftkv.bench.LoadRun;
import com.projects.raftkv.bench.LoadSpec;
import com.projects.raftkv.bench.ResultStore;
import com.projects.raftkv.bench.RunRecord;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * The dashboard's HTTP side (DESIGN.md M6-C). Bound to 127.0.0.1 only: it starts processes on this machine.
 * <pre>
 *   GET  /                      the page (index.html, app.js, style.css, echarts.min.js)
 *   GET  /api/options           mixes, mutations, defaults
 *   POST /api/run/sim|blind|load|failover   {settings} → {"jobId"} (409 while another job runs)
 *   GET  /api/job               the current job's state and live progress
 *   POST /api/job/cancel
 *   GET  /api/runs              saved runs, newest first (summaries)
 *   GET  /api/runs/{id}         one saved run, in full
 *   DELETE /api/runs/{id}
 * </pre>
 */
public final class DashboardServer implements AutoCloseable {
    private static final ObjectMapper JSON = ResultStore.JSON;
    private static final Map<String, String> TYPES = Map.of("html", "text/html; charset=utf-8", "js",
            "application/javascript; charset=utf-8", "css", "text/css; charset=utf-8", "svg", "image/svg+xml");

    private final HttpServer http;
    private final Jobs jobs;

    private DashboardServer(int port, ResultStore store) {
        this.jobs = new Jobs(store);
        try {
            this.http = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open port " + port, e);
        }
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/", this::staticFile);
        http.createContext("/api/options", ex -> {
            try (ex) {
                json(ex, 200, options());
            }
        });
        http.createContext("/api/run/", this::run);
        http.createContext("/api/job", this::job);
        http.createContext("/api/runs", this::runs);
    }

    public static DashboardServer start(int port, ResultStore store) {
        DashboardServer s = new DashboardServer(port, store);
        s.http.start();
        return s;
    }

    public int port() { return http.getAddress().getPort(); }

    // ---------------------------------------------------------------- API

    private Map<String, Object> options() {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("mixes", SimSettings.MIXES);
        o.put("mutations", SimSettings.mutations());
        o.put("checkers", SimJob.CHECKERS);
        o.put("cpus", Runtime.getRuntime().availableProcessors());
        o.put("loadDefaults", LoadSpec.defaults());
        List<Map<String, Object>> bugs = new ArrayList<>();
        for (BlindTest.Bug b : BlindTest.BUGS) bugs.add(Map.of("name", b.name(), "what", b.what(), "profile", b.profile().label));
        o.put("bugs", bugs);
        return o;
    }

    private void run(HttpExchange ex) throws IOException {
        try (ex) {
            if (!ex.getRequestMethod().equals("POST")) {
                json(ex, 405, Map.of("error", "POST"));
                return;
            }
            String kind = ex.getRequestURI().getPath().substring("/api/run/".length());
            JsonNode body = JSON.readTree(ex.getRequestBody());
            Optional<String> id;
            try {
                id = switch (kind) {
                    case "sim" -> startSim(JSON.treeToValue(body, SimSettings.class));
                    case "load" -> startLoad(JSON.treeToValue(body, LoadSpec.class));
                    case "failover" -> startFailover(JSON.treeToValue(body, FailoverRun.Spec.class));
                    case "blind" -> startBlind(JSON.treeToValue(body, BlindTest.Spec.class));
                    default -> throw new IllegalArgumentException("unknown run kind " + kind);
                };
            } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
                Throwable root = e;
                while (root.getCause() != null) root = root.getCause();
                json(ex, 400, Map.of("error", String.valueOf(root.getMessage())));
                return;
            }
            if (id.isEmpty()) json(ex, 409, Map.of("error", "another run is in progress"));
            else json(ex, 200, Map.of("jobId", id.get()));
        }
    }

    private Optional<String> startSim(SimSettings s) {
        return jobs.start("sim", s.label(), (p, cancel) -> {
            SimJob.Result r = SimJob.run(s, cancel, pr -> {
                p.put("done", pr.done());
                p.put("of", pr.of());
                p.put("passed", pr.passed());
                p.put("failed", pr.failed());
            });
            return jobs.store().save("sim", s.label(), s, r);
        });
    }

    private Optional<String> startBlind(BlindTest.Spec spec) {
        return jobs.start("blind", spec.label(), (p, cancel) -> {
            BlindTest.Result r = BlindTest.run(spec, cancel, pr -> {
                p.put("done", pr.done());
                p.put("of", pr.of());
                p.put("flagged", pr.failed());
            });
            return jobs.store().save("blind", spec.label(), spec, r);
        });
    }

    private Optional<String> startLoad(LoadSpec spec) {
        return jobs.start("load", spec.label(), (p, cancel) -> {
            List<Map<String, Object>> live = java.util.Collections.synchronizedList(new ArrayList<>());
            p.put("live", live);
            LoadRun.Result r = LoadRun.run(spec, Files.createTempDirectory("raft-dashboard"), pr -> {
                p.put("phase", pr.phase());
                p.put("second", pr.second());
                p.put("of", pr.ofSeconds());
                if (pr.phase().equals("measuring") && pr.opsPerSec() > 0) {
                    live.add(Map.of("second", pr.second(), "ops", pr.opsPerSec(), "p99", pr.p99Ms()));
                }
            });
            return jobs.store().save("load", spec.label(), spec, r);
        });
    }

    private Optional<String> startFailover(FailoverRun.Spec spec) {
        return jobs.start("failover", spec.label(), (p, cancel) -> {
            List<FailoverRun.Kill> live = java.util.Collections.synchronizedList(new ArrayList<>());
            p.put("live", live);
            FailoverRun.Result r = FailoverRun.run(spec, Files.createTempDirectory("raft-dashboard"), pr -> {
                p.put("done", pr.done());
                p.put("of", pr.of());
                pr.last().ifPresent(live::add);
            });
            return jobs.store().save("failover", spec.label(), spec, r);
        });
    }

    private void job(HttpExchange ex) throws IOException {
        try (ex) {
            if (ex.getRequestURI().getPath().equals("/api/job/cancel") && ex.getRequestMethod().equals("POST")) {
                jobs.cancel();
                json(ex, 200, Map.of("ok", true));
                return;
            }
            Optional<Jobs.Status> s = jobs.status();
            if (s.isPresent()) json(ex, 200, s.get());
            else json(ex, 200, Map.of("state", "idle"));
        }
    }

    private void runs(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/api/runs") || path.equals("/api/runs/")) {
                List<Map<String, Object>> out = new ArrayList<>();
                for (RunRecord r : jobs.store().list()) out.add(summary(r));
                json(ex, 200, out);
                return;
            }
            String id = path.substring("/api/runs/".length());
            if (ex.getRequestMethod().equals("DELETE")) {
                json(ex, jobs.store().delete(id) ? 200 : 404, Map.of("deleted", id));
                return;
            }
            Optional<RunRecord> r = jobs.store().get(id);
            if (r.isPresent()) json(ex, 200, r.get());
            else json(ex, 404, Map.of("error", "no run " + id));
        }
    }

    /** The few numbers the run list and the compare view need, without the per-seed or per-second data. */
    static Map<String, Object> summary(RunRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id());
        m.put("kind", r.kind());
        m.put("label", r.label());
        m.put("createdAt", r.createdAt());
        m.put("gitCommit", r.gitCommit());
        m.put("settings", r.settings());
        JsonNode res = r.result();
        Map<String, Object> h = new LinkedHashMap<>();
        switch (r.kind()) {
            case "sim" -> {
                h.put("seeds", res.path("seeds").asInt());
                h.put("passed", res.path("passed").asInt());
                h.put("outcomes", res.path("outcomes"));
                h.put("wallMs", res.path("wallMs").asLong());
            }
            case "load" -> {
                h.put("opsPerSec", res.path("opsPerSec").asDouble());
                h.put("writeP50", res.path("writes").path("p50Ms").asDouble());
                h.put("writeP99", res.path("writes").path("p99Ms").asDouble());
                h.put("readP50", res.path("reads").path("p50Ms").asDouble());
                h.put("readP99", res.path("reads").path("p99Ms").asDouble());
                h.put("fsyncMeanMs", res.path("server").path("fsyncMeanMs").asDouble());
                h.put("batchMean", res.path("server").path("batchMean").asDouble());
            }
            case "blind" -> {
                h.put("seeds", res.path("seeds").asInt());
                h.put("planted", res.path("planted").asInt());
                h.put("caught", res.path("caught").asInt());
                h.put("missed", res.path("missed").asInt());
                h.put("falseAlarms", res.path("falseAlarms").asInt());
            }
            case "failover" -> {
                h.put("kills", res.path("kills").size());
                h.put("electionP50", res.path("election").path("p50Ms").asDouble());
                h.put("electionP99", res.path("election").path("p99Ms").asDouble());
                h.put("gapP50", res.path("clientGap").path("p50Ms").asDouble());
                h.put("gapP99", res.path("clientGap").path("p99Ms").asDouble());
            }
            default -> { }
        }
        m.put("headline", h);
        return m;
    }

    // ---------------------------------------------------------------- static files

    private void staticFile(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/")) path = "/index.html";
            if (!path.matches("/[a-zA-Z0-9._-]+")) {
                json(ex, 404, Map.of("error", "not found"));
                return;
            }
            try (InputStream in = DashboardServer.class.getResourceAsStream("/web" + path)) {
                if (in == null) {
                    json(ex, 404, Map.of("error", "not found"));
                    return;
                }
                byte[] body = in.readAllBytes();
                String ext = path.substring(path.lastIndexOf('.') + 1);
                ex.getResponseHeaders().set("Content-Type", TYPES.getOrDefault(ext, "application/octet-stream"));
                ex.getResponseHeaders().set("Cache-Control", "no-cache");
                ex.sendResponseHeaders(200, body.length);
                ex.getResponseBody().write(body);
            }
        }
    }

    private static void json(HttpExchange ex, int status, Object body) throws IOException {
        byte[] out = JSON.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, out.length);
        ex.getResponseBody().write(out);
    }

    @Override
    public void close() {
        http.stop(0);
    }
}
