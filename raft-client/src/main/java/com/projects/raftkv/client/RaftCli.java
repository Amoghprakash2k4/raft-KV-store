package com.projects.raftkv.client;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

/**
 * Command-line client (DESIGN.md M5-C):
 * <pre>
 *   raft-cli [--cluster 1=http://localhost:8081,2=...] put KEY VALUE
 *   raft-cli get KEY | del KEY | cas KEY EXPECTED|- NEW | status
 * </pre>
 * The cluster defaults to $RAFT_CLUSTER, then to the compose.yaml ports (8081-8083). Each invocation is one
 * client with a random id (DEVIATIONS.md K2), so a retry inside one command is exactly-once.
 */
public final class RaftCli {
    private RaftCli() {}

    public static void main(String[] args) throws Exception {
        System.exit(run(args, System.out, System.err));
    }

    /** Runs one command; returns the exit status. */
    public static int run(String[] args, PrintStream out, PrintStream err) throws InterruptedException {
        String cluster = Optional.ofNullable(System.getenv("RAFT_CLUSTER"))
                .orElse("1=http://localhost:8081,2=http://localhost:8082,3=http://localhost:8083");
        int i = 0;
        if (args.length >= 2 && args[0].equals("--cluster")) {
            cluster = args[1];
            i = 2;
        }
        if (args.length <= i) return usage(err);
        Map<NodeId, URI> nodes = parseCluster(cluster);
        String cmd = args[i];
        String[] rest = java.util.Arrays.copyOfRange(args, i + 1, args.length);
        if (cmd.equals("status")) return status(nodes, out);
        KvCommand command = switch (cmd) {
            case "put" -> rest.length == 2 ? new KvCommand.Put(rest[0], rest[1]) : null;
            case "get" -> rest.length == 1 ? new KvCommand.Get(rest[0]) : null;
            case "del" -> rest.length == 1 ? new KvCommand.Delete(rest[0]) : null;
            case "cas" -> rest.length == 3
                    ? new KvCommand.Cas(rest[0], rest[1].equals("-") ? Optional.empty() : Optional.of(rest[1]), rest[2]) : null;
            default -> null;
        };
        if (command == null) return usage(err);
        long clientId = ThreadLocalRandom.current().nextLong(1, 1L << 23);
        HttpClusterClient client = new HttpClusterClient(nodes, clientId, new ClientSession.Config(1_000, 100), 10_000);
        try {
            out.println(format(client.execute(command)));
            return 0;
        } catch (TimeoutException e) {
            err.println("no answer: " + e.getMessage() + " (a write may still take effect)");
            return 2;
        }
    }

    static String format(KvResult r) {
        return switch (r) {
            case KvResult.Value v -> v.value().orElse("(none)");
            case KvResult.CasOutcome c -> (c.swapped() ? "swapped" : "not swapped") + ", previous: " + c.previous().orElse("(none)");
            case KvResult.Stale s -> "stale request (already past seq " + s.lastSeq() + ")";
            case KvResult.Invalid inv -> "invalid: " + inv.reason();
        };
    }

    private static int status(Map<NodeId, URI> nodes, PrintStream out) throws InterruptedException {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
        for (Map.Entry<NodeId, URI> e : nodes.entrySet()) {
            String line;
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(e.getValue().resolve("/status"))
                        .timeout(Duration.ofSeconds(1)).build(), HttpResponse.BodyHandlers.ofString());
                line = r.body();
            } catch (java.io.IOException ex) {
                line = "unreachable (" + ex.getClass().getSimpleName() + ")";
            }
            out.println(e.getKey() + " " + e.getValue() + " " + line);
        }
        return 0;
    }

    static Map<NodeId, URI> parseCluster(String spec) {
        Map<NodeId, URI> out = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2) throw new IllegalArgumentException("bad cluster entry: " + part);
            out.put(new NodeId(Integer.parseInt(kv[0].trim())), URI.create(kv[1].trim()));
        }
        return out;
    }

    private static int usage(PrintStream err) {
        err.println("usage: raft-cli [--cluster 1=http://host:port,...] put K V | get K | del K | cas K EXPECTED|- NEW | status");
        return 1;
    }
}
