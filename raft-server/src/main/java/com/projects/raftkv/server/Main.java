package com.projects.raftkv.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts one node from environment variables ({@link ServerConfig}). */
public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) throws InterruptedException {
        ServerConfig cfg = ServerConfig.fromEnv(System.getenv());
        // Fail-stop: after a failed fsync this process must not keep running on unknown durable state.
        RaftServer node = RaftServer.start(cfg, () -> Runtime.getRuntime().halt(1));
        ClientApi api = ClientApi.start(node, cfg.clientPort());
        log.info("{} up: peers on :{}, clients on :{}, data in {}", cfg.self(), node.peerPort(), api.port(), cfg.dataDir());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            api.close();
            node.close();
        }, "shutdown"));
        Thread.currentThread().join();
    }
}
