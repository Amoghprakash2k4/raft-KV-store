package com.projects.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * M6 finding: without TCP_NODELAY every request on a kept-alive connection stalled about 40 ms when the
 * peer was Linux (Nagle's algorithm + delayed ACK), so a Docker cluster served one client at 20 ops/s.
 * The latency only shows on Linux, so this pins the setting itself.
 */
class ClientApiNoDelayTest {

    @Test
    void loadingTheClientApiTurnsNagleOffForTheJdkHttpServer() throws ClassNotFoundException {
        Class.forName(ClientApi.class.getName(), true, ClientApi.class.getClassLoader());   // runs the static block
        assertThat(System.getProperty("sun.net.httpserver.nodelay")).isEqualTo("true");
    }
}
