package com.projects.raftkv.statemachine;

import java.util.Objects;

/**
 * The bytes a client proposes: a command plus the client session fields (clientId, seq). They are
 * in the log format from M2 on but only used from M4, for exactly-once dedup at apply time (thesis
 * §6.3), so adding dedup will not change the log format (DESIGN.md M2-E).
 */
public record KvRequest(long clientId, long seq, KvCommand command) {
    public KvRequest { Objects.requireNonNull(command); }
}
