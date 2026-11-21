package com.projects.raftkv.transport;

import com.projects.raftkv.core.Message;

/**
 * Outbound peer messaging. Fire-and-forget: delivery may be lost, delayed, duplicated or reordered,
 * and the core must be correct under all of these. The destination is {@link Message#to()}.
 */
@FunctionalInterface
public interface Transport {
    void send(Message message);
}
