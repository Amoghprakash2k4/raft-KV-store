package com.projects.raftkv.server;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import java.util.Objects;
import java.util.Optional;

/** How the node answers one client request (write or read), before it becomes an HTTP response. */
public sealed interface ClientReply {
    /** Committed and applied (a write) or served by ReadIndex (a read); {@code result} is a KvCodec result. */
    record Ok(Bytes result) implements ClientReply {
        public Ok { Objects.requireNonNull(result); }
    }

    /** Not the leader (thesis §6.2); {@code leader} if this node knows it. */
    record NotLeader(Optional<NodeId> leader) implements ClientReply {
        public NotLeader { Objects.requireNonNull(leader); }
    }

    /** It may or may not have been executed (overwritten entry, snapshot): resend the same request (§6.3). */
    record Retry(String why) implements ClientReply {}

    /** The node cannot take requests (mailbox full, halted, shutting down). Try another node. */
    record Unavailable(String why) implements ClientReply {}
}
