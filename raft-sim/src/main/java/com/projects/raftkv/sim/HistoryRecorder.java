package com.projects.raftkv.sim;

import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What the clients saw (invariant 7's input): one record per client operation (not per attempt), with
 * the virtual time the client invoked it and the time it received the answer. An operation still
 * unanswered when the run ends is recorded with {@link #UNKNOWN_RETURN} and no output: it may have
 * taken effect (at any time after its invocation) or not at all.
 */
final class HistoryRecorder {

    /** The return time of an operation that never got an answer. */
    static final long UNKNOWN_RETURN = Long.MAX_VALUE;

    record Operation(int id, String client, KvCommand command, long invokeMs, long returnMs, Optional<KvResult> output) {
        Operation {
            Objects.requireNonNull(command);
            Objects.requireNonNull(output);
            if (returnMs < invokeMs) throw new IllegalArgumentException("returns before it was invoked");
            if (output.isEmpty() != (returnMs == UNKNOWN_RETURN))
                throw new IllegalArgumentException("an output iff a return time");
        }

        String key() { return command.key(); }

        boolean unknown() { return output.isEmpty(); }

        @Override public String toString() {
            return client + " " + command + " [" + invokeMs + ", " + (unknown() ? "?" : returnMs) + "] -> "
                    + output.map(Object::toString).orElse("?");
        }
    }

    private record Open(String client, KvCommand command, long invokeMs) {}

    private final List<Operation> done = new ArrayList<>();
    private final Map<String, Open> open = new HashMap<>();   // by client: one operation in flight each

    void invoke(String client, KvCommand command, long now) {
        if (open.putIfAbsent(client, new Open(client, command, now)) != null)
            throw new IllegalStateException(client + " already has an operation in flight");
    }

    /** Records the answer the client accepted; returns the operation's command. */
    KvCommand complete(String client, KvResult output, long now) {
        Open o = open.remove(client);
        if (o == null) throw new IllegalStateException(client + " has no operation in flight");
        done.add(new Operation(0, client, o.command(), o.invokeMs(), now, Optional.of(output)));
        return o.command();
    }

    /** Completed operations plus the ones still open, as unknown. Ids are assigned by the checker per key. */
    List<Operation> history() {
        List<Operation> all = new ArrayList<>(done);
        open.values().stream()
                .sorted(java.util.Comparator.comparing(Open::client))
                .forEach(o -> all.add(new Operation(0, o.client(), o.command(), o.invokeMs(), UNKNOWN_RETURN, Optional.empty())));
        return all;
    }

    int completed() { return done.size(); }

    int unknown() { return open.size(); }
}
