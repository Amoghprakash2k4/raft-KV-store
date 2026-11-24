package com.projects.raftkv.sim;

import com.projects.raftkv.client.ClientSession;
import com.projects.raftkv.client.ClientSession.Attempt;
import com.projects.raftkv.client.ClientSession.Decision;
import com.projects.raftkv.client.ClientSession.Reply;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.function.LongSupplier;

/**
 * Closed-loop simulated clients (DESIGN.md M4-A). Each client runs the real {@link ClientSession}: one
 * operation at a time, retries with the same seq, leader hints, timeouts. Every operation is recorded
 * in the {@link HistoryRecorder} from invocation to the answer the client finally accepts, which is
 * what the linearizability checker (invariant 7) judges.
 */
final class ClientWorkload {

    interface Scheduler { void at(long timeMs, Runnable action); }

    /** Puts one attempt on the (lossy) client link to its target node. */
    interface Sender { void send(String client, Attempt attempt); }

    private static final class Client {
        final ClientSession session;
        final String name;
        long lastCompletedAt = -1;

        Client(ClientSession session) {
            this.session = session;
            this.name = "c" + session.clientId();
        }
    }

    private final WorkloadConfig cfg;
    private final SplittableRandom rng;
    private final Scheduler scheduler;
    private final Sender sender;
    private final LongSupplier clock;
    private final HistoryRecorder history;
    private final List<Client> clients = new ArrayList<>();

    long opsStarted, writesCompleted, readsCompleted, attempts, timeouts, redirects, retryReplies;
    long lastWriteCompletedAt = -1;

    ClientWorkload(WorkloadConfig cfg, List<NodeId> members, SplittableRandom rng, Scheduler scheduler,
                   Sender sender, LongSupplier clock, HistoryRecorder history) {
        this.cfg = cfg;
        this.rng = rng;
        this.scheduler = scheduler;
        this.sender = sender;
        this.clock = clock;
        this.history = history;
        for (int i = 1; i <= cfg.clients(); i++) {
            clients.add(new Client(new ClientSession(i, members, rng.split(),
                    new ClientSession.Config(cfg.timeoutMs(), cfg.backoffMs()))));
        }
    }

    void start() {
        for (Client c : clients) scheduler.at(clock.getAsLong() + think(), () -> nextOperation(c));
    }

    /** The earliest time any client last completed an operation (liveness: every client makes progress). */
    long slowestClientLastCompletedAt() {
        return clients.stream().mapToLong(c -> c.lastCompletedAt).min().orElse(Long.MAX_VALUE);
    }

    private void nextOperation(Client c) {
        long now = clock.getAsLong();
        opsStarted++;
        String key = "k" + rng.nextInt(cfg.keys());
        Attempt a;
        if (rng.nextInt(100) < cfg.readPercent()) {
            history.invoke(c.name, new KvCommand.Get(key), now);
            a = c.session.read(key);
        } else {
            KvCommand cmd = randomWrite(c, key);
            history.invoke(c.name, cmd, now);
            a = c.session.write(cmd);
        }
        dispatch(c, a);
    }

    /** Values name their writer and seq ("c3-17"), so every written value is unique: a strong check. */
    private KvCommand randomWrite(Client c, String key) {
        String value = c.name + "-" + (c.session.seq() + 1);
        int roll = rng.nextInt(100);
        if (roll < 55) return new KvCommand.Put(key, value);
        if (roll < 75) return new KvCommand.Delete(key);
        Optional<String> expected = rng.nextBoolean() ? Optional.empty()
                : Optional.of("c" + (1 + rng.nextInt(cfg.clients())) + "-" + (1 + rng.nextInt((int) Math.max(1, c.session.seq() + 1))));
        return new KvCommand.Cas(key, expected, value);
    }

    private void dispatch(Client c, Attempt a) {
        scheduler.at(clock.getAsLong() + a.delayMs(), () -> {
            attempts++;
            sender.send(c.name, a);
            scheduler.at(clock.getAsLong() + cfg.timeoutMs(), () -> {
                Decision d = c.session.onTimeout(a.attemptId());
                if (d instanceof Decision.Send) timeouts++;
                handle(c, d);
            });
        });
    }

    /** A node's answer to an attempt arrived back at its client. */
    void onReply(long attemptId, Reply reply) {
        Client c = clients.get((int) (attemptId >>> 40) - 1);
        Decision d = c.session.onReply(attemptId, reply);
        if (d instanceof Decision.Send) {
            if (reply instanceof Reply.NotLeader) redirects++;
            else retryReplies++;
        }
        handle(c, d);
    }

    private void handle(Client c, Decision d) {
        switch (d) {
            case Decision.Ignore i -> { }
            case Decision.Send s -> dispatch(c, s.attempt());
            case Decision.Completed done -> {
                long now = clock.getAsLong();
                KvResult result = KvCodec.decodeResult(done.result());
                boolean read = history.complete(c.name, result, now) instanceof KvCommand.Get;
                if (read) readsCompleted++;
                else {
                    writesCompleted++;
                    lastWriteCompletedAt = now;
                }
                c.lastCompletedAt = now;
                scheduler.at(now + think(), () -> nextOperation(c));
            }
        }
    }

    private int think() {
        return cfg.minThinkMs() + rng.nextInt(cfg.maxThinkMs() - cfg.minThinkMs() + 1);
    }
}
