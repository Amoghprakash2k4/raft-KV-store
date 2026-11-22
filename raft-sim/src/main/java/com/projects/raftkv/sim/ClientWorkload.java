package com.projects.raftkv.sim;

import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.node.ProposalOutcome;
import com.projects.raftkv.statemachine.KvCodec;
import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.function.LongSupplier;

/**
 * Open-loop simulated clients (DESIGN.md M2-D). Each client proposes a random PUT/DELETE/CAS/GET to the
 * node it believes is leader, follows rejection hints, and never retries: M2 checks that replicas agree,
 * not what clients observe (that is M4's linearizability checker).
 */
final class ClientWorkload {

    interface Scheduler { void at(long timeMs, Runnable action); }

    interface Submitter { void submit(String client, NodeId target, long proposalId, Bytes command); }

    /** A client that hears nothing for this long assumes its target is gone and picks another node. */
    private static final long SILENCE_MS = 300;

    private final class Client {
        final long id;
        long seq;
        NodeId target;
        long lastHeardAt;
        String lastValue = "";

        Client(long id, NodeId target) { this.id = id; this.target = target; }
        String name() { return "c" + id; }
    }

    private final WorkloadConfig cfg;
    private final List<NodeId> members;
    private final SplittableRandom rng;
    private final Scheduler scheduler;
    private final Submitter submitter;
    private final LongSupplier clock;
    private final Map<Long, Client> inFlight = new HashMap<>();
    private long nextProposalId = 1;

    long proposals, committed, lost, rejected;
    long lastCommitAt = -1;

    ClientWorkload(WorkloadConfig cfg, List<NodeId> members, SplittableRandom rng, Scheduler scheduler,
                   Submitter submitter, LongSupplier clock) {
        this.cfg = cfg;
        this.members = members;
        this.rng = rng;
        this.scheduler = scheduler;
        this.submitter = submitter;
        this.clock = clock;
    }

    void start() {
        for (int i = 1; i <= cfg.clients(); i++) {
            Client c = new Client(i, randomMember());
            scheduler.at(clock.getAsLong() + interval(), () -> send(c));
        }
    }

    private void send(Client c) {
        long now = clock.getAsLong();
        if (now - c.lastHeardAt > SILENCE_MS) {
            c.target = randomMember();
            c.lastHeardAt = now;
        }
        c.seq++;
        long proposalId = nextProposalId++;
        inFlight.put(proposalId, c);
        proposals++;
        submitter.submit(c.name(), c.target, proposalId, KvCodec.encode(new KvRequest(c.id, c.seq, randomCommand(c))));
        scheduler.at(now + interval(), () -> send(c));
    }

    void onOutcome(ProposalOutcome o) {
        Client c = inFlight.remove(o.proposalId());
        if (c == null) return;
        long now = clock.getAsLong();
        c.lastHeardAt = now;
        switch (o) {
            case ProposalOutcome.Committed x -> {
                committed++;
                lastCommitAt = now;
            }
            case ProposalOutcome.Lost x -> lost++;
            case ProposalOutcome.Rejected r -> {
                rejected++;
                c.target = r.leaderHint().orElseGet(this::randomMember);
            }
        }
    }

    private KvCommand randomCommand(Client c) {
        String key = "k" + rng.nextInt(cfg.keys());
        String value = c.name() + "-" + c.seq;
        int roll = rng.nextInt(100);
        KvCommand cmd;
        if (roll < 50) cmd = new KvCommand.Put(key, value);
        else if (roll < 65) cmd = new KvCommand.Delete(key);
        else if (roll < 85) cmd = new KvCommand.Cas(key, rng.nextBoolean() ? Optional.empty() : Optional.of(c.lastValue), value);
        else cmd = new KvCommand.Get(key);
        if (!(cmd instanceof KvCommand.Get) && !(cmd instanceof KvCommand.Delete)) c.lastValue = value;
        return cmd;
    }

    private int interval() {
        return cfg.minIntervalMs() + rng.nextInt(cfg.maxIntervalMs() - cfg.minIntervalMs() + 1);
    }

    private NodeId randomMember() {
        return members.get(rng.nextInt(members.size()));
    }
}
