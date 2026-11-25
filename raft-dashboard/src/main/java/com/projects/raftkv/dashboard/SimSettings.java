package com.projects.raftkv.dashboard;

import com.projects.raftkv.core.ProtocolMutation;
import com.projects.raftkv.sim.FaultConfig;
import com.projects.raftkv.sim.SimConfig;
import com.projects.raftkv.storage.StorageMutation;
import java.util.ArrayList;
import java.util.List;

/**
 * What the Simulator tab can run (DESIGN.md M6-C): the gate's fault mixes, the M5 scenarios, the
 * hardening flags, group commit, and any one mutation, so a deliberately broken rule can be watched
 * being caught.
 *
 * @param mix      chaos, crashLoop, leaderHunting, quiet, isolateFollower, isolateLeader
 * @param mutation "none", a {@link ProtocolMutation} or {@link StorageMutation} name, LYING_DISK or SKIP_DEDUP
 */
public record SimSettings(int nodes, int seeds, long firstSeed, String mix, boolean preVote, boolean checkQuorum,
                          int batchWindowMs, String mutation) {

    public static final List<String> MIXES = List.of("chaos", "crashLoop", "leaderHunting", "quiet", "isolateFollower",
            "isolateLeader");

    public SimSettings {
        if (nodes != 3 && nodes != 5) throw new IllegalArgumentException("nodes must be 3 or 5");
        if (seeds < 1 || seeds > 20_000) throw new IllegalArgumentException("seeds must be 1..20000");
        if (firstSeed < 1) throw new IllegalArgumentException("firstSeed must be >= 1");
        if (!MIXES.contains(mix)) throw new IllegalArgumentException("unknown mix " + mix);
        if (batchWindowMs < 0 || batchWindowMs > 50) throw new IllegalArgumentException("batchWindowMs must be 0..50");
        if (!mutations().contains(mutation)) throw new IllegalArgumentException("unknown mutation " + mutation);
    }

    public static List<String> mutations() {
        List<String> out = new ArrayList<>(List.of("none", "LYING_DISK", "SKIP_DEDUP"));
        for (ProtocolMutation m : ProtocolMutation.values()) out.add(m.name());
        for (StorageMutation m : StorageMutation.values()) out.add(m.name());
        return out;
    }

    public SimConfig toConfig() {
        SimConfig c = switch (mix) {
            case "chaos" -> SimConfig.chaos(nodes);
            case "crashLoop" -> SimConfig.crashLoop(nodes);
            case "leaderHunting" -> SimConfig.chaos(nodes).withFaults(FaultConfig.leaderHunting());
            case "quiet" -> SimConfig.quiet(nodes);
            case "isolateFollower" -> SimConfig.scenario(nodes, SimConfig.Scenario.ISOLATE_FOLLOWER);
            case "isolateLeader" -> SimConfig.scenario(nodes, SimConfig.Scenario.ISOLATE_LEADER);
            default -> throw new IllegalArgumentException(mix);
        };
        c = c.withHardening(preVote, checkQuorum).withBatching(batchWindowMs);
        return switch (mutation) {
            case "none" -> c;
            case "LYING_DISK" -> c.withLyingDisks();
            case "SKIP_DEDUP" -> c.withoutDedup();
            default -> isStorage(mutation) ? c.withMutation(StorageMutation.valueOf(mutation))
                    : c.withMutation(ProtocolMutation.valueOf(mutation));
        };
    }

    private static boolean isStorage(String name) {
        for (StorageMutation m : StorageMutation.values()) if (m.name().equals(name)) return true;
        return false;
    }

    public String label() {
        return nodes + " nodes, " + seeds + " seeds, " + mix + (preVote ? "" : ", no PreVote") + (checkQuorum ? "" : ", no CheckQuorum")
                + (batchWindowMs > 0 ? ", group commit" : "") + (mutation.equals("none") ? "" : ", mutation " + mutation);
    }
}
