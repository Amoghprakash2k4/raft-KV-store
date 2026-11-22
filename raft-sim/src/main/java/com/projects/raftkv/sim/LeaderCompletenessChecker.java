package com.projects.raftkv.sim;

import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.core.RaftStatus;
import com.projects.raftkv.core.Role;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Invariant 4, Leader Completeness (§5.4, Figure 3): if an entry is committed in a given term, it is
 * present in the logs of the leaders of all higher terms.
 *
 * <p>The first time any node reports index i as committed, the checker records i's prefix hash and an
 * upper bound on the term it was committed in: the reporting node's currentTerm (commitment happened no
 * later than that). Every leader of term T must then hold, with the same prefix hash, every committed
 * index whose bound is below T. A prefix hash match at the highest such index covers all lower ones.
 */
public final class LeaderCompletenessChecker {
    private record Committed(long prefixHash, long termBound) {}

    private final List<Committed> committed = new ArrayList<>();             // position i - 1 = index i
    private final TreeMap<Long, Long> maxIndexByTermBound = new TreeMap<>();  // termBound -> highest index
    private final Map<NodeId, Integer> checkedAtVersion = new HashMap<>();
    private int version;

    void observe(long now, RaftStatus s, NodeLogTracker tracker) {
        recordCommits(s, tracker);
        if (s.soft().role() == Role.LEADER) checkLeader(now, s, tracker);
        else checkedAtVersion.remove(s.id());
    }

    private void recordCommits(RaftStatus s, NodeLogTracker tracker) {
        for (long i = committed.size() + 1L; i <= s.commitIndex(); i++) {
            long bound = s.hard().currentTerm();
            committed.add(new Committed(tracker.hashAt(i), bound));
            maxIndexByTermBound.merge(bound, i, Math::max);
            version++;
        }
        // A node committing a *different* entry at an already-committed index shows up as a State
        // Machine Safety violation in the same step (commit and apply happen together), so it is
        // reported there, not here.
    }

    /** Re-checks a leader only when it is newly elected or new commits were recorded since its last check. */
    private void checkLeader(long now, RaftStatus s, NodeLogTracker tracker) {
        Integer last = checkedAtVersion.put(s.id(), version);
        if (last != null && last == version) return;
        long term = s.soft().term();
        long required = 0;
        for (long idx : maxIndexByTermBound.headMap(term, false).values()) required = Math.max(required, idx);
        if (required == 0) return;
        if (tracker.lastIndex() < required || tracker.hashAt(required) != committed.get((int) required - 1).prefixHash()) {
            throw new InvariantViolation("Leader Completeness violated at t=" + now + "ms: " + s.id()
                    + " is leader of term " + term + " but its log (last index " + tracker.lastIndex()
                    + ") does not contain every entry committed in earlier terms, up to index " + required);
        }
    }

    public int committedEntries() { return committed.size(); }
}
