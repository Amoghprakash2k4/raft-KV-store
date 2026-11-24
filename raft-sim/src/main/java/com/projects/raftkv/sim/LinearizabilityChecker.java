package com.projects.raftkv.sim;

import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Invariant 7: the client history is linearizable (Herlihy &amp; Wing 1990). Every operation must appear
 * to take effect atomically at one point between its invocation and its response, and the resulting
 * sequence must be legal for a key-value store.
 *
 * <p>Search: Wing &amp; Gong (1993) with Lowe's memoization, the algorithm Porcupine uses. Walk the
 * invocation/response events in time order. At an invocation, try to linearize that operation now
 * (if the model accepts its output from the current state); at a response whose operation has not been
 * linearized yet, the current choice was wrong, so backtrack. A memo of (set of linearized operations,
 * state) skips configurations already shown to fail.
 *
 * <p>Keys are independent registers, and linearizability is compositional (Herlihy &amp; Wing, "locality"),
 * so the history is split per key and each part checked alone; that keeps the search small.
 *
 * <p>An operation with no response (unknown outcome) has its response at +infinity and accepts any
 * output: it can be linearized anywhere after its invocation, including "after everything", which is
 * the same as never.
 */
final class LinearizabilityChecker {

    /** {@code key} and {@code describe} explain the first failing key; {@code steps} is the search size. */
    record Result(boolean linearizable, boolean budgetExceeded, String key, long steps, int operations, String describe) {}

    private LinearizabilityChecker() {}

    static Result check(List<HistoryRecorder.Operation> history, long stepBudget) {
        Map<String, List<HistoryRecorder.Operation>> byKey = history.stream().collect(
                Collectors.groupingBy(HistoryRecorder.Operation::key, LinkedHashMap::new, Collectors.toList()));
        long steps = 0;
        List<String> keys = new ArrayList<>(byKey.keySet());
        keys.sort(Comparator.naturalOrder());
        for (String key : keys) {
            Result r = checkKey(key, byKey.get(key), stepBudget);
            steps += r.steps();
            if (!r.linearizable()) {
                return new Result(false, r.budgetExceeded(), key, steps, history.size(), r.describe());
            }
        }
        return new Result(true, false, null, steps, history.size(), "linearizable");
    }

    // ---------------------------------------------------------------- the model: one register per key

    /** {@code ok}: the operation, with its recorded output, is legal from the state; {@code next}: the state after it. */
    private record Step(boolean ok, Optional<String> next) {}

    private static final Step ILLEGAL = new Step(false, Optional.empty());

    /**
     * The sequential specification of {@link com.projects.raftkv.statemachine.KvStateMachine} for one key.
     * Put/Delete return the previous value, Get the current one, Cas (swapped, previous). An unknown
     * output accepts anything. A Stale or Invalid answer is never legal for a correct client.
     */
    static Step step(Optional<String> state, KvCommand c, Optional<KvResult> output) {
        Optional<String> next;
        KvResult expected;
        switch (c) {
            case KvCommand.Put p -> { expected = new KvResult.Value(state); next = Optional.of(p.value()); }
            case KvCommand.Delete d -> { expected = new KvResult.Value(state); next = Optional.empty(); }
            case KvCommand.Get g -> { expected = new KvResult.Value(state); next = state; }
            case KvCommand.Cas cas -> {
                boolean swap = state.equals(cas.expected());
                expected = new KvResult.CasOutcome(swap, state);
                next = swap ? Optional.of(cas.newValue()) : state;
            }
        }
        if (output.isPresent() && !output.get().equals(expected)) return ILLEGAL;
        return new Step(true, next);
    }

    // ---------------------------------------------------------------- the search (one key)

    /** A node of the doubly linked event list: an invocation (with its response as {@code match}) or a response. */
    private static final class Entry {
        final int op;
        final boolean call;
        final long time;
        Entry match;
        Entry prev, next;

        Entry(int op, boolean call, long time) { this.op = op; this.call = call; this.time = time; }
    }

    private record Memo(BitSet linearized, Optional<String> state) {}

    private record Frame(Entry entry, Optional<String> state) {}

    private static Result checkKey(String key, List<HistoryRecorder.Operation> raw, long budget) {
        int n = raw.size();
        List<HistoryRecorder.Operation> ops = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            HistoryRecorder.Operation o = raw.get(i);
            ops.add(new HistoryRecorder.Operation(i, o.client(), o.command(), o.invokeMs(), o.returnMs(), o.output()));
        }
        // Events in time order; at equal times invocations first (they then count as concurrent: lenient).
        List<Entry> events = new ArrayList<>(2 * n);
        for (HistoryRecorder.Operation o : ops) {
            Entry call = new Entry(o.id(), true, o.invokeMs());
            Entry ret = new Entry(o.id(), false, o.returnMs());
            call.match = ret;
            events.add(call);
            events.add(ret);
        }
        events.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> !e.call).thenComparingInt(e -> e.op));
        Entry head = new Entry(-1, false, Long.MIN_VALUE);
        Entry last = head;
        for (Entry e : events) {
            last.next = e;
            e.prev = last;
            last = e;
        }

        Set<Memo> memo = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        BitSet linearized = new BitSet(n);
        Optional<String> state = Optional.empty();
        Entry entry = head.next;
        long steps = 0;
        int deepest = 0;
        int stuckAt = -1;
        while (head.next != null) {
            if (++steps > budget) {
                return new Result(false, true, key, steps, n, "key " + key + ": search budget of " + budget
                        + " steps exceeded with " + n + " operations (too much concurrency to decide)");
            }
            if (entry.call) {
                HistoryRecorder.Operation o = ops.get(entry.op);
                Step s = step(state, o.command(), o.output());
                if (s.ok()) {
                    BitSet next = (BitSet) linearized.clone();
                    next.set(entry.op);
                    if (memo.add(new Memo(next, s.next()))) {
                        stack.push(new Frame(entry, state));
                        state = s.next();
                        linearized = next;
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // A response whose operation is not linearized yet: undo the most recent choice.
                if (stack.size() >= deepest) {
                    deepest = stack.size();
                    stuckAt = entry.op;
                }
                if (stack.isEmpty()) {
                    return new Result(false, false, key, steps, n, describe(key, ops, deepest, stuckAt));
                }
                Frame f = stack.pop();
                entry = f.entry();
                state = f.state();
                linearized = (BitSet) linearized.clone();
                linearized.clear(entry.op);
                unlift(entry);
                entry = entry.next;
            }
        }
        return new Result(true, false, key, steps, n, "linearizable");
    }

    private static void lift(Entry e) {
        e.prev.next = e.next;
        e.next.prev = e.prev;
        Entry m = e.match;
        m.prev.next = m.next;
        if (m.next != null) m.next.prev = m.prev;
    }

    private static void unlift(Entry e) {
        Entry m = e.match;
        m.prev.next = m;
        if (m.next != null) m.next.prev = m;
        e.prev.next = e;
        e.next.prev = e;
    }

    /** The operation the search could never get past, and everything overlapping it in time. */
    private static String describe(String key, List<HistoryRecorder.Operation> ops, int deepest, int stuckAt) {
        StringBuilder sb = new StringBuilder("key " + key + ": not linearizable (" + ops.size()
                + " operations; at most " + deepest + " could be ordered legally). No legal point for:\n  "
                + ops.get(stuckAt));
        HistoryRecorder.Operation s = ops.get(stuckAt);
        sb.append("\nThe last operations that completed before it began (their effects must be visible):");
        ops.stream()
                .filter(o -> o.returnMs() < s.invokeMs())
                .sorted(Comparator.comparingLong(HistoryRecorder.Operation::returnMs).reversed())
                .limit(5)
                .sorted(Comparator.comparingLong(HistoryRecorder.Operation::returnMs))
                .forEach(o -> sb.append("\n  ").append(o));
        sb.append("\nOperations overlapping it, in invocation order:");
        ops.stream()
                .filter(o -> o.invokeMs() <= s.returnMs() && o.returnMs() >= s.invokeMs() - 50)
                .sorted(Comparator.comparingLong(HistoryRecorder.Operation::invokeMs))
                .limit(25)
                .forEach(o -> sb.append("\n  ").append(o));
        return sb.toString();
    }
}
