package com.projects.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.projects.raftkv.statemachine.KvCommand;
import com.projects.raftkv.statemachine.KvResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/**
 * Invariant 7 (Linearizability). Each operation must appear to take effect at one instant between its
 * invocation and its response, consistently with a single register per key (Herlihy &amp; Wing 1990;
 * search: Wing &amp; Gong 1993 with Lowe's memoization, as in Porcupine).
 */
class LinearizabilityCheckerTest {

    private static final long NEVER = HistoryRecorder.UNKNOWN_RETURN;
    private final List<HistoryRecorder.Operation> h = new ArrayList<>();

    private void op(String client, KvCommand c, long call, long ret, KvResult out) {
        h.add(new HistoryRecorder.Operation(h.size(), client, c, call, ret, Optional.ofNullable(out)));
    }

    private static KvResult val(String v) { return new KvResult.Value(Optional.ofNullable(v)); }

    private static KvCommand put(String k, String v) { return new KvCommand.Put(k, v); }

    private static KvCommand get(String k) { return new KvCommand.Get(k); }

    private LinearizabilityChecker.Result check() {
        return LinearizabilityChecker.check(h, 1_000_000);
    }

    @Test
    void aSequentialHistoryIsLinearizable() {
        op("c1", put("x", "1"), 0, 10, val(null));
        op("c1", get("x"), 20, 30, val("1"));
        op("c2", put("x", "2"), 40, 50, val("1"));
        op("c2", new KvCommand.Delete("x"), 60, 70, val("2"));
        op("c1", get("x"), 80, 90, val(null));
        assertThat(check().linearizable()).isTrue();
    }

    /** The stale read ReadIndex prevents: x=2 completed before the read began, yet it saw 1. */
    @Test
    void aStaleReadIsNotLinearizable() {
        op("c1", put("x", "1"), 0, 10, val(null));
        op("c2", put("x", "2"), 20, 30, val("1"));
        op("c3", get("x"), 40, 50, val("1"));
        LinearizabilityChecker.Result r = check();
        assertThat(r.linearizable()).isFalse();
        assertThat(r.key()).isEqualTo("x");
        assertThat(r.describe()).contains("c3");
    }

    /** A read concurrent with a write may see the value before or after it. */
    @Test
    void aConcurrentReadMaySeeEitherValue() {
        op("c1", put("x", "1"), 0, 10, val(null));
        op("c2", put("x", "2"), 20, 100, val("1"));
        op("c3", get("x"), 30, 40, val("1"));
        op("c4", get("x"), 50, 60, val("2"));
        assertThat(check().linearizable()).isTrue();
    }

    /** ...but once one reader saw the new value, a later reader cannot see the old one. */
    @Test
    void readsCannotGoBackInTime() {
        op("c1", put("x", "1"), 0, 10, val(null));
        op("c2", put("x", "2"), 20, 100, val("1"));
        op("c3", get("x"), 30, 40, val("2"));
        op("c4", get("x"), 50, 60, val("1"));
        assertThat(check().linearizable()).isFalse();
    }

    /** The retry-without-dedup bug: c1's PUT ran again after c2's PUT completed. */
    @Test
    void aWriteThatTookEffectTwiceIsNotLinearizable() {
        op("c1", put("x", "1"), 0, 100, val(null));
        op("c2", put("x", "2"), 110, 120, val("1"));
        op("c3", get("x"), 130, 140, val("1"));
        assertThat(check().linearizable()).isFalse();
    }

    /** An operation with no response may have taken effect (at any time after its call) or not at all. */
    @Test
    void anUnknownWriteMayOrMayNotHaveHappened() {
        op("c1", put("x", "1"), 0, NEVER, null);
        op("c2", get("x"), 50, 60, val(null));
        op("c3", get("x"), 70, 80, val("1"));
        assertThat(check().linearizable()).isTrue();

        h.clear();
        op("c1", put("x", "1"), 0, NEVER, null);
        op("c2", get("x"), 50, 60, val("3"));
        assertThat(check().linearizable()).isFalse();
    }

    /** It cannot take effect before it was invoked. */
    @Test
    void anUnknownWriteCannotExplainAnEarlierRead() {
        op("c2", get("x"), 0, 10, val("1"));
        op("c1", put("x", "1"), 50, NEVER, null);
        assertThat(check().linearizable()).isFalse();
    }

    @Test
    void casIsCheckedAgainstTheRegister() {
        op("c1", new KvCommand.Cas("x", Optional.empty(), "a"), 0, 10, new KvResult.CasOutcome(true, Optional.empty()));
        op("c2", new KvCommand.Cas("x", Optional.empty(), "b"), 20, 30, new KvResult.CasOutcome(false, Optional.of("a")));
        op("c3", get("x"), 40, 50, val("a"));
        assertThat(check().linearizable()).isTrue();

        h.clear();
        op("c1", new KvCommand.Cas("x", Optional.empty(), "a"), 0, 10, new KvResult.CasOutcome(true, Optional.empty()));
        op("c2", new KvCommand.Cas("x", Optional.empty(), "b"), 20, 30, new KvResult.CasOutcome(true, Optional.empty()));
        assertThat(check().linearizable()).isFalse();
    }

    /** A Stale or Invalid reply never belongs to a correct client history. */
    @Test
    void staleRepliesAreViolations() {
        op("c1", put("x", "1"), 0, 10, new KvResult.Stale(3));
        assertThat(check().linearizable()).isFalse();
    }

    /** Keys are independent registers (linearizability is compositional), so each is checked alone. */
    @Test
    void keysAreCheckedSeparately() {
        op("c1", put("x", "1"), 0, 10, val(null));
        op("c2", put("y", "1"), 0, 10, val(null));
        op("c1", get("y"), 20, 30, val("1"));
        op("c2", get("x"), 20, 30, val(null));        // wrong: x was 1
        LinearizabilityChecker.Result r = check();
        assertThat(r.linearizable()).isFalse();
        assertThat(r.key()).isEqualTo("x");
    }

    @Test
    void theBudgetIsReportedWhenExceeded() {
        // 12 concurrent no-op deletes fit in any order (2^12 subsets to explore) before an impossible read.
        for (int i = 0; i < 12; i++) op("c" + i, new KvCommand.Delete("x"), 0, 1000, val(null));
        op("c99", get("x"), 2000, 2010, val("z"));
        LinearizabilityChecker.Result r = LinearizabilityChecker.check(h, 50);
        assertThat(r.linearizable()).isFalse();
        assertThat(r.budgetExceeded()).isTrue();
    }

    /**
     * Property: a history produced by a real register, with each operation's interval drawn around its
     * true linearization point, is always accepted; changing one read's answer to a value nobody ever
     * wrote is always rejected.
     */
    @Property(tries = 300)
    void generatedHistoriesAreAcceptedAndCorruptedOnesRejected(@ForAll long seed) {
        SplittableRandom rng = new SplittableRandom(seed);
        List<HistoryRecorder.Operation> ops = new ArrayList<>();
        Optional<String> reg = Optional.empty();
        long t = 0;
        int gets = 0;
        for (int i = 0; i < 40; i++) {
            t += 1 + rng.nextInt(10);                       // the true linearization point
            long call = t - rng.nextInt(30), ret = t + rng.nextInt(30);
            String v = "v" + i;
            KvCommand c;
            KvResult out;
            switch (rng.nextInt(4)) {
                case 0 -> { c = put("x", v); out = new KvResult.Value(reg); reg = Optional.of(v); }
                case 1 -> { c = new KvCommand.Delete("x"); out = new KvResult.Value(reg); reg = Optional.empty(); }
                case 2 -> {
                    Optional<String> exp = rng.nextBoolean() ? reg : Optional.of("nope");
                    boolean swap = exp.equals(reg);
                    c = new KvCommand.Cas("x", exp, v);
                    out = new KvResult.CasOutcome(swap, reg);
                    if (swap) reg = Optional.of(v);
                }
                default -> { c = get("x"); out = new KvResult.Value(reg); gets++; }
            }
            boolean unknown = rng.nextInt(20) == 0 && !(c instanceof KvCommand.Get);
            ops.add(new HistoryRecorder.Operation(i, "c" + rng.nextInt(5), c, call, unknown ? NEVER : ret,
                    unknown ? Optional.empty() : Optional.of(out)));
        }
        assertThat(LinearizabilityChecker.check(ops, 5_000_000).linearizable()).isTrue();

        if (gets == 0) return;
        List<HistoryRecorder.Operation> corrupted = new ArrayList<>(ops);
        for (int i = 0; i < corrupted.size(); i++) {
            HistoryRecorder.Operation o = corrupted.get(i);
            if (o.command() instanceof KvCommand.Get) {
                corrupted.set(i, new HistoryRecorder.Operation(o.id(), o.client(), o.command(), o.invokeMs(),
                        o.returnMs(), Optional.of(val("never-written"))));
                break;
            }
        }
        assertThat(LinearizabilityChecker.check(corrupted, 5_000_000).linearizable()).isFalse();
    }
}
