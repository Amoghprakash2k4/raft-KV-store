package com.projects.raftkv.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.core.Bytes;
import java.util.Optional;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * DESIGN.md M2-B: the codec round-trips every value and is canonical (one value, one byte string),
 * which M3's snapshot comparison and the simulator's state hashes rely on.
 */
class KvCodecProperties {

    @Property
    void requestRoundTrips(@ForAll("requests") KvRequest r) {
        assertThat(KvCodec.decodeRequest(KvCodec.encode(r))).isEqualTo(r);
    }

    @Property
    void resultRoundTrips(@ForAll("results") KvResult r) {
        assertThat(KvCodec.decodeResult(KvCodec.encode(r))).isEqualTo(r);
    }

    /** Canonical: any byte string the decoder accepts re-encodes to exactly the same bytes. */
    @Property(tries = 2000)
    void anyAcceptedBytesAreCanonical(@ForAll byte[] raw) {
        Bytes b = Bytes.copyOf(raw);
        KvRequest decoded;
        try {
            decoded = KvCodec.decodeRequest(b);
        } catch (IllegalArgumentException rejected) {
            return;
        }
        assertThat(KvCodec.encode(decoded)).isEqualTo(b);
    }

    @Property
    void truncatedEncodingsAreRejected(@ForAll("requests") KvRequest r, @ForAll("fraction") double f) {
        byte[] full = KvCodec.encode(r).toByteArray();
        int cut = (int) (full.length * f);
        byte[] prefix = java.util.Arrays.copyOf(full, cut);
        assertThatThrownBy(() -> KvCodec.decodeRequest(Bytes.copyOf(prefix)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Example
    void trailingBytesAreRejected() {
        byte[] full = KvCodec.encode(new KvRequest(1, 2, new KvCommand.Get("k"))).toByteArray();
        byte[] longer = java.util.Arrays.copyOf(full, full.length + 1);
        assertThatThrownBy(() -> KvCodec.decodeRequest(Bytes.copyOf(longer)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Example
    void invalidUtf8IsRejected() {
        byte[] full = KvCodec.encode(new KvRequest(1, 2, new KvCommand.Get("k"))).toByteArray();
        full[full.length - 1] = (byte) 0xFF;   // the key's only byte
        assertThatThrownBy(() -> KvCodec.decodeRequest(Bytes.copyOf(full)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<Double> fraction() {
        return Arbitraries.doubles().between(0, 0.999).ofScale(3);
    }

    @Provide
    Arbitrary<KvRequest> requests() {
        Arbitrary<KvCommand> cmd = Arbitraries.oneOf(
                Combinators.combine(strings(), strings()).as(KvCommand.Put::new),
                strings().map(KvCommand.Get::new),
                strings().map(KvCommand.Delete::new),
                Combinators.combine(strings(), optionalStrings(), strings()).as(KvCommand.Cas::new));
        return Combinators.combine(Arbitraries.longs(), Arbitraries.longs(), cmd).as(KvRequest::new);
    }

    @Provide
    Arbitrary<KvResult> results() {
        return Arbitraries.oneOf(
                optionalStrings().map(KvResult.Value::new),
                Combinators.combine(Arbitraries.of(true, false), optionalStrings()).as(KvResult.CasOutcome::new),
                strings().map(KvResult.Invalid::new));
    }

    private static Arbitrary<String> strings() {
        return Arbitraries.strings().all().ofMaxLength(20)
                .filter(s -> s.codePoints().noneMatch(cp -> cp >= 0xD800 && cp <= 0xDFFF));
    }

    private static Arbitrary<Optional<String>> optionalStrings() {
        return strings().optional();
    }
}
