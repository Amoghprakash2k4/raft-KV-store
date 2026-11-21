package com.projects.raftkv.core;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class BytesTest {

    /** Value equality: two log entries with equal command bytes must be equal (Log Matching checks rely on it). */
    @Property
    void equalContentMeansEqualAndSameHash(@ForAll byte[] data) {
        Bytes a = Bytes.copyOf(data);
        Bytes b = Bytes.copyOf(data.clone());
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    /** Mutating the source or the returned array must never change the stored value. */
    @Property
    void isDefensivelyCopied(@ForAll byte[] data) {
        Bytes b = Bytes.copyOf(data);
        Bytes snapshot = Bytes.copyOf(data);
        if (data.length > 0) {
            data[0]++;
            b.toByteArray()[0]++;
        }
        assertThat(b).isEqualTo(snapshot);
    }
}
