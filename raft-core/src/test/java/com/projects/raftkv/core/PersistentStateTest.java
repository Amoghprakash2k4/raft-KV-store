package com.projects.raftkv.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PersistentStateTest {

    private static final HardState TERM_3 = new HardState(3, Optional.empty());

    @Test
    void acceptsContiguousLogWithNonDecreasingTerms() {
        PersistentState s = new PersistentState(TERM_3, List.of(noop(1, 1), noop(2, 1), noop(3, 3)));
        assertThat(s.entries()).hasSize(3);
    }

    @Test
    void rejectsGapInIndexes() {
        assertThatThrownBy(() -> new PersistentState(TERM_3, List.of(noop(1, 1), noop(3, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsLogNotStartingAtOne() {
        assertThatThrownBy(() -> new PersistentState(TERM_3, List.of(noop(2, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDecreasingTerms() {
        assertThatThrownBy(() -> new PersistentState(TERM_3, List.of(noop(1, 2), noop(2, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** A node can never hold an entry from a term it hasn't reached (it would have adopted that term). */
    @Test
    void rejectsEntryTermAboveCurrentTerm() {
        assertThatThrownBy(() -> new PersistentState(TERM_3, List.of(noop(1, 4))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void entriesAreImmutable() {
        PersistentState s = new PersistentState(TERM_3, List.of(noop(1, 1)));
        assertThatThrownBy(() -> s.entries().add(noop(2, 1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static LogEntry noop(long index, long term) {
        return new LogEntry(index, term, new EntryPayload.Noop());
    }
}
