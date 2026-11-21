package com.projects.raftkv.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReadyTest {

    @Test
    void emptyReadyNeedsNoSync() {
        assertThat(Ready.EMPTY.isEmpty()).isTrue();
        assertThat(Ready.EMPTY.mustSync()).isFalse();
    }

    @Test
    void anyPersistOpRequiresSync() {
        Ready r = new Ready(List.of(new PersistOp.SaveHardState(HardState.INITIAL)),
                List.of(), List.of(), List.of(), Optional.empty());
        assertThat(r.mustSync()).isTrue();
    }
}
