package com.projects.raftkv.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Saved runs: the dashboard's history and the compare view read them back. */
class ResultStoreTest {

    @TempDir
    Path dir;

    @Test
    void savesAndReadsBackSettingsAndResult() {
        ResultStore store = new ResultStore(dir);
        RunRecord saved = store.save("load", "3 nodes, 16 clients", Map.of("clients", 16), Map.of("opsPerSec", 625.5));

        assertThat(saved.id()).matches("load-\\d{8}-\\d{6}-\\d{3}");
        assertThat(Files.exists(dir.resolve(saved.id() + ".json"))).isTrue();
        RunRecord back = store.get(saved.id()).orElseThrow();
        assertThat(back.kind()).isEqualTo("load");
        assertThat(back.label()).isEqualTo("3 nodes, 16 clients");
        assertThat(back.settings().path("clients").asInt()).isEqualTo(16);
        assertThat(back.result().path("opsPerSec").asDouble()).isEqualTo(625.5);
        assertThat(back.machine()).containsKeys("os", "cpus");
    }

    @Test
    void listsNewestFirstAndSkipsFilesThatAreNotRuns() throws Exception {
        ResultStore store = new ResultStore(dir);
        RunRecord first = store.save("sim", "a", Map.of(), Map.of());
        Thread.sleep(5);   // ids and createdAt have millisecond resolution
        RunRecord second = store.save("failover", "b", Map.of(), Map.of());
        Files.writeString(dir.resolve("notes.json"), "{ not a run");

        List<RunRecord> all = store.list();
        assertThat(all).extracting(RunRecord::id).containsExactly(second.id(), first.id());
    }

    @Test
    void anEmptyOrMissingDirectoryHasNoRuns() {
        assertThat(new ResultStore(dir.resolve("missing")).list()).isEmpty();
    }

    @Test
    void deletesOnlyWhatItSaved() {
        ResultStore store = new ResultStore(dir);
        RunRecord r = store.save("sim", "a", Map.of(), Map.of());
        assertThat(store.delete(r.id())).isTrue();
        assertThat(store.get(r.id())).isEmpty();
        assertThat(store.delete(r.id())).isFalse();
    }

    /** Ids come from the browser: anything that isn't kind-digits must not reach the filesystem. */
    @Test
    void rejectsIdsThatCouldEscapeTheDirectory() throws Exception {
        Files.writeString(dir.getParent().resolve("secret.json"), "{}");
        ResultStore store = new ResultStore(dir);
        for (String bad : List.of("../secret", "sim-1/../../secret", "SIM-1", "sim-1.json", "")) {
            assertThat(store.get(bad)).as(bad).isEmpty();
            assertThat(store.delete(bad)).as(bad).isFalse();
        }
        assertThat(Files.exists(dir.getParent().resolve("secret.json"))).isTrue();
    }
}
