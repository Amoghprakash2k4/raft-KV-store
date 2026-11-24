package com.projects.raftkv.storage.disk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.projects.raftkv.storage.StorageException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Behaviour every Disk must share while nothing crashes. Runs against RealDisk and SimDisk. */
abstract class DiskContractTest {

    abstract Disk disk();

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void appendCreatesAndExtendsFiles() {
        Disk d = disk();
        assertThat(d.exists("a")).isFalse();
        d.append("a", b("hello "));
        d.append("a", b("world"));
        assertThat(d.exists("a")).isTrue();
        assertThat(d.size("a")).isEqualTo(11);
        assertThat(d.read("a")).isEqualTo(b("hello world"));
    }

    @Test
    void listIsSortedByName() {
        Disk d = disk();
        d.append("wal-2", b("x"));
        d.append("wal-1", b("y"));
        d.append("snap", b("z"));
        assertThat(d.list()).containsExactly("snap", "wal-1", "wal-2");
    }

    @Test
    void truncateCutsTheTail() {
        Disk d = disk();
        d.append("a", b("abcdef"));
        d.truncate("a", 2);
        d.append("a", b("Z"));
        assertThat(d.read("a")).isEqualTo(b("abZ"));
    }

    @Test
    void renameReplacesTheTargetAtomically() {
        Disk d = disk();
        d.append("snap", b("old"));
        d.append("snap.tmp", b("new"));
        d.rename("snap.tmp", "snap");
        assertThat(d.exists("snap.tmp")).isFalse();
        assertThat(d.read("snap")).isEqualTo(b("new"));
    }

    @Test
    void deleteRemovesAFile() {
        Disk d = disk();
        d.append("a", b("x"));
        d.delete("a");
        assertThat(d.exists("a")).isFalse();
        assertThat(d.list()).isEmpty();
    }

    @Test
    void readingAMissingFileFails() {
        assertThatThrownBy(() -> disk().read("nope")).isInstanceOf(StorageException.class);
    }

    @Test
    void syncsSucceedOnAHealthyDisk() {
        Disk d = disk();
        d.append("a", b("x"));
        d.fsync("a");
        d.fsyncDir();
        assertThat(d.read("a")).isEqualTo(b("x"));
    }
}
