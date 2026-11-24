package com.projects.raftkv.storage.disk;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RealDiskTest extends DiskContractTest {
    @TempDir Path dir;

    @Override Disk disk() { return new RealDisk(dir); }

    @Test
    void dataIsVisibleToANewInstance() {
        Disk d = disk();
        d.append("a", b("persist me"));
        d.fsync("a");
        d.fsyncDir();
        d.close();
        assertThat(new RealDisk(dir).read("a")).isEqualTo(b("persist me"));
    }
}
