package com.projects.raftkv.storage.disk;

import java.util.SplittableRandom;

class SimDiskTest extends DiskContractTest {
    @Override Disk disk() { return new SimDisk(new SplittableRandom(1)); }
}
