package com.projects.raftkv.sim;

import com.projects.raftkv.storage.StorageMutation;
import java.util.EnumSet;
import java.util.Set;

/**
 * Durability settings of a simulated node (DESIGN.md M3). Small on purpose, so that every run rolls
 * WAL segments, takes snapshots, writes checkpoints and streams multi-chunk InstallSnapshots.
 *
 * @param walSegmentBytes    WAL segment size
 * @param snapshotEvery      each node snapshots every N applied entries, N drawn per node from
 *                           [snapshotEvery, 2 * snapshotEvery); 0 disables snapshots
 * @param snapshotChunkBytes InstallSnapshot chunk size (Figure 13)
 * @param mutations          deliberate storage bugs (mutation tests only, DESIGN.md M3-F)
 */
public record StorageConfig(int walSegmentBytes, int snapshotEvery, int snapshotChunkBytes,
                            Set<StorageMutation> mutations) {
    public StorageConfig {
        if (walSegmentBytes < 1 || snapshotEvery < 0 || snapshotChunkBytes < 1)
            throw new IllegalArgumentException("bad storage config");
        mutations = Set.copyOf(mutations);
    }

    public static StorageConfig sim() { return new StorageConfig(4096, 40, 64, Set.of()); }

    public StorageConfig withSnapshotEvery(int n) {
        return new StorageConfig(walSegmentBytes, n, snapshotChunkBytes, mutations);
    }

    public StorageConfig withMutation(StorageMutation m) {
        Set<StorageMutation> next = mutations.isEmpty() ? EnumSet.noneOf(StorageMutation.class) : EnumSet.copyOf(mutations);
        next.add(m);
        return new StorageConfig(walSegmentBytes, snapshotEvery, snapshotChunkBytes, next);
    }
}
