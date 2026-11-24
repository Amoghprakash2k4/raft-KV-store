package com.projects.raftkv.storage.wal;

import com.projects.raftkv.storage.StorageMutation;
import java.util.Set;

/**
 * @param maxSegmentBytes a segment is closed (and a new one started) once a sync leaves it this big;
 *                        after a compaction, a checkpoint is written once the WAL is bigger than this
 * @param mutations       deliberate bugs for mutation tests only (DESIGN.md M3-F)
 */
public record WalOptions(long maxSegmentBytes, Set<StorageMutation> mutations) {
    public static final WalOptions DEFAULTS = new WalOptions(64L * 1024 * 1024);

    public WalOptions(long maxSegmentBytes) { this(maxSegmentBytes, Set.of()); }

    public WalOptions {
        if (maxSegmentBytes < 1) throw new IllegalArgumentException("maxSegmentBytes < 1");
        mutations = Set.copyOf(mutations);
    }
}
