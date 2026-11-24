package com.projects.raftkv.storage;

/**
 * Deliberate storage bugs for mutation testing (DESIGN.md M3-F), like ProtocolMutation for the core.
 * The simulator must catch each one. Production code never enables these.
 */
public enum StorageMutation {
    /** Recovery keeps a torn tail instead of cutting it, so later appends land after garbage. */
    SKIP_TORN_TAIL_CUT,
    /** The snapshot file is renamed into place without fsyncing its data first. */
    SKIP_SNAPSHOT_DATA_FSYNC
}
