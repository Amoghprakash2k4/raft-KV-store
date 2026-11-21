plugins {
    // Resolves the Java 21 toolchain on machines (e.g. CI) that don't have it installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "raft-kv"

include(
    "raft-core",
    "raft-storage",
    "raft-statemachine",
    "raft-transport",
    "raft-node",
    "raft-client",
    "raft-sim",
)
