plugins {
    id("raft.java-conventions")
}

dependencies {
    implementation(project(":raft-core"))
    implementation(project(":raft-storage"))
    implementation(project(":raft-transport"))
    implementation(project(":raft-statemachine"))
    implementation(project(":raft-node"))
    implementation(project(":raft-client"))
}

// Seed control:  -Psim.seeds=5000 (count per test)   -Psim.seed=1234 (replay one seed)
tasks.test {
    providers.gradleProperty("sim.seeds").orNull?.let { systemProperty("sim.seeds", it) }
    providers.gradleProperty("sim.seed").orNull?.let { systemProperty("sim.seed", it) }
    providers.gradleProperty("sim.trace").orNull?.let { systemProperty("sim.trace", it) }
    providers.gradleProperty("sim.clientWindow").orNull?.let { systemProperty("sim.clientWindow", it) }
    maxHeapSize = "1g"
    testLogging { events("failed", "standard_out") }
}
