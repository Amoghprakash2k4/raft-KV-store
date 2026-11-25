// Local web dashboard: run simulations and benchmarks from the browser (DESIGN.md M6-C).
plugins {
    id("raft.java-conventions")
    application
}

application {
    mainClass = "com.projects.raftkv.dashboard.MainApplication"
    applicationName = "raft-dashboard"
}

dependencies {
    implementation(project(":raft-sim"))
    implementation(project(":raft-storage"))   // StorageMutation
    implementation(project(":raft-core"))      // ProtocolMutation
    implementation(project(":raft-bench"))
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

tasks.named<JavaExec>("run") {
    workingDir = rootDir   // shares results/ with raft-bench
}
