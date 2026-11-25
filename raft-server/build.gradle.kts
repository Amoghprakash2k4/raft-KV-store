// One real node: event loop + gRPC peers + HTTP client API + metrics (DESIGN.md M5).
plugins {
    id("raft.java-conventions")
    application
}

application {
    mainClass = "com.projects.raftkv.server.Main"
    applicationName = "raft-server"
}

dependencies {
    implementation(project(":raft-node"))
    implementation(project(":raft-storage"))
    implementation(project(":raft-transport"))
    implementation(project(":raft-statemachine"))
    implementation(project(":raft-client"))
    implementation(libs.micrometer.prometheus)
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)

    testImplementation(project(":raft-sim"))   // the linearizability checker, for the kill -9 test
}

// The raft-cli command ships in the same distribution (build/install/raft-server/bin/raft-cli).
val cliScripts by tasks.registering(CreateStartScripts::class) {
    mainClass = "com.projects.raftkv.client.RaftCli"
    applicationName = "raft-cli"
    outputDir = layout.buildDirectory.dir("cli-scripts").get().asFile
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
}
distributions.named("main") {
    contents { from(cliScripts) { into("bin") } }
}

// kill -9 tests start real JVMs: slow, so they run only on request.
//   ./gradlew :raft-server:integrationTest
tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}
val integrationTest by tasks.registering(Test::class) {
    description = "Real processes, real disks, SIGKILL (DESIGN.md M5-E)"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    systemProperty("raft.server.classpath", sourceSets.main.get().runtimeClasspath.asPath)
    providers.gradleProperty("kill9.kills").orNull?.let { systemProperty("kill9.kills", it) }
    outputs.upToDateWhen { false }
    testLogging { events("passed", "failed"); showStandardStreams = true }
    shouldRunAfter(tasks.test)
}
