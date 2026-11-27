// Benchmarks against real server processes (DESIGN.md M6-B). Also the engine behind raft-dashboard.
plugins {
    id("raft.java-conventions")
    application
}

application {
    mainClass = "com.projects.raftkv.bench.BenchMain"
    applicationName = "raft-bench"
}

dependencies {
    api(project(":raft-client"))
    implementation(project(":raft-server"))   // on the classpath the bench hands to the server JVMs it starts
    implementation(libs.hdrhistogram)
    api(libs.jackson.databind)
    runtimeOnly(libs.logback.classic)
}

tasks.named<JavaExec>("run") {
    workingDir = rootDir   // results/ lands in the repo root
}

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}
val integrationTest by tasks.registering(Test::class) {
    description = "Starts real server processes for a few seconds"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    testLogging { events("passed", "failed"); showStandardStreams = true }
    outputs.upToDateWhen { false }
}
