// Generated protobuf + gRPC classes for the peer transport (DESIGN.md M5-B). A module of its own so the
// generated code can compile without our -Werror -Xlint:all, which it was never written for.
plugins {
    id("raft.java-conventions")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(libs.protobuf.java)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}" }
    }
    generateProtoTasks {
        all().configureEach {
            plugins {
                // omit: no javax.annotation.Generated, so no extra annotations dependency is needed
                create("grpc") { option("@generated=omit") }
            }
        }
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.removeAll { it == "-Werror" || it.startsWith("-Xlint") }
}
