plugins {
    id("raft.java-conventions")
}

dependencies {
    api(project(":raft-core"))
    api(libs.grpc.api)                   // ManagedChannel / ServerBuilder appear in the public API
    implementation(project(":raft-proto"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.slf4j.api)
    testImplementation(libs.grpc.inprocess)
}
