plugins {
    id("raft.java-conventions")
}

dependencies {
    api(project(":raft-core"))
    implementation(project(":raft-storage"))
    implementation(project(":raft-transport"))
    implementation(project(":raft-statemachine"))
}
