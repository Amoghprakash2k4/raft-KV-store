plugins {
    id("raft.java-conventions")
}

dependencies {
    api(project(":raft-core"))
    implementation(project(":raft-storage"))
    implementation(project(":raft-transport"))
    api(project(":raft-statemachine"))   // NodeDriver's API takes a StateMachine
}
