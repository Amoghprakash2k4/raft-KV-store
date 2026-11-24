plugins {
    id("raft.java-conventions")
}

dependencies {
    api(project(":raft-core"))           // NodeId, Bytes
    api(project(":raft-statemachine"))   // KvCommand, KvRequest, KvCodec: the request format
}
