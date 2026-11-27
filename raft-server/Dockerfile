# One Raft KV node (DESIGN.md M5-E). Built from the repo root by compose.yaml:
#   docker compose up -d --build
# Stage 1 builds the distribution with the project's own Gradle wrapper; stage 2 is a JRE only.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts ./
COPY gradle gradle
COPY buildSrc buildSrc
COPY raft-core raft-core
COPY raft-storage raft-storage
COPY raft-transport raft-transport
COPY raft-proto raft-proto
COPY raft-node raft-node
COPY raft-statemachine raft-statemachine
COPY raft-client raft-client
COPY raft-sim raft-sim
COPY raft-server raft-server
# settings.gradle.kts includes every module, so Gradle needs these folders even though the image
# only builds raft-server (M6).
COPY raft-bench raft-bench
COPY raft-dashboard raft-dashboard
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon :raft-server:installDist -x test

FROM eclipse-temurin:21-jre
COPY --from=build /src/raft-server/build/install/raft-server /opt/raft
ENV PATH="/opt/raft/bin:${PATH}" \
    JAVA_OPTS="-Xmx256m -XX:+ExitOnOutOfMemoryError"
VOLUME /data
EXPOSE 7000 8080
# exec form: the JVM is PID 1, so "docker compose kill" (SIGKILL) hits it directly.
ENTRYPOINT ["/opt/raft/bin/raft-server"]
