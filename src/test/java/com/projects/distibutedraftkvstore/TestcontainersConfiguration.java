package com.projects.distibutedraftkvstore;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.testcontainers.service.connection.Ssl;
import org.springframework.context.annotation.Bean;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.grafana.LgtmStackContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.pulsar.PulsarContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    CassandraContainer cassandraContainer() {
        return new CassandraContainer(DockerImageName.parse("cassandra:latest"));
    }

    @Bean
    @ServiceConnection
    @Ssl
    ElasticsearchContainer elasticsearchContainer() {
        return new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:9.3.3"));
    }

    @Bean
    @ServiceConnection
    LgtmStackContainer grafanaLgtmContainer() {
        return new LgtmStackContainer(DockerImageName.parse("grafana/otel-lgtm:latest"));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
    }

    @Bean
    @ServiceConnection
    MongoDBContainer mongoDbContainer() {
        return new MongoDBContainer(DockerImageName.parse("mongo:latest"));
    }

    @Bean
    @ServiceConnection
    MySQLContainer mysqlContainer() {
        return new MySQLContainer(DockerImageName.parse("mysql:latest"));
    }

    @Bean
    @ServiceConnection
    Neo4jContainer neo4jContainer() {
        return new Neo4jContainer(DockerImageName.parse("neo4j:latest"));
    }

    @Bean
    @ServiceConnection
    OracleContainer oracleFreeContainer() {
        return new OracleContainer(DockerImageName.parse("gvenzl/oracle-free:latest"));
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:latest"));
    }

    @Bean
    @ServiceConnection
    PulsarContainer pulsarContainer() {
        return new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:latest"));
    }

    @Bean
    @ServiceConnection
    RabbitMQContainer rabbitContainer() {
        return new RabbitMQContainer(DockerImageName.parse("rabbitmq:latest"));
    }

    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redisContainer() {
        return new GenericContainer<>(DockerImageName.parse("redis:latest")).withExposedPorts(6379);
    }

}
