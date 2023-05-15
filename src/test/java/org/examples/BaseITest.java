package org.examples;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
public abstract class BaseITest<S, T> {

    protected static final String PRODUCER_KEY = "Messages";
    protected static final String CONSUMER_GROUP = "Test";
    protected static final String CONSUMER_AUTO_COMMIT = "true";

    @Container
    public static ElasticsearchContainer elasticsearch = new ElasticsearchContainer(DockerImageName
            .parse("docker.elastic.co/elasticsearch/elasticsearch")
            .withTag("7.9.2"));

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName
            .parse("confluentinc/cp-kafka")
            .withTag("7.4.0"))
            .dependsOn(elasticsearch);

    @DynamicPropertySource
    static void init(DynamicPropertyRegistry registry) {
        registry.add("management.elastic.metrics.export.host", () -> "http://" + elasticsearch.getHttpHostAddress());
        registry.add("spring.kafka.bootstrap-servers", () -> kafka.getBootstrapServers());
    }

    protected Producer<String, S> producer;
    protected Consumer<String, T> consumer;

    @BeforeEach
    void setUp() {
        producer = createProducer();
        consumer = createConsumer();
        consumer.subscribe(getConsumerTopics());
    }

    @AfterEach
    void tearDown() {
        consumer.unsubscribe();
    }

    protected void publishRecord(ProducerRecord<String, S> record) {
        final Future<RecordMetadata> future = producer.send(record);

        await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.MINUTES)
                .with()
                .until(future::isDone);
    }

    protected abstract Producer<String, S> createProducer();

    protected abstract Consumer<String, T> createConsumer();

    protected abstract List<String> getConsumerTopics();
}
