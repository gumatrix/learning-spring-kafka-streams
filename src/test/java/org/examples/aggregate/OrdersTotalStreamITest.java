package org.examples.aggregate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.examples.BaseITest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;
import java.util.Map;

import static org.examples.aggregate.OrdersTotalStream.SINK_TOPIC;
import static org.examples.aggregate.OrdersTotalStream.SOURCE_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrdersTotalStreamITest extends BaseITest<Double, Double> {

    @Test
    void multipleOrders() {
        // Arrange
        final List<Double> amounts = List.of(10.99, 3.50, 100.18);

        // Act
        double expectedOrdersTotal = 0; // Calculate total ahead of time

        for (Double amount : amounts) {
            publishRecord(new ProducerRecord<>(SOURCE_TOPIC, PRODUCER_KEY, amount));
            expectedOrdersTotal += amount;
        }

        // Assert
        ConsumerRecord<String, Double> actualOrdersTotal = KafkaTestUtils.getSingleRecord(consumer, SINK_TOPIC);

        assertEquals(expectedOrdersTotal, actualOrdersTotal.value());
    }

    @Override
    protected Producer<String, Double> createProducer() {
        final Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getBootstrapServers());
        final DefaultKafkaProducerFactory<String, Double> producerFactory = new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new DoubleSerializer()
        );

        return producerFactory.createProducer();
    }

    @Override
    protected Consumer<String, Double> createConsumer() {
        final Map<String, Object> configs = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), CONSUMER_GROUP, CONSUMER_AUTO_COMMIT);
        final DefaultKafkaConsumerFactory<String, Double> consumerFactory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new DoubleDeserializer()
        );

        return consumerFactory.createConsumer();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return List.of(SINK_TOPIC);
    }
}