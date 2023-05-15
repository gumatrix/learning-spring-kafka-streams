package org.examples.headerenricher;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.examples.BaseITest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.examples.headerenricher.ProductProcessorStream.SINK_TOPIC;
import static org.examples.headerenricher.ProductProcessorStream.SOURCE_TOPIC;
import static org.examples.headerenricher.ProductType.HARDWARE;

public class ProductProcessorStreamITest extends BaseITest<Product, Product> {

    @Test
    void enrichProductHeaderWithHardwareProductType() {
        // Arrange
        final ProductType expectedProductType = HARDWARE;
        final Product keyboard = new Product("Standard QWERTY keyboard", expectedProductType, BigDecimal.valueOf(12.99));

        // Act
        publishRecord(new ProducerRecord<>(SOURCE_TOPIC, PRODUCER_KEY, keyboard));

        // Assert
        assertEnrichedProductTypeHeaderEquals(expectedProductType);
    }

    @Override
    protected Producer<String, Product> createProducer() {
        final Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getBootstrapServers());
        final DefaultKafkaProducerFactory<String, Product> factory = new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new JsonSerializer<>()
        );

        return factory.createProducer();
    }

    @Override
    protected Consumer<String, Product> createConsumer() {
        final Map<String, Object> configs = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), CONSUMER_GROUP, CONSUMER_AUTO_COMMIT);
        final DefaultKafkaConsumerFactory<String, Product> factory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new JsonDeserializer<>(Product.class)
        );

        return factory.createConsumer();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return List.of(SINK_TOPIC);
    }

    private void assertEnrichedProductTypeHeaderEquals(ProductType productType) {
        final ConsumerRecord<String, Product> actualResult = KafkaTestUtils.getSingleRecord(consumer, SINK_TOPIC);

        final Map<String, String> headers = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(actualResult.headers().iterator(), Spliterator.ORDERED), false)
                .collect(Collectors.toMap(Header::key, header -> new String(header.value(), StandardCharsets.UTF_8)));

        assertEquals(productType.toString(), headers.get("ProductType"));
    }
}
