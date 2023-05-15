package org.examples.count;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.examples.BaseITest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;
import java.util.Map;

import static org.examples.count.WordsCountStream.SINK_TOPIC;
import static org.examples.count.WordsCountStream.SOURCE_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WordsCountStreamITest extends BaseITest<String, Long> {

    @Test
    void multiWordCount() {
        // Arrange
        final List<String> words = List.of("the", "quick", "brown", "fox", "jumped", "over", "lazy", "dog");
        final String text = String.join(" ", words);

        // Act
        publishRecord(new ProducerRecord<>(SOURCE_TOPIC, "Text", text));

        // Assert
        ConsumerRecords<String, Long> actualRecords = KafkaTestUtils.getRecords(consumer);

        assertEquals(words.size(), actualRecords.count());

        actualRecords.forEach(record -> {
            assertTrue(words.contains(record.key()));
            assertEquals(1, record.value());
        });
    }

    @Override
    protected Producer<String, String> createProducer() {
        final Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getBootstrapServers());
        final DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new StringSerializer()
        );

        return producerFactory.createProducer();
    }

    @Override
    protected Consumer<String, Long> createConsumer() {
        final Map<String, Object> configs = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), CONSUMER_GROUP, CONSUMER_AUTO_COMMIT);
        final DefaultKafkaConsumerFactory<String, Long> consumerFactory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new LongDeserializer()
        );

        return consumerFactory.createConsumer();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return List.of(SINK_TOPIC);
    }
}
