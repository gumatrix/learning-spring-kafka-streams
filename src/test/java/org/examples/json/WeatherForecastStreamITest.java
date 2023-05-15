package org.examples.json;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.examples.BaseITest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.examples.json.WeatherForecastStream.SINK_TOPIC;
import static org.examples.json.WeatherForecastStream.SOURCE_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WeatherForecastStreamITest extends BaseITest<Observer, Forecast> {

    private static final String GREATER_LONDON_LOCATION = "Greater London";
    private static final String BEXLEYHEATH_LOCATION = "Bexleyheath";
    private static final String CROYDON_LOCATION = "Croydon";

    @Test
    void calculateAverageForecastPerLocation() {
        // Arrange
        final List<Observer> observers = List.of(
                new Observer(GREATER_LONDON_LOCATION, 21.0f, 1060.5f, 9.3f),
                new Observer(GREATER_LONDON_LOCATION, 21.1f, 1060.4f, 9.5f),
                new Observer(GREATER_LONDON_LOCATION, 21.2f, 1060.3f, 9.4f),
                new Observer(BEXLEYHEATH_LOCATION, 20.9f, 1062.35f, 9.8f),
                new Observer(CROYDON_LOCATION, 21.4f, 1061.48f, 10.1f)
        );

        final Map<String, Forecast> expectedForecasts = Map.of(
                GREATER_LONDON_LOCATION, new Forecast(21.1f, 1060.4f, 9.4f),
                BEXLEYHEATH_LOCATION, new Forecast(20.9f, 1062.35f, 9.8f),
                CROYDON_LOCATION, new Forecast(21.4f, 1061.48f, 10.1f)
        );

        // Act
        observers.forEach(observer -> publishRecord(new ProducerRecord<>(SOURCE_TOPIC, generateKey(), observer)));

        // Assert
        final ConsumerRecords<String, Forecast> actualForecasts = KafkaTestUtils.getRecords(consumer);

        assertEquals(expectedForecasts.size(), actualForecasts.count());

        actualForecasts.forEach(forecast -> {
            assertEquals(expectedForecasts.get(forecast.key()), forecast.value());
        });
    }

    @Override
    protected Producer<String, Observer> createProducer() {
        final Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getBootstrapServers());
        final DefaultKafkaProducerFactory<String, Observer> producerFactory = new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new JsonSerializer<>()
        );

        return producerFactory.createProducer();
    }

    @Override
    protected Consumer<String, Forecast> createConsumer() {
        final Map<String, Object> configs = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), CONSUMER_GROUP, CONSUMER_AUTO_COMMIT);
        final DefaultKafkaConsumerFactory<String, Forecast> consumerFactory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new JsonDeserializer<>(Forecast.class)
        );

        return consumerFactory.createConsumer();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return List.of(SINK_TOPIC);
    }

    private static String generateKey() {
        return UUID.randomUUID().toString();
    }
}
