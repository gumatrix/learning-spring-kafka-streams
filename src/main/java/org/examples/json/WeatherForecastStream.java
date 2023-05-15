package org.examples.json;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.function.BinaryOperator;

@Configuration(proxyBeanMethods = false)
class WeatherForecastStream {

    static final String SOURCE_TOPIC = "weather-observer-source";
    static final String SINK_TOPIC = "weather-forecast-sink";

    @Bean
    public NewTopic observerTopic() {
        return TopicBuilder.name(SOURCE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic forecastTopic() {
        return TopicBuilder.name(SINK_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public KStream<String, Observer> weatherForecastKStream(StreamsBuilder builder) {
        final KStream<String, Observer> observers = builder.stream(
                SOURCE_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Observer.class)));

        final KTable<String, Observation> observations = observers
                .groupBy((key, value) -> value.location())
                .aggregate(
                        Observation::new,
                        (key, value, observation) -> {
                            observation.record(value);

                            return observation;
                        },
                        Materialized.<String, Observation, KeyValueStore<Bytes, byte[]>>as("weather-observation-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(Observation.class))
                );

        observations.toStream()
                .mapValues(observation -> {
                    float totalTemperature = observation.getTemperatures()
                            .stream()
                            .reduce(0f, new BinaryOperator<Float>() {
                                @Override
                                public Float apply(Float aFloat, Float aFloat2) {
                                    return null;
                                }
                            });

                    float totalPressure = observation.getPressures()
                            .stream()
                            .reduce(0f, Float::sum);

                    float totalWindSpeed = observation.getWindSpeeds()
                            .stream()
                            .reduce(0f, Float::sum);

                    return new Forecast(
                            totalTemperature / observation.getCount(),
                            totalPressure / observation.getCount(),
                            totalWindSpeed / observation.getCount()
                    );
                })
                .to(SINK_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Forecast.class)));

        return observers;
    }
}
