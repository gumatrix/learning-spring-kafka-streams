package org.examples.aggregate;

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

@Configuration(proxyBeanMethods = false)
public class OrdersTotalStream {

    static final String SOURCE_TOPIC = "orders-source";
    static final String SINK_TOPIC = "orders-total-sink";

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(SOURCE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic ordersTotalTopic() {
        return TopicBuilder.name(SINK_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public KStream<String, Double> ordersTotalKStream(StreamsBuilder builder) {
        final KStream<String, Double> orders = builder
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.Double()));

        final KTable<String, Double> ordersTotal = orders
                .groupByKey()
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value,
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("orders-total-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        ordersTotal.toStream()
                .to(SINK_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));

        return orders;
    }
}
