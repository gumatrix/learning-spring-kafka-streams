package org.examples.headerenricher;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.streams.HeaderEnricherProcessor;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

@Configuration
public class ProductProcessorStream {

    static final String SOURCE_TOPIC = "plain-product-topic";
    static final String SINK_TOPIC = "enriched-product-topic";

    @Bean
    public NewTopic plainProductTopic() {
        return TopicBuilder.name(SOURCE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic enrichedProductTopic() {
        return TopicBuilder.name(SINK_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public KStream<String, Product> productsKStream(StreamsBuilder builder) {
        final Serde<String> keySerde = Serdes.String();
        final JsonSerde<Product> valueSerde = new JsonSerde<>(Product.class);
        final SpelExpressionParser parser = new SpelExpressionParser();

        final KStream<String, Product> products = builder.stream(SOURCE_TOPIC, Consumed.with(keySerde, valueSerde));

        products.process(() -> new HeaderEnricherProcessor<>(
                        Map.of("ProductType", parser.parseExpression("record.value().type().name()"))
                ))
                .to(SINK_TOPIC, Produced.with(keySerde, valueSerde));

        return products;
    }
}
