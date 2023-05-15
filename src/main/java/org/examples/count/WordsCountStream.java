package org.examples.count;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.List;

@Configuration(proxyBeanMethods = false)
class WordsCountStream {

    static final String SOURCE_TOPIC = "text-source";
    static final String SINK_TOPIC = "words-count-sink";

    @Bean
    public NewTopic textTopic() {
        return TopicBuilder.name(SOURCE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic wordsCountTopic() {
        return TopicBuilder.name(SINK_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public KStream<String, String> wordsCountKStream(StreamsBuilder builder) {
        final KStream<String, String> text = builder
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, Long> wordsCount = text
                .flatMapValues((key, value) -> List.of(value.split("\\W+")))
                .groupBy((key, value) -> value)
                .count();

        wordsCount.toStream()
                .to(SINK_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return text;
    }
}
