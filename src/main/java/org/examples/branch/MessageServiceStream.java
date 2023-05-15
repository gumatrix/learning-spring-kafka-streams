package org.examples.branch;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.function.Consumer;

@Configuration
public class MessageServiceStream {

    static final String MESSAGE_TOPIC = "message-source";
    static final String EMAIL_NOTIFICATION_TOPIC = "email-notification-sink";
    static final String POST_NOTIFICATION_TOPIC = "post-notification-sink";
    static final String SMS_NOTIFICATION_TOPIC = "sms-notification-sink";
    static final String UNKNOWN_MESSAGE_TYPE_TOPIC = "unknown-message-type-sink";

    @Bean
    public NewTopic messageTopic() {
        return TopicBuilder.name(MESSAGE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic emailNotificationTopic() {
        return TopicBuilder.name(EMAIL_NOTIFICATION_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic postNotificationTopic() {
        return TopicBuilder.name(POST_NOTIFICATION_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic smsNotificationTopic() {
        return TopicBuilder.name(SMS_NOTIFICATION_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic unknownMessageTypeTopic() {
        return TopicBuilder.name(UNKNOWN_MESSAGE_TYPE_TOPIC)
                .partitions(1)
                .compact()
                .build();
    }

    @Bean
    public KStream<String, Message> messageNotifcationsKStream(StreamsBuilder builder) {
        return new KafkaStreamBrancher<String, Message>()
                .branch((key, value) -> MessageType.EMAIL == value.type(), handleMessageNotification(EMAIL_NOTIFICATION_TOPIC))
                .branch((key, value) -> MessageType.POST == value.type(), handleMessageNotification(POST_NOTIFICATION_TOPIC))
                .branch((key, value) -> MessageType.SMS == value.type(), handleMessageNotification(SMS_NOTIFICATION_TOPIC))
                .defaultBranch(ks -> ks.to(UNKNOWN_MESSAGE_TYPE_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Message.class))))
                .onTopOf(builder.stream(MESSAGE_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Message.class))));
    }

    private Consumer<KStream<String, Message>> handleMessageNotification(String destinationTopic) {
        return messageKStream -> messageKStream
                .mapValues(this::buildMessageNotification)
                .to(destinationTopic, Produced.with(Serdes.String(), new JsonSerde<>(MessageNotification.class)));
    }

    private MessageNotification buildMessageNotification(Message message) {
        return new MessageNotification(message.from(), message.to(), message.subjectLine(), message.message());
    }
}
