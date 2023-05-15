package org.examples.branch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.stream.StreamSupport;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.examples.branch.MessageServiceStream.EMAIL_NOTIFICATION_TOPIC;
import static org.examples.branch.MessageServiceStream.MESSAGE_TOPIC;
import static org.examples.branch.MessageServiceStream.POST_NOTIFICATION_TOPIC;
import static org.examples.branch.MessageServiceStream.SMS_NOTIFICATION_TOPIC;
import static org.examples.branch.MessageType.EMAIL;
import static org.examples.branch.MessageType.POST;
import static org.examples.branch.MessageType.SMS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageServiceStreamITest extends BaseITest<Message, MessageNotification> {

    private static final String SENDER = "peterparker";
    private static final String RECIPIENT = "maryjane";
    private static final String SUBJECT_LINE = "Running late";
    private static final String MESSAGE = "Hi MJ, I'll swing by real soon, delayed at work... again!";

    @Test
    void emailMessageNotification() {
        // Arrange
        final Message emailMessage = new Message(EMAIL, SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);
        final MessageNotification expectedMessageNotification = new MessageNotification(SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);

        // Act
        publishRecord(new ProducerRecord<>(MESSAGE_TOPIC, PRODUCER_KEY, emailMessage));

        // Assert
        assertMessageNotificationReceived(EMAIL_NOTIFICATION_TOPIC, expectedMessageNotification);
    }

    @Test
    void postMessageNotification() {
        // Arrange
        final Message postMessage = new Message(POST, SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);
        final MessageNotification expectedMessageNotification = new MessageNotification(SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);

        // Act
        publishRecord(new ProducerRecord<>(MESSAGE_TOPIC, PRODUCER_KEY, postMessage));

        // Assert
        assertMessageNotificationReceived(POST_NOTIFICATION_TOPIC, expectedMessageNotification);
    }

    @Test
    void smsMessageNotification() {
        // Arrange
        final Message smsMessage = new Message(SMS, SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);
        final MessageNotification expectedMessageNotification = new MessageNotification(SENDER, RECIPIENT, SUBJECT_LINE, MESSAGE);

        // Act
        publishRecord(new ProducerRecord<>(MESSAGE_TOPIC, PRODUCER_KEY, smsMessage));

        // Assert
        assertMessageNotificationReceived(SMS_NOTIFICATION_TOPIC, expectedMessageNotification);
    }

    @Override
    protected Producer<String, Message> createProducer() {
        final Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getBootstrapServers());
        final DefaultKafkaProducerFactory<String, Message> producerFactory = new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new JsonSerializer<>()
        );

        return producerFactory.createProducer();
    }

    @Override
    protected Consumer<String, MessageNotification> createConsumer() {
        final Map<String, Object> configs = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), CONSUMER_GROUP, CONSUMER_AUTO_COMMIT);
        final DefaultKafkaConsumerFactory<String, MessageNotification> consumerFactory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new JsonDeserializer<>(MessageNotification.class)
        );

        return consumerFactory.createConsumer();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return List.of(EMAIL_NOTIFICATION_TOPIC, POST_NOTIFICATION_TOPIC, SMS_NOTIFICATION_TOPIC);
    }

    private void assertMessageNotificationReceived(String topic, MessageNotification expectedMessageNotification) {
        // Get all message notifications from all topics
        final ConsumerRecords<String, MessageNotification> allMessageNotifications = KafkaTestUtils.getRecords(consumer);

        // Get all message notifications by topic
        final Iterable<ConsumerRecord<String, MessageNotification>> messageNotificationsByTopic = allMessageNotifications.records(topic);

        // Convert to a list
        final List<MessageNotification> messageNotifications = StreamSupport.stream(
                        spliteratorUnknownSize(messageNotificationsByTopic.iterator(), ORDERED), false)
                .map(ConsumerRecord::value)
                .toList();

        // Ensure that the correct message notification was sent
        assertEquals(List.of(expectedMessageNotification), messageNotifications);
    }
}
