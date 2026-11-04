package com.cpdevelops.kafka_cdc.students_service;

import com.cpdevelops.kafka_cdc.students_service.config.CdcProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.cdc.retry.initial-interval=50ms",
        "app.cdc.retry.max-interval=100ms",
        "spring.kafka.streams.auto-startup=false"
})
@EmbeddedKafka(
        partitions = 1,
        topics = "edu.public.submissions",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class CdcPipelineIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CdcProperties properties;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Test
    void validChangeIsPublishedAsACleanEvent() throws Exception {
        try (Consumer<String, String> consumer = consumerFor(properties.eventsTopic())) {
            kafkaTemplate.send(properties.sourceTopic(), "{\"id\":501}", createdEvent(501, "88.5"))
                    .get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, String> event = pollForKey(consumer, "501", Duration.ofSeconds(20));

            assertThat(event.value())
                    .contains("\"changeType\":\"CREATED\"")
                    .contains("\"submissionId\":501");
        }
    }

    @Test
    void invalidScoreGoesToTheDeadLetterTopic() throws Exception {
        try (Consumer<String, String> consumer = consumerFor(properties.deadLetterTopic())) {
            kafkaTemplate.send(properties.sourceTopic(), "{\"id\":602}", createdEvent(602, "150"))
                    .get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, String> deadLetter =
                    pollForKey(consumer, "{\"id\":602}", Duration.ofSeconds(20));

            assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
                    .contains("SchemaValidationException");
            assertThat(header(deadLetter, KafkaHeaders.DLT_ORIGINAL_TOPIC))
                    .isEqualTo(properties.sourceTopic());
        }
    }

    private Consumer<String, String> consumerFor(String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static ConsumerRecord<String, String> pollForKey(Consumer<String, String> consumer,
                                                             String key, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                if (key.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record with key " + key + " within " + timeout);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String createdEvent(long id, String score) {
        return """
                {"before": null,
                 "after": {"id": %d, "student_id": "stu-it", "course_id": "CS101", "assignment_id": "hw-it",
                           "score": %s, "status": "GRADED", "submitted_at": "2026-11-01T10:00:00Z"},
                 "source": {"lsn": %d, "ts_ms": 1793560805104},
                 "op": "c"}
                """.formatted(id, score, 1000 + id);
    }
}