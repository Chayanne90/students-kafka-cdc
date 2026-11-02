package com.cpdevelops.kafka_cdc.students_service.kafka.producer;

import com.cpdevelops.kafka_cdc.students_service.config.CdcProperties;
import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import com.cpdevelops.kafka_cdc.students_service.exceptions.EventPublishingExecption;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Component
public class SubmissionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SubmissionEventPublisher.class);
    private final KafkaTemplate<String,String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final String topic;

    public SubmissionEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                    JsonMapper jsonMapper,
                                    CdcProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.topic = properties.eventsTopic();
    }

    public  void publish(SubmissionEvent event) {
        String key = String.valueOf(event.submissionId());
        String value = jsonMapper.writeValueAsString(event);

        ProducerRecord <String, String> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("schema-version", bytes(String.valueOf(event.schemaVersion())));
        record.headers().add("change-type", bytes(event.changeType().name()));

        try {
            SendResult<String, String> result = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
            log.info("Published {} V{} for submission {} -> {} {} @{}",
                    event.changeType(),event.schemaVersion(),event.submissionId(),
                    topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishingExecption("Interrupted while publishing " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishingExecption("Kafka did not confirm event " + event.eventId(), e);
        }

    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
