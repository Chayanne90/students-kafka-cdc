package com.cpdevelops.kafka_cdc.students_service.service;

import com.cpdevelops.kafka_cdc.students_service.config.CdcProperties;
import com.cpdevelops.kafka_cdc.students_service.event.DebeziumChangeEvent;
import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEventMapper;
import com.cpdevelops.kafka_cdc.students_service.exceptions.InvalidChangeEventException;
import com.cpdevelops.kafka_cdc.students_service.kafka.producer.SubmissionEventPublisher;
import com.cpdevelops.kafka_cdc.students_service.schema.JsonSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Service
public class SubmissionCdcService {

    private static final Logger log = LoggerFactory.getLogger(SubmissionCdcService.class);

    private final JsonMapper jsonMapper;
    private final JsonSchemaValidator schemaValidator;
    private final SubmissionEventMapper eventMapper;
    private final SubmissionEventPublisher publisher;
    private final CdcProperties properties;

    public SubmissionCdcService(JsonMapper jsonMapper, JsonSchemaValidator schemaValidator,
                                SubmissionEventMapper eventMapper, SubmissionEventPublisher publisher,
                                CdcProperties properties) {
        this.jsonMapper = jsonMapper;
        this.schemaValidator = schemaValidator;
        this.eventMapper = eventMapper;
        this.publisher = publisher;
        this.properties = properties;
    }


    public void process(String key, String value) {

        // if a null value showsup, then nothing to process...
        if (value == null) {
            log.debug("No vlaue for key: " + key);
            return;
        }

        // 1. parse the raw Debesium Envelop
        DebeziumChangeEvent change = DebeziumChangeEvent.from(parse(value));
        JsonNode row = change.rowState();

        // 2. Validate the database row against the row contract
        schemaValidator.validate(properties.rowSchema(), jsonMapper.writeValueAsString(row));

        // 3. Map the raw row to our clean event
        SubmissionEvent event = eventMapper.toEvent(change, row);

        // 4. Validate our own output against the event contract
        schemaValidator.validate("submission-event-v" + event.schemaVersion(), jsonMapper.writeValueAsString(event));

        // 5. Publish and wait for Kafka to confirm
        publisher.publish(event);
    }

    private JsonNode parse(String value) {
        try {
            return jsonMapper.readTree(value);
        } catch (JacksonException e) {
            throw new InvalidChangeEventException("Message is not valid JSON: " + e.getMessage(), e);
        }
    }

}
