package com.cpdevelops.kafka_cdc.students_service.kafka.consumer;

import com.cpdevelops.kafka_cdc.students_service.kafka.producer.SubmissionEventPublisher;
import com.cpdevelops.kafka_cdc.students_service.service.SubmissionCdcService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SubmissionCdcListener {

    private static final Logger log = LoggerFactory.getLogger(SubmissionEventPublisher.class);
     private final SubmissionCdcService service;

    public SubmissionCdcListener(SubmissionCdcService service) {
        this.service = service;
    }

    @KafkaListener(id="submission-cdc-listener", topics="${app.cdc.source-topic}", groupId = "submission-cdc-consumer")
    public void onMessage(ConsumerRecord<String, String> record) {

        log.info("Received change event key={} partition={} offset={}",
                record.key(),record.partition(),record.offset());

        service.process(record.key(), record.value());
    }

}
