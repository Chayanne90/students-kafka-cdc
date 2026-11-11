package com.cpdevelops.kafka_cdc.students_service.kafka.consumer;

import com.cpdevelops.kafka_cdc.students_service.metrics.CdcMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Component
public class DeadLetterInspector {

    private final CdcMetrics metrics;


    private static final Logger log = LoggerFactory.getLogger(DeadLetterInspector.class);

    public DeadLetterInspector(CdcMetrics metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(
            id = "dead-letter-inspector",
            topics = "${app.cdc.dead-letter-topic}",
            groupId = "dead-letter-inspector")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        try {

            String cause = text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
            metrics.recordDeadLetter(cause.substring(cause.lastIndexOf('.') + 1));
            log.error("""
                    DEAD LETTER received
                      key:       {}
                      original:  {}-{}@{}
                      exception: {}
                      cause:     {}
                      message:   {}""",
                    record.key(),
                    text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                    number(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                    number(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                    text(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                    text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN),
                    text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
        } catch (RuntimeException e) {
            log.error("Could not inspect dead letter at offset {}", record.offset(), e);
        }
    }

    private static String text(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? "n/a" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String number(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) {
            return "n/a";
        }
        ByteBuffer buffer = ByteBuffer.wrap(header.value());
        return switch (header.value().length) {
            case Integer.BYTES -> String.valueOf(buffer.getInt());
            case Long.BYTES -> String.valueOf(buffer.getLong());
            default -> "n/a";
        };
    }
}