package com.cpdevelops.kafka_cdc.students_service.streams;

import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class OccurredAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if(record.value() instanceof SubmissionEvent event && event.occurredAt() != null) {
            return event.occurredAt().toEpochMilli();
        }
        return record.timestamp();
    }
}
