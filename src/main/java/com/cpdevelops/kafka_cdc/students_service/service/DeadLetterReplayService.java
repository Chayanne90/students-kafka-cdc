package com.cpdevelops.kafka_cdc.students_service.service;

import com.cpdevelops.kafka_cdc.students_service.config.CdcProperties;
import com.cpdevelops.kafka_cdc.students_service.dto.responses.ReplayResult;
import com.cpdevelops.kafka_cdc.students_service.exceptions.EventPublishingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class DeadLetterReplayService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayService.class);

    static final String REPLAY_COUNT_HEADER = "x-replay-count";
    private static final int MAX_REPLAYS = 3;
    private static final String REPLAYER_GROUP = "dead-letter-replayer";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration MAX_RUN_TIME = Duration.ofSeconds(30);

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CdcProperties properties;

    public DeadLetterReplayService(ConsumerFactory<String, String> consumerFactory,
                                   KafkaTemplate<String, String> kafkaTemplate,
                                   CdcProperties properties) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public ReplayResult replayAll() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        int replayed = 0;
        int skipped = 0;
        int emptyPollsAfterAssignment = 0;
        Instant deadline = Instant.now().plus(MAX_RUN_TIME);

        try (Consumer<String, String> consumer =
                     consumerFactory.createConsumer(REPLAYER_GROUP, null, null, overrides)) {

            consumer.subscribe(List.of(properties.deadLetterTopic()));

            while (emptyPollsAfterAssignment < 3 && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

                if (records.isEmpty()) {
                    // Only count empty polls once partitions are assigned (joining the group takes a few seconds)
                    if (!consumer.assignment().isEmpty()) {
                        emptyPollsAfterAssignment++;
                    }
                    continue;
                }
                emptyPollsAfterAssignment = 0;

                for (ConsumerRecord<String, String> record : records) {
                    int previousReplays = replayCount(record);
                    if (previousReplays >= MAX_REPLAYS) {
                        log.warn("Skipping dead letter key={} offset={}: already replayed {} times",
                                record.key(), record.offset(), previousReplays);
                        skipped++;
                        continue;
                    }
                    republish(record, previousReplays + 1);
                    replayed++;
                }

                // Mark these dead letters as handled only after they were all sent back
                consumer.commitSync();
            }
        }

        log.info("Dead-letter replay finished: replayed={} skipped={}", replayed, skipped);
        return new ReplayResult(replayed, skipped);
    }

    private void republish(ConsumerRecord<String, String> deadLetter, int replayCount) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(properties.sourceTopic(), deadLetter.key(), deadLetter.value());
        record.headers().add(REPLAY_COUNT_HEADER,
                String.valueOf(replayCount).getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishingException("Interrupted while replaying key " + deadLetter.key(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishingException("Could not replay key " + deadLetter.key(), e);
        }
    }

    private static int replayCount(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(REPLAY_COUNT_HEADER);
        if (header == null) {
            return 0;
        }

        String headerValue = new String(header.value(), StandardCharsets.UTF_8);
        try {
            return Integer.parseInt(headerValue);
        } catch (NumberFormatException e) {
            log.warn("Invalid {} header value '{}' for key {}. Defaulting replay count to 0.",
                    REPLAY_COUNT_HEADER, headerValue, record.key());
            return 0;
        }
    }
}