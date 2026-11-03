package com.cpdevelops.kafka_cdc.students_service.config;
import com.cpdevelops.kafka_cdc.students_service.exceptions.InvalidChangeEventException;
import com.cpdevelops.kafka_cdc.students_service.exceptions.SchemaValidationException;
import com.cpdevelops.kafka_cdc.students_service.metrics.CdcMetrics;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                 CdcProperties properties, CdcMetrics metrics) {

        // 1. Where failed records go: the dead-letter topic, partition chosen by Kafka from the key
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(properties.deadLetterTopic(), -1));

        Timer.Sample sample = metrics.startProcessing();

        // 2. How retries are spaced: exponential backoff with a retry limit
        CdcProperties.Retry retry = properties.retry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        // 3. Combine them
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // 4. Classify: these failures are permanent, so skip retries and go straight to the DLT
        errorHandler.addNotRetryableExceptions(
                InvalidChangeEventException.class,
                SchemaValidationException.class);

        // 5. Log every failed attempt, so retries are visible
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) -> {
            Throwable cause = NestedExceptionUtils.getMostSpecificCause(exception);
            metrics.recordFailedAttempt(cause.getClass().getSimpleName());
            log.warn("Attempt {} failed for {}-{}@{} key={}: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), record.key(),
                    cause.getMessage());
        });

        return errorHandler;
    }
}