package com.cpdevelops.kafka_cdc.students_service.service;

import com.cpdevelops.kafka_cdc.students_service.config.CdcProperties;
import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import com.cpdevelops.kafka_cdc.students_service.exceptions.TransientProcessingException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@Component
public class FailureSimulator {

    private final boolean enabled;
    private final Duration processingDelay;
    private final Map<String, AtomicInteger> attemptsByEventId = new ConcurrentHashMap<>();

    public FailureSimulator(CdcProperties properties) {
        this.enabled = properties.failureSimulation().enabled();
        this.processingDelay = properties.failureSimulation().processingDelay();
    }

    public void maybeDelay() {
        if (!enabled || processingDelay.isZero()) {
            return;
        }
        try {
            /*
            Thread.sleep(Duration) pauses the consumer thread
            * */
            Thread.sleep(processingDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientProcessingException("Interrupted during simulated processing delay");
        }
    }


    public void maybeFail(SubmissionEvent event) {
        if (!enabled) {
            return;
        }

        String studentId = event.studentId();

        if (studentId.startsWith("always-fail-")) {
            throw new TransientProcessingException("Simulated outage while processing " + studentId);
        }
        if (studentId.startsWith("flaky-")) {
            int attempt = attemptsByEventId
                    .computeIfAbsent(event.eventId(), id -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt <= 2) {
                throw new TransientProcessingException(
                        "Simulated temporary failure #" + attempt + " for " + studentId);
            }
        }
    }
}