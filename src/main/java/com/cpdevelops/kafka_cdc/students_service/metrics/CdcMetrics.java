package com.cpdevelops.kafka_cdc.students_service.metrics;


import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;


@Component
public class CdcMetrics {

    private final MeterRegistry registry;
    private final Timer endToEndLatency;
    private final Timer processingTime;

    public CdcMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.endToEndLatency = Timer.builder("cdc.end_to_end.latency")
                .description("Time from the database commit to the clean event being published")
                .publishPercentileHistogram()
                .register(registry);

        this.processingTime = Timer.builder("cdc.processing.time")
                .description("Time the consumer spends processing one change event")
                .publishPercentileHistogram()
                .register(registry);
    }

    /* Call when processing starts, pass the result to recordPublisher*/
    public Timer.Sample startProcessing() {
        return Timer.start(registry);
    }

    //  Call after the clean event was published and confirmed
    public void recordPublished(SubmissionEvent event, Timer.Sample sample) {
        sample.stop(processingTime);

        Duration latency = Duration.between(event.occurredAt(), Instant.now());
        if (!latency.isNegative()){
            endToEndLatency.record(latency);
        }

        Counter.builder("cdc.event.processed")
                .description("Clean events published")
                .tag("change_type", event.changeType().name())
                .register(registry)
                .increment();
    }

    // Call on every failed processing attempt (retries included)
    public void recordFailedAttempt(String exceptionName) {
        Counter.builder("cdc.processing.failures")
                .description("Failed procesing attemps")
                .tag("exception", exceptionName)
                .register(registry)
                .increment();
    }

    // Call when a record arrives in the dead-letter topic
    public void recordDeadLetter(String causeName) {
        Counter.builder("cdc.events.dead_lettered")
                .description("Records sent to the dead-letter topic")
                .tag("cause", causeName)
                .register(registry)
                .increment();
    }

}
