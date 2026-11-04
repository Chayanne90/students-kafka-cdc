package com.cpdevelops.kafka_cdc.students_service;

import com.cpdevelops.kafka_cdc.students_service.config.AnalyticsProperties;
import com.cpdevelops.kafka_cdc.students_service.event.ChangeType;
import com.cpdevelops.kafka_cdc.students_service.event.SubmissionEvent;
import com.cpdevelops.kafka_cdc.students_service.streams.CourseAnalyticsTopology;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CourseAnalyticsTopologyTest {

    private static final String EVENTS = "edu.submission-events";
    private static final String ACTIVITY = "edu.course-activity";
    private static final String STATS = "edu.course-grade-stats";
    private static final Instant T0 = Instant.parse("2026-11-01T10:00:05Z");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> events;
    private TestOutputTopic<String, String> activity;
    private TestOutputTopic<String, String> stats;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AnalyticsProperties analytics =
                new AnalyticsProperties(ACTIVITY, STATS, Duration.ofMinutes(1), Duration.ofSeconds(30));
        CourseAnalyticsTopology.build(builder, jsonMapper, EVENTS, analytics);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        events = driver.createInputTopic(EVENTS, new StringSerializer(), new StringSerializer());
        activity = driver.createOutputTopic(ACTIVITY, new StringDeserializer(), new StringDeserializer());
        stats = driver.createOutputTopic(STATS, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void averageStaysCorrectThroughGradingRegradingAndDeletion() {
        send(event(1, ChangeType.CREATED, null, T0));
        send(event(1, ChangeType.UPDATED, "80", T0.plusSeconds(10)));
        send(event(2, ChangeType.CREATED, null, T0.plusSeconds(20)));
        send(event(2, ChangeType.UPDATED, "90", T0.plusSeconds(30)));

        JsonNode afterGrading = latestStats("CS101");
        assertThat(afterGrading.get("gradedCount").asLong()).isEqualTo(2);
        assertThat(afterGrading.get("averageScore").asDouble()).isEqualTo(85.0);

        send(event(1, ChangeType.UPDATED, "100", T0.plusSeconds(40)));   // re-grade 80 -> 100

        JsonNode afterRegrade = latestStats("CS101");
        assertThat(afterRegrade.get("gradedCount").asLong()).isEqualTo(2);
        assertThat(afterRegrade.get("averageScore").asDouble()).isEqualTo(95.0);

        send(event(2, ChangeType.DElETED, "90", T0.plusSeconds(50)));    // delete the 90

        JsonNode afterDelete = latestStats("CS101");
        assertThat(afterDelete.get("gradedCount").asLong()).isEqualTo(1);
        assertThat(afterDelete.get("averageScore").asDouble()).isEqualTo(100.0);
    }

    @Test
    void countsNewSubmissionsPerCoursePerMinuteUsingEventTime() {
        send(event(10, ChangeType.CREATED, null, T0));                    // 10:00:05
        send(event(11, ChangeType.CREATED, null, T0.plusSeconds(20)));    // 10:00:25
        send(event(12, ChangeType.CREATED, null, T0.plusSeconds(70)));    // 10:01:15, next window
        send(event(10, ChangeType.UPDATED, "50", T0.plusSeconds(75)));    // not a new submission

        Map<String, Long> latestCountByWindowStart = new HashMap<>();
        for (KeyValue<String, String> record : activity.readKeyValuesToList()) {
            JsonNode node = jsonMapper.readTree(record.value);
            latestCountByWindowStart.put(node.get("windowStart").asString(), node.get("submission").asLong());
        }

        assertThat(latestCountByWindowStart)
                .containsEntry("2026-11-01T10:00:00Z", 2L)
                .containsEntry("2026-11-01T10:01:00Z", 1L);
    }

    private void send(SubmissionEvent event) {
        events.pipeInput(String.valueOf(event.submissionId()), jsonMapper.writeValueAsString(event));
    }

    private JsonNode latestStats(String courseId) {
        Map<String, String> latestByCourse = stats.readKeyValuesToMap();
        return jsonMapper.readTree(latestByCourse.get(courseId));
    }

    private static SubmissionEvent event(long id, ChangeType type, String score, Instant occurredAt) {
        return new SubmissionEvent(
                id + "-" + occurredAt.toEpochMilli() + "-" + type,
                1,
                type,
                id,
                "stu-" + id,
                "CS101",
                "hw-1",
                score == null ? null : new BigDecimal(score),
                score == null ? "SUBMITTED" : "GRADED",
                T0,
                occurredAt,
                null);
    }
}