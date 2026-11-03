package com.cpdevelops.kafka_cdc.students_service.service;

import com.cpdevelops.kafka_cdc.students_service.config.AnalyticsProperties;
import com.cpdevelops.kafka_cdc.students_service.exceptions.AnalyticsUnavailableException;
import com.cpdevelops.kafka_cdc.students_service.streams.CourseAnalyticsTopology;
import com.cpdevelops.kafka_cdc.students_service.streams.model.CourseActivity;
import com.cpdevelops.kafka_cdc.students_service.streams.model.CourseGradeStats;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class CourseAnalyticsQueryService {

    private final StreamsBuilderFactoryBean streamsFactory;
    private final AnalyticsProperties analytics;

    public CourseAnalyticsQueryService(StreamsBuilderFactoryBean streamsFactory,
                                       AnalyticsProperties analytics) {
        this.streamsFactory = streamsFactory;
        this.analytics = analytics;
    }

    public Optional<CourseGradeStats> gradeStats(String courseId) {
        ReadOnlyKeyValueStore<String, CourseGradeStats> store =
                store(CourseAnalyticsTopology.GRADE_STATS_STORE, QueryableStoreTypes.keyValueStore());
        return Optional.ofNullable(store.get(courseId));
    }

    public List<CourseGradeStats> allGradeStats() {
        ReadOnlyKeyValueStore<String, CourseGradeStats> store =
                store(CourseAnalyticsTopology.GRADE_STATS_STORE, QueryableStoreTypes.keyValueStore());
        List<CourseGradeStats> result = new ArrayList<>();
        try (KeyValueIterator<String, CourseGradeStats> iterator = store.all()) {
            iterator.forEachRemaining(entry -> result.add(entry.value));
        }
        return result;
    }

    public List<CourseActivity> recentActivity(String courseId, Duration lookback) {
        ReadOnlyWindowStore<String, Long> store =
                store(CourseAnalyticsTopology.ACTIVITY_STORE, QueryableStoreTypes.windowStore());
        Instant to = Instant.now();
        Instant from = to.minus(lookback);

        List<CourseActivity> result = new ArrayList<>();
        try (WindowStoreIterator<Long> iterator = store.fetch(courseId, from, to)) {
            iterator.forEachRemaining(entry -> {
                Instant windowStart = Instant.ofEpochMilli(entry.key);
                result.add(new CourseActivity(courseId, windowStart,
                        windowStart.plus(analytics.activityWindow()), entry.value));
            });
        }
        return result;
    }

    public String describeTopology() {
        return streamsFactory.getTopology() == null
                ? "Topology not built yet"
                : streamsFactory.getTopology().describe().toString();
    }

    private <T> T store(String storeName, QueryableStoreType<T> storeType) {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new AnalyticsUnavailableException("Kafka Streams is not running (state: "
                    + (streams == null ? "NOT_STARTED" : streams.state()) + ")");
        }
        try {
            return streams.store(StoreQueryParameters.fromNameAndType(storeName, storeType));
        } catch (InvalidStateStoreException e) {
            throw new AnalyticsUnavailableException("State store '" + storeName + "' is not queryable yet", e);
        }
    }
}