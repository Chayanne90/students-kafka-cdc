package com.cpdevelops.kafka_cdc.students_service.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.json.JsonMapper;

public class JsonSerdes {

    private JsonSerdes() {
    }

    public static <T> Serde<T> of(JsonMapper jsonMapper, Class<T> type) {
        Serializer<T> serializer = (topic, data) ->
                data == null ? null : jsonMapper.writeValueAsBytes(data);
        Deserializer<T> deserializer = (topic, bytes) ->
                bytes == null ? null : jsonMapper.readValue(bytes, type);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
