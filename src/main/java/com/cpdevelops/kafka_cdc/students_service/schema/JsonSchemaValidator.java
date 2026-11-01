package com.cpdevelops.kafka_cdc.students_service.schema;

import com.cpdevelops.kafka_cdc.students_service.exception.SchemaValidationException;
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class JsonSchemaValidator {

    private final SchemaRegistry registry =
            SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);


    /* regular hashmap can't handle multiple threads, so we use ConcurrentHashMap */
    private final Map<String, Schema> cache = new ConcurrentHashMap<>();
    private final ResourceLoader resourceLoader;

    public JsonSchemaValidator(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    public void validate(String schemaName, String json) {
        Schema schema = cache.computeIfAbsent(schemaName, this::load);

        List<Error> errors = schema.validate(json, InputFormat.JSON, executionContext ->
                executionContext.executionConfig(config -> config.formatAssertionsEnabled(true)));

        if (!errors.isEmpty()) {
            String detials = errors.stream()
                    .map(error -> error.getInstanceLocation() + ":" + error.getMessage())
                    .collect(Collectors.joining("; "));
            throw new SchemaValidationException(schemaName + " ->" +detials);
        }
    }

    private Schema load(String schemaName) {
        Resource resource = resourceLoader.getResource("classpath:schemas/" + schemaName + ".json");
        try (InputStream in = resource.getInputStream()) {
            String schemaText = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return registry.getSchema(schemaText, InputFormat.JSON);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load JSON Schema '" + schemaName + "'", e);
        }
    }
}
