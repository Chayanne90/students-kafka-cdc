# students-kafka-cdc

````
com.example.cdclab
├── config/
│   ├── CdcProperties.java              ← typed settings from application.yml
│   └── KafkaTopicConfig.java           ← creates the output topic
├── event/
│   ├── ChangeType.java                 ← CREATED / UPDATED / DELETED / SNAPSHOT
│   ├── DebeziumChangeEvent.java        ← the parsed raw Debezium envelope
│   ├── SubmissionEvent.java            ← the clean event we publish (our contract)
│   └── SubmissionEventMapper.java      ← converts raw → clean
├── kafka/
│   ├── consumer/SubmissionCdcListener.java     ← Kafka entry point (like a controller)
│   └── producer/SubmissionEventPublisher.java  ← sends events to Kafka
├── schema/
│   └── JsonSchemaValidator.java        ← validates JSON against schema files
├── service/
│   └── SubmissionCdcService.java       ← orchestrates the 5 processing steps
└── exception/
├── InvalidChangeEventException.java
├── SchemaValidationException.java
└── EventPublishingException.java

src/main/resources/schemas/
├── submission-row-v1.json              ← contract for the DATABASE row
└── submission-event-v1.json            ← contract for the PUBLISHED event
```