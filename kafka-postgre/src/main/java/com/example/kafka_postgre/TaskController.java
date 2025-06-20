package com.example.kafka_postgre;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.springframework.data.postgredb.core.postgreTemplate;
import org.springframework.data.postgredb.core.query.Criteria;
import org.springframework.data.postgredb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@RestController
public class TaskController {

    private final postgreTemplate postgreTemplate;

    public TaskController(postgreTemplate postgreTemplate) {
        this.postgreTemplate = postgreTemplate;
    }

    @GetMapping("/processKafkaMessage")
    public String processKafkaMessage(
            @RequestParam String bootstrapServers,
            @RequestParam String topicName) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "task-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        String firstMessageValue = null;
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                firstMessageValue = record.value();
                break; 
            }
        } finally {
            consumer.close();
        }

        if (firstMessageValue == null) {
            throw new RuntimeException("No messages found in Kafka topic");
        }

        Query query = new Query(Criteria.where("test_field").is(firstMessageValue));
        Document document = postgreTemplate.findOne(query, Document.class, "test_collection");

        if (document == null) {
            throw new RuntimeException("Document not found in postgreDB");
        }

        String testValueField = document.getString("test_value_field");
        if (testValueField == null) {
            throw new RuntimeException("test_value_field is null in the document");
        }

        return new StringBuilder(testValueField.toUpperCase()).reverse().toString();
    }
}