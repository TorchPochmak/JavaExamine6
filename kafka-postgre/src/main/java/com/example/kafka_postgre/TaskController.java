package com.example.kafka_postgre;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;

@RestController
public class TaskController {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public TaskController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/countWords")
    public int countWords(
            @RequestParam String bootstrapServers,
            @RequestParam String topicName) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        List<String> messages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            boolean moreRecords = true;
            while (moreRecords) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    moreRecords = false;
                } else {
                    for (ConsumerRecord<String, String> record : records) {
                        messages.add(record.value());
                    }
                }
            }
        }

        if (messages.isEmpty()) {
            throw new RuntimeException("No messages found in Kafka topic");
        }

        int totalWords = 0;
        // Ищем все строки, содержащие слово 'test'
        List<String> foundRows = jdbcTemplate.query(
            "SELECT some_column FROM some_table WHERE some_column LIKE ?",
            (rs, rowNum) -> rs.getString(1),
            "%test%"
        );
        for (String row : foundRows) {
            totalWords += Arrays.stream(row.split(" ")).filter(s -> !s.isEmpty()).count();
        }
        return totalWords;
    }
}