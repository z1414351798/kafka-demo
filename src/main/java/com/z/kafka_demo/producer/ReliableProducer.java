package com.z.kafka_demo.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class ReliableProducer {

    private static final String TOPIC = "my-topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String payload) {
        String messageId = UUID.randomUUID().toString();

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, messageId, payload);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata meta = result.getRecordMetadata();
                System.out.printf("✅ sent id=%s to %s-%d@offset %d%n",
                        messageId, meta.topic(), meta.partition(), meta.offset());
            } else {
                System.err.printf("❌ failed id=%s : %s%n", messageId, ex.getMessage());
            }
        });
    }
}
