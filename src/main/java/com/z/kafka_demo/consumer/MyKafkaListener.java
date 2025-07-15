package com.z.kafka_demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MyKafkaListener {

    // Simulated processed message cache (use Redis or DB in production)
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "my-topic", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String messageId = record.key(); // Assuming the key is a unique ID
        String message = record.value();

        try {
            if (processedMessageIds.contains(messageId)) {
                System.out.println("Duplicate message skipped: " + messageId);
                ack.acknowledge();
                return;
            }

            // ✅ Business logic
            System.out.println("Processing message: " + messageId + " -> " + message);

            // Add to processed set
            processedMessageIds.add(messageId);

            // ✅ Acknowledge offset after successful processing
            ack.acknowledge();

        } catch (Exception e) {
            // ❌ Message will be retried unless offset is manually committed
            System.err.println("Error processing message: " + e.getMessage());
        }
    }
}
