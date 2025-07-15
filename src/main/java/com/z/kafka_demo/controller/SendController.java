package com.z.kafka_demo.controller;

import com.z.kafka_demo.producer.ReliableProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/send")
@RequiredArgsConstructor
public class SendController {

    private final ReliableProducer producer;

    @PostMapping
    public ResponseEntity<Void> send(@RequestBody String payload) {
        producer.send(payload);
        return ResponseEntity.accepted().build();
    }
}
