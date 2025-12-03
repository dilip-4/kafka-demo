package com.example.kafkademo.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/produce")
public class ProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String inputTopic;

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate,
                              @Value("${kafka.topics.input}") String inputTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.inputTopic = inputTopic;
    }

    @PostMapping
    public ResponseEntity<String> produce(@RequestParam(required = false) String key,
                                          @RequestBody String payload) {
        kafkaTemplate.send(inputTopic, key, payload);
        return ResponseEntity.ok("sent");
    }
}
