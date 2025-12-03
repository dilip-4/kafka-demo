package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;

import java.util.UUID;

@Component
public class ConsumerListener {

    @KafkaListener(topics = "${kafka.topics.input}", groupId = "demo-group")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            // process
            System.out.printf("Consumed: key=%s value=%s partition=%d offset=%d%n",
                    record.key(), record.value(), record.partition(), record.offset());

            // business logic here...
        } catch (Exception ex) {
            // don't ack -> message will be redelivered (or handled by error handler)
            //logger.error("Processing failed for offset {}: {}", record.offset(), ex.getMessage(), ex);
            // Optionally rethrow to let the error handler handle retry / DLQ
            throw ex;
        }
    }

    @KafkaListener(topics = "rebalance-demo", groupId = "rebalance-group")
    public void rebalanceTest(String msg, ConsumerRecord<String,String> record) {
        System.out.println("ConsumerInstance=" + UUID.randomUUID() +
                " Partition=" + record.partition() +
                " Offset=" + record.offset() +
                " Value=" + msg);
    }


}
