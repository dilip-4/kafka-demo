package com.example.kafkademo.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class StreamsProcessor {

    @Value("${kafka.topics.input}")
    private String inputTopic;

    @Value("${kafka.topics.output}")
    private String outputTopic;

    @Bean
    public Topology kStreamTopology(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> counts = stream
                .flatMapValues(v -> java.util.Arrays.asList(v.toLowerCase().split("\\W+")))
                .groupBy((k, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counts-store"));

        counts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
