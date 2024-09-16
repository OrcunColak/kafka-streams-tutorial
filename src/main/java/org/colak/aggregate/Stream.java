package org.colak.aggregate;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

@Slf4j
class Stream {

    private static final String TOPIC_NAME = "demo_topic";

    public static void main(String[] args) {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-topic-aggregate");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build a streaming pipeline
        StreamsBuilder builder = new StreamsBuilder();

        // KStream and KTable can be thought of as helper classes within StreamsBuilder to perform the transformations.
        // Kstream is used on real time event processing and is stateless (can be useful for windowed grouping in a span of time interval).
        // KTable on other hand operates on table of data performing counts, aggregations based on key, making it suitable for batch operations.
        // Source
        KStream<String, String> kStream = builder.stream(TOPIC_NAME);

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> kTable = kStream
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .aggregate(
                        () -> 0L,
                        (key, event, aggregate) -> aggregate + event.length(),
                        Materialized.as("demo-topic-split-aggregate")
                );


        kTable.toStream().to("demo-topic-split-aggregate-topic", Produced.with(Serdes.String(), Serdes.Long()));


        // The KafkaStreams class is used to start and manage the execution of the Kafka Streams application.
        // It takes the topology defined using StreamsBuilder and runs it on a Kafka cluster.
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();
    }

}
