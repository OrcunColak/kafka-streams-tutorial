package org.colak.statestore.local;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

// See https://medium.com/datapebbles/kafka-state-store-walkthrough-e4556c73be11
@Slf4j
class LocalStoreTest {

    private static final String INPUT_TOPIC_NAME = "demo_topic";
    private static final String STORE_NAME = "counts-store";

    public static void main(String[] args) {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo_topic-count");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build a streaming pipeline
        StreamsBuilder builder = new StreamsBuilder();

        // Source
        KStream<String, String> kStream = builder.stream(INPUT_TOPIC_NAME);

        // Step 4: Perform the count aggregation and materialize it in a KeyValueStore
        KTable<String, Long> countTable = kStream
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE_NAME));

        // Step 5: Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        StateQueryService stateQueryService = new StateQueryService(streams,STORE_NAME);

        // Access the counts for a specific key
        String key = "someKey";
        Long count = stateQueryService.getCount(key);
        System.out.println("Count for key " + key + ": " + count);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
