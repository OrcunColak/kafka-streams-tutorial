package org.colak.statestore.local;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

class StateQueryService {

    private final ReadOnlyKeyValueStore<String, Long> keyValueStore;

    public StateQueryService(KafkaStreams streams, String storeName) {
        keyValueStore = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
        );
    }

    public Long getCount(String key) {
        return keyValueStore.get(key);
    }
}

