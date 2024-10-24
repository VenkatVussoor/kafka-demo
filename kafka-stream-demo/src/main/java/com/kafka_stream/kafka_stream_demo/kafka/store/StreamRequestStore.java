package com.kafka_stream.kafka_stream_demo.kafka.store;

import com.kafka_stream.kafka_stream_demo.model.StreamRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamRequestStore {

    @Value("${custom.kafka.streams.state.store.processing-topic}")
    private String processingTopic;

    private final StreamsBuilderFactoryBean streamsBuilderBean;

    public ReadOnlyKeyValueStore<String, StreamRequest> getKeyValueStore() {
        KafkaStreams kafkaStreams = streamsBuilderBean.getKafkaStreams();
        if (kafkaStreams != null) {
            return kafkaStreams
                    .store(StoreQueryParameters.fromNameAndType(processingTopic, QueryableStoreTypes.keyValueStore()));
        }
        return null;
    }

    public StreamRequest getStreamRequest(String employeeId) {
        return getKeyValueStore().get(employeeId);
    }


}
