package com.kafka_stream.kafka_stream_demo.kafka.config;

import com.kafka_stream.kafka_stream_demo.model.StreamRequest;
import com.kafka_stream.kafka_stream_demo.utils.JsonPOJODeserializer;
import com.kafka_stream.kafka_stream_demo.utils.JsonPOJOSerializer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class StreamRequestConfig {

    @Value("${custom.kafka.streams.state.store.processing-topic}")
    @Setter
    private String streamRequestStore;

    @Value("${custom.kafka.streams.topic.processing-topic}")
    @Setter
    private String streamProcessingTopic;


    private String topicPrefix="demo-";



    @Bean
    public KTable<String, StreamRequest> streamRequestTable(final StreamsBuilder kStreamBuilder) {
        return  kStreamBuilder.<String, StreamRequest>stream(
                topicPrefix + streamProcessingTopic,
                Consumed.with(Serdes.String(),
                              getSreamRequestSerde())).toTable(Materialized.<String, StreamRequest, KeyValueStore<Bytes, byte[]>>as(streamRequestStore).withKeySerde(Serdes.String()).withValueSerde(getSreamRequestSerde()));
    }

    private Serde<StreamRequest> getSreamRequestSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<StreamRequest> streamRequestSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", StreamRequest.class);
        streamRequestSerializer.configure(serdeProps, false);

        final Deserializer<StreamRequest> streamRequestDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", StreamRequest.class);
        streamRequestDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(streamRequestSerializer, streamRequestDeserializer);
    }
}
