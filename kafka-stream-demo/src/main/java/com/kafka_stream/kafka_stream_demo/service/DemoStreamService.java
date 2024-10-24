package com.kafka_stream.kafka_stream_demo.service;

import com.kafka_stream.kafka_stream_demo.model.StreamRequest;
import com.kafka_stream.kafka_stream_demo.utils.JsonPOJODeserializer;
import com.kafka_stream.kafka_stream_demo.utils.JsonPOJOSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@EnableKafka
@EnableKafkaStreams
public class DemoStreamService {


    private String topicPrefix = "demo-";

    @Value("${custom.kafka.streams.topic.input-topic}")
    private String inputTopic;

    @Value("${custom.kafka.streams.topic.processing-topic}")
    private String processedTopic;


    @Bean
    public KStream processStream(StreamsBuilder streamsBuilder) {

        KStream<String, StreamRequest> stream = streamsBuilder
                .stream(topicPrefix + inputTopic, Consumed.with(Serdes.String(), getSreamRequestSerde()));

        stream.filter((k,v) -> v.getLocation().equalsIgnoreCase("pune") ||
                v.getLocation().equalsIgnoreCase("gift")).mapValues((k,v) -> {v.setTeam("CXP");
                    return v;
                });

        stream.filter((k,v) -> v.getLocation().equalsIgnoreCase("Bangalore"))
                .mapValues((k,v) -> {v.setTeam("Valtech Global");
            return v;
        });

        // Write the results to the output topic
        stream.to(topicPrefix + processedTopic, Produced.with(Serdes.String(), getSreamRequestSerde()));

        return stream;
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
