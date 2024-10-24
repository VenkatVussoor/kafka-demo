package com.kafka_stream.kafka_stream_demo.kafka.producer;

import com.kafka_stream.kafka_stream_demo.model.StreamRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@EnableKafka
@Service
@Slf4j
@RequiredArgsConstructor
public class StreamProducer {

    @Autowired
    KafkaTemplate<String, StreamRequest> kafkaTemplate;

    private String topicPrefix = "demo-";

    private String mainTopic = "input-topic";


    public void sendMessage(StreamRequest streamRequest) {
        log.info("Sending message {} to {}", streamRequest.getEmployeeId(), topicPrefix + mainTopic);
        kafkaTemplate.send(topicPrefix+mainTopic, streamRequest.getEmployeeId(), streamRequest);
    }
}
