package com.kafka_stream.kafka_stream_demo.controller;

import com.kafka_stream.kafka_stream_demo.kafka.producer.StreamProducer;
import com.kafka_stream.kafka_stream_demo.kafka.store.StreamRequestStore;
import com.kafka_stream.kafka_stream_demo.model.StreamRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class StreamController {

    @Autowired
    private StreamProducer demoProducer;

    @Autowired
    private StreamRequestStore store;

    @PostMapping("/process-stream")
    public String performStreamOperation(@RequestBody StreamRequest streamRequest){

        demoProducer.sendMessage(streamRequest);
        return streamRequest.getEmployeeName()+" has been registered Successfully";

    }

    @GetMapping("/getemployee/{employeeId}")
    public StreamRequest getEmployeeDetails(@PathVariable String employeeId){
        StreamRequest streamRequest = store.getStreamRequest(employeeId);
        return streamRequest;
    }
}
