package com.kafka_stream.kafka_stream_demo.model;

import lombok.Data;

@Data
public class StreamRequest {

    private String employeeId;
    private String employeeName;
    private String location;
    private String team;
}
