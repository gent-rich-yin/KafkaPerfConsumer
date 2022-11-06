package com.example.kafka.perf;

import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(originPatterns = {"localhost", "http://kafka-perf-test.s3-website-us-east-1.amazonaws.com/"})
public class PerfServices {
    @GetMapping("topic")
    public String getTopic() {
        return PerfStates.topic;
    }

    @GetMapping("perfMessage")
    public String getPerfMessage() {
        return PerfStates.perfMessage;
    }

    @PostMapping("topic")
    public void setTopic(@RequestBody String topic) {
        PerfStates.topic = topic;
    }
}
