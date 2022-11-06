package com.example.kafka.perf;

import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(originPatterns = "*")
public class PerfServices {
    @GetMapping("topic")
    public String getTopic() {
        return PerfStates.topic;
    }

    @GetMapping("perfMessage")
    public String getPerfMessage() {
        return PerfStates.perfMessage == null ? "" : PerfStates.perfMessage;
    }

    @PostMapping("topic")
    public void setTopic(@RequestBody(required=false) String topic) {
        PerfStates.topic = topic;
    }

}
