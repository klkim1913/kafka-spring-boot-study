package com.study.kafka.simplekafka.controller;

import com.study.kafka.simplekafka.domain.PracticalAdvice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@RestController
@Slf4j
public class HelloKafkaController {
    private final KafkaTemplate<String, Object> template;
    private final String topicName;
    private final int messagesPerRequest;

    public HelloKafkaController(final KafkaTemplate<String, Object> template,
                                @Value("${study.topic-name}") final String topicName,
                                @Value("${study.messages-per-request}") final int messagesPerRequest) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
    }

    @GetMapping("/hello")
    public String hello() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch((messagesPerRequest));
        IntStream.range(0, messagesPerRequest)
            .forEach(i -> this.template.send(topicName, String.valueOf(i),
                new PracticalAdvice("A Practical Advice", i))
            );
        if (!latch.await(60, TimeUnit.SECONDS)) {
            log.info("waiting time elapsed before the count reached zero");
        }
        log.info("All messages received");
        return "Hello Kafka!";
    }
}
