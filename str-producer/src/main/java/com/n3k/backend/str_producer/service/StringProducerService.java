package com.n3k.backend.str_producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class StringProducerService {
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  private static final Logger log = LoggerFactory.getLogger(StringProducerService.class);

  public void sendMessage(String message) {
    kafkaTemplate.send("str-topic", message).whenComplete((result, exception) -> {
      if (exception != null) {
        log.error("Error sending message: {}", exception.getMessage());
      }
      log.info("Message sent: {}", result.getProducerRecord().value());
      log.info("Partition: {}, Offset: {}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    });
  }
}
