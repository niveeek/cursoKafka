package com.n3k.backend.str_consumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class StrConsumerListener {
  private static final Logger log = LoggerFactory.getLogger(StrConsumerListener.class);

  @KafkaListener(
      groupId = "group-1",
      topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}),
      containerFactory = "validMessageContainerFactory"
  )
  public void listener1(String message) {
    log.info("Listener1 ::: Recibiendo el mensaje: {}", message);
  }

  @KafkaListener(
      groupId = "group-1",
      topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}),
      containerFactory = "validMessageContainerFactory"
  )
  public void listener2(String message) {
    log.info("Listener2 ::: Recibiendo el mensaje: {}", message);
  }

  @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "validMessageContainerFactory")
  public void listener3(String message) {
    log.info("Listener3 ::: Recibiendo el mensaje: {}", message);
  }
}
