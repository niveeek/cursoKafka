package com.n3k.backend.str_consumer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.HashMap;
import java.util.logging.Logger;

@Configuration
public class StringConsumerConfig {
  @Autowired
  private KafkaProperties kafkaProperties;
  private final Logger log = Logger.getLogger(StringConsumerConfig.class.getName());

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    var config = new HashMap<String, Object>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return new DefaultKafkaConsumerFactory<>(config);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> validMessageContainerFactory(ConsumerFactory<String, String> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumerFactory);
    factory.setRecordInterceptor(validMessage());
    return factory;
  }

  private RecordInterceptor<String, String> validMessage() {
    return (record, consumer) -> {
      if (record.value().contains("Moin")) {
        log.info("Contiene la palabra Moin");
        return record;
      }
      return record;
    };
  }
}
