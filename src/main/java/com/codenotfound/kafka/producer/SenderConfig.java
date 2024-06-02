package com.codenotfound.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import com.codenotfound.kafka.serializer.CustomKafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.codenotfound.kafka.serializer.AvroSerializer;

import example.avro.User;

@Configuration
public class SenderConfig {

  @Value("${kafka.bootstrap-servers}")
  private String bootstrapServers;

//  @Autowired
//  Serializer serializer;

//  @Bean
//  @Profile("!test")
//  public AvroSerializer avroSerializer(){
//    return new AvroSerializer();
//  }
//
//  @Bean
//  @Profile("test")
//  public CustomKafkaAvroSerializer customKafkaAvroSerialize(){
//    return new CustomKafkaAvroSerializer();
//  }

  @Bean
  @Profile("!test")
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
    props.put("schema.registry.url", "not-used");
    return props;
  }

  @Bean
  @Profile("test")
  public Map<String, Object> producerTestConfigs() {
    Map<String, Object> props = new HashMap<>();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomKafkaAvroSerializer.class);
    props.put("schema.registry.url", "not-used");
    return props;
  }

  @Bean
  @Profile("!test")
  public ProducerFactory<String, User> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean("producerFactory")
  @Profile("test")
  public ProducerFactory<String, User> producerTestFactory() {
    return new DefaultKafkaProducerFactory<>(producerTestConfigs());
  }

  @Bean
  public KafkaTemplate<String, User> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public Sender sender() {
    return new Sender();
  }
}
