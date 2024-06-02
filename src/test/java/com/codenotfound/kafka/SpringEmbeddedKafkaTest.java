package com.codenotfound.kafka;

import com.codenotfound.kafka.consumer.Receiver;
import com.codenotfound.kafka.producer.Sender;
import com.codenotfound.kafka.serializer.CustomKafkaAvroDeserializer;
import example.avro.User;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringEmbeddedKafkaTest {

  @Autowired
  private Sender sender;

  @Autowired
  private Receiver receiver;

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @ClassRule
  public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "avro.t");

//  @Before
//  public void setUp() throws Exception {
//    // wait until the partitions are assigned
//    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
//        .getListenerContainers()) {
//      ContainerTestUtils.waitForAssignment(messageListenerContainer,
//          embeddedKafka.getPartitionsPerTopic());
//    }
//  }

  @Test
  public void testReceiver() throws Exception {
    User user = User.newBuilder().setName("John Doe").setFavoriteColor("green")
        .setFavoriteNumber(null).build();
    sender.send(user);

    Thread.sleep(5000);
    //receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
    Map<String, Object> configs = KafkaTestUtils.consumerProps("groupId1", "true", embeddedKafka);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomKafkaAvroDeserializer.class);
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    configs.put("schema.registry.url", "not-used");


    Consumer<String, User> consumer = new DefaultKafkaConsumerFactory<String, User>(configs).createConsumer();
    //embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "avro.t");
    consumer.subscribe(Collections.singletonList("avro.t"));
    ConsumerRecord<String, User> record = KafkaTestUtils.getSingleRecord(consumer, "avro.t");
    System.out.println(String.format("User record is %s", record.value()));
//    List<ConsumerRecord<String, User>> recordList = StreamSupport.stream(records.spliterator(), false)
//            .collect(Collectors.toList());
//    System.out.println("records....." + recordList.size());

    assertThat(record).isNotNull();
  }
}
