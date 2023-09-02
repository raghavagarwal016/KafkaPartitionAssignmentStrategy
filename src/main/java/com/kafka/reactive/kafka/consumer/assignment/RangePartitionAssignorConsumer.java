package com.kafka.reactive.kafka.consumer.assignment;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
public class RangePartitionAssignorConsumer {

  public static void start(String instanceId, List<String> event) {

    //create kafka consumer config
    Map<String, Object> consumerConfig = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, //consumer will deserialize key
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, //consumer will deserialize value
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // will consume from the earliest offset. Default is latest. If latest it will consume after it has joined consumers.
        ConsumerConfig.GROUP_ID_CONFIG, "RangePartitionAssignorConsumerGroup", // groupId
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId, // identifier for a consumer in consumer group.
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName() // setting partition strategy as RangeAssignor
    );

    //create receiver
    ReceiverOptions receiverOptions = ReceiverOptions
        .create(consumerConfig) // create receiver using consumer config
        .subscription(event); // topics we want to listen

    //actual consumer
    Flux<ReceiverRecord<Object, Object>> messages = KafkaReceiver
        .create(receiverOptions) // create consumer
        .receive(); // start recieving the messages. It will create a flux.

    messages
        .doOnNext(message -> log.info("key : {}, value : {}, header : {} ", message.key(), message.value(), message.headers())) // processing the message
        .doOnNext(message -> message.receiverOffset().acknowledge()) // acknowledge after processing the message
        .subscribe(); // subscribe to the event messages (non deamon thread)

  }

}