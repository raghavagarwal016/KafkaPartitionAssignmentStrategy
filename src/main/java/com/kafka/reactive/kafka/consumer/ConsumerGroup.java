package com.kafka.reactive.kafka.consumer;

import java.util.Arrays;

import com.kafka.reactive.kafka.consumer.assignment.CooperativeStickyPartitionAssignorConsumer;
import com.kafka.reactive.kafka.consumer.assignment.InvertedRangePartitionAssignorConsumer;
import com.kafka.reactive.kafka.consumer.assignment.RangePartitionAssignorConsumer;
import com.kafka.reactive.kafka.consumer.assignment.RoundRobinPartitionAssignorConsumer;
import com.kafka.reactive.kafka.consumer.assignment.StickyPartitionAssignorConsumer;

public class ConsumerGroup {

  private static class Consumer1 {
    public static void main(String[] args) {
      // RangePartitionAssignorConsumer.start("C1", Arrays.asList("topic-a", "topic-b"));
      // RoundRobinPartitionAssignorConsumer.start("C1", Arrays.asList("topic-a", "topic-b"));
      // StickyPartitionAssignorConsumer.start("C1", Arrays.asList("topic-a", "topic-b"));
      // CooperativeStickyPartitionAssignorConsumer.start("C1", Arrays.asList("topic-a", "topic-b"));
      InvertedRangePartitionAssignorConsumer.start("C1", Arrays.asList("topic-a", "topic-b"));
    }
  }


  private static class Consumer2 {
    public static void main(String[] args) {
      // RangePartitionAssignorConsumer.start("C2", Arrays.asList("topic-a", "topic-b"));
      // RoundRobinPartitionAssignorConsumer.start("C2", Arrays.asList("topic-a", "topic-b"));
      // StickyPartitionAssignorConsumer.start("C2", Arrays.asList("topic-a", "topic-b"));
      // CooperativeStickyPartitionAssignorConsumer.start("C2", Arrays.asList("topic-a", "topic-b"));
      InvertedRangePartitionAssignorConsumer.start("C2", Arrays.asList("topic-a", "topic-b"));
    }
  }


  private static class Consumer3 {
    public static void main(String[] args) {
      // RangePartitionAssignorConsumer.start("C3", Arrays.asList("topic-a", "topic-b"));
      // RoundRobinPartitionAssignorConsumer.start("C3", Arrays.asList("topic-a", "topic-b"));
      // StickyPartitionAssignorConsumer.start("C3", Arrays.asList("topic-a", "topic-b"));
      // CooperativeStickyPartitionAssignorConsumer.start("C3", Arrays.asList("topic-a", "topic-b"));
      InvertedRangePartitionAssignorConsumer.start("C3", Arrays.asList("topic-a", "topic-b"));
    }
  }

}
