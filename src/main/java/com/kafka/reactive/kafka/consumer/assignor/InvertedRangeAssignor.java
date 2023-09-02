package com.kafka.reactive.kafka.consumer.assignor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

public class InvertedRangeAssignor extends AbstractPartitionAssignor {
  public static final String INVERTED_RANGE_ASSIGNER_NAME = "inverted-range";

  public InvertedRangeAssignor() {
  }

  @Override
  public String name() {
    return INVERTED_RANGE_ASSIGNER_NAME;
  }

  @Override
  public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {

    //generate list of topic partition from partitionsPerTopic and group them by topic name
    Map<String, List<TopicPartition>> topicPartitions = partitionsPerTopic
        .entrySet()
        .stream()
        .flatMap(partition -> IntStream.range(0 ,partition.getValue().intValue()).mapToObj(partitionNumber -> new TopicPartition(partition.getKey(), partitionNumber)))
        .collect(Collectors.groupingBy(TopicPartition::topic));

    //which partition assigned to which subscriber
    Map<String, List<TopicPartition>> assignments = new HashMap<>();

    //Reverse consumer list which is in ascending order of member_id
    List<String> consumerMemberIds = subscriptions.entrySet().stream().map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Collections.reverse(consumerMemberIds);

    //assign partitions to consumers using modulus
    for (Map.Entry<String, List<TopicPartition>> topicPartition : topicPartitions.entrySet()) {
      List<String> memberIdsWeWillAssignPartitionsTo =
          consumerMemberIds.stream().limit(topicPartition.getValue().size()).collect(Collectors.toList());
      for (TopicPartition partition : topicPartition.getValue()) {
        String memberId = getSubscriptionMemberId(partition, memberIdsWeWillAssignPartitionsTo);
        List<TopicPartition> assignedPartitions =
            assignments.getOrDefault(memberId, new ArrayList<>());
        assignedPartitions.add(partition);
        assignments.put(memberId, assignedPartitions);
      }
    }

    for (String memberId : consumerMemberIds) {
      if (!assignments.keySet().contains(memberId)) {
       assignments.put(memberId, new ArrayList<>());
      }
    }

    return assignments;
  }



  private String getSubscriptionMemberId(TopicPartition topicPartition,
      List<String> memberIdsWeWillAssignPartitionsTo) {
    return memberIdsWeWillAssignPartitionsTo.get(topicPartition.partition() % memberIdsWeWillAssignPartitionsTo.size());
  }
}
