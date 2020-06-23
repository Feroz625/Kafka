package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public void configure(Map configs) {
    }

    public void close() {
    }

    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {
        List partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionValue = Integer.valueOf((String) value);

        if (partitionValue > (numPartitions - 1))
            return numPartitions - 1;

        return partitionValue;
    }

}
