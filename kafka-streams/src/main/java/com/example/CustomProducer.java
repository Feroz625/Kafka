package com.example;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class CustomProducer {

    public static void main(String[] args) throws Exception{

        String topicName = "custom-partitioned-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.43.10:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.example.CustomPartitioner");

        Producer producer = new KafkaProducer
                (props);

        for(int i = 0; i < 40; i++) {
            int v = i % 3;
            producer.send(new ProducerRecord(topicName, String.valueOf(v), String.valueOf(v)));
        }
        System.out.println("Message sent successfully");
        producer.close();
        System.out.println("SimpleProducer Completed.");
    }
}
