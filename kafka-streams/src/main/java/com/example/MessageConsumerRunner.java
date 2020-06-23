package com.example;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumerRunner {

    public static void main(String[] args) {
        MessageConsumer consumer = new MessageConsumer();
        Thread thread = new Thread(consumer);
        thread.start();
        
        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {
        }
    }

    private static class MessageConsumer implements Runnable {

        private final KafkaConsumer consumer;
        private final String topic;

        public MessageConsumer() {
            Properties prop = createConsumerConfig();
            this.consumer = new KafkaConsumer(prop);
            this.topic = "custom-partitioned-topic";
            this.consumer.subscribe(Collections.singletonList(this.topic));
        }

        private Properties createConsumerConfig() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.43.10:9092");
            props.put("group.id", "custom-partitions");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            return props;
        }

        public void run() {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (final ConsumerRecord<String, String> record : records)
                    System.out.println("Message is " + record.value() + ",Printed from Partition: " + record.partition());
            }
        }

    }

}