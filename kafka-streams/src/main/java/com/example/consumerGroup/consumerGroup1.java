package com.example.consumerGroup;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class consumerGroup1 {
	public static void main(String[] args) throws Exception {

		   String topicName = "my-cg-topic";
	      Properties props = new Properties();
	      
	      props.put("bootstrap.servers", "192.168.43.10:9092");
	      props.put("group.id", "test1");
	      props.put("enable.auto.commit", "false");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("auto.offset.reset", "earliest");
	      KafkaConsumer<String, String> consumer = new KafkaConsumer
	         <String, String>(props);
	      
	      consumer.subscribe(Arrays.asList(topicName));
	      
	      System.out.println("Subscribed to topic " + topicName);
	      int i = 0;
	      
	      while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	         
	         System.out.printf("offset = %d, key = %s, value = %s\n", 
	            record.offset(), record.key(), record.value());
	      }
	   }
	}
