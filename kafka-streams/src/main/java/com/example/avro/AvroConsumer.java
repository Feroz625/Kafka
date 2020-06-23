package com.example.avro;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class AvroConsumer {

	public static void main(String[] args) {
		
		String topicName = "CustomerDetails";
		
			Properties props = new Properties();
        
		    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.10:9092");
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-customers");
	        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	      
	        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
	        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.43.10:8081");

        
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

