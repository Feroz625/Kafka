package com.example.avro;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String topic = "CustomerDetails";
		
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.10:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.setProperty("value.serializer",  KafkaAvroSerializer.class.getName());
		props.setProperty("schema.registry.url", "http://192.168.43.10:8081");

		KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(props);
		
		Customer txn = Customer.newBuilder()
				.setFullname("Test1")
				.setId(001)
				.setCity("Hyd")
				.build();
		ProducerRecord<String, Customer> producerRecord1 = new ProducerRecord<String, Customer>(topic, txn.getFullname().toString(), txn);

		Future<RecordMetadata> future = kafkaProducer.send(producerRecord1);
		RecordMetadata metadata = future.get();
		System.out.println(metadata.offset() + " " + metadata.partition() + " " + metadata.topic());

		Customer txn2 = Customer.newBuilder()
				.setFullname("Test2")
				.setId(002)
				.setCity("Hyd")
				.build();
		ProducerRecord<String, Customer> producerRecord2 = new ProducerRecord<String, Customer>(topic, txn2.getFullname().toString(), txn2);

		future = kafkaProducer.send(producerRecord2);
		metadata = future.get();
		System.out.println(metadata.offset() + " " + metadata.partition() + " " + metadata.topic());
		
		Customer txn3 = Customer.newBuilder()
				.setFullname("Test3")
				.setId(003)
				.setCity("Hyd")
				.build();
		ProducerRecord<String, Customer> producerRecord3 = new ProducerRecord<String, Customer>(topic, txn3.getFullname().toString(), txn3);

		future = kafkaProducer.send(producerRecord3);
		metadata = future.get();
		System.out.println(metadata.offset() + " " + metadata.partition() + " " + metadata.topic());
		
		Customer txn4 = Customer.newBuilder()
				.setFullname("Test4")
				.setId(004)
				.setCity("Hyd")
				.build();
		ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(topic, txn4.getFullname().toString(), txn4);

		future = kafkaProducer.send(producerRecord);
		 metadata = future.get();
		System.out.println(metadata.offset() + " " + metadata.partition() + " " + metadata.topic());
		
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
