package com.adrienben.demo.kstreamconnectionsaggregationexample;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Set;
import java.util.concurrent.ExecutionException;

@SpringBootTest
@EmbeddedKafka(
		topics = {},
		ports = { 9092 }
)
public class AppTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private ObjectMapper mapper;

	@Test
	void integrationTest() throws ExecutionException, InterruptedException {

	}

	private <K, V> Producer<K, V> createProducer(
			Serializer<K> keySerializer,
			Serializer<V> valueSerializer
	) {
		var producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		var defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProps, keySerializer, valueSerializer);
		return defaultKafkaProducerFactory.createProducer();
	}

	private <K, V> Consumer<K, V> createConsumer(
			String topic,
			Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer
	) {
		var consumerProps = KafkaTestUtils.consumerProps("integration-test", "true", embeddedKafka);
		var defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, keyDeserializer, valueDeserializer);
		var consumer = defaultKafkaConsumerFactory.createConsumer();
		consumer.subscribe(Set.of(topic));
		return consumer;
	}
}
