package com.adrienben.demo.kstreamconnectionsaggregationexample;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Offer;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Sku;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.OFFER_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRICES_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCTS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCT_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SKU_DETAILS_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@SpringBootTest
@EmbeddedKafka(
		topics = { PRODUCT_DETAILS_TOPIC, SKU_DETAILS_TOPIC, OFFER_DETAILS_TOPIC, PRICES_TOPIC, PRODUCTS_TOPIC },
		ports = { 19092 }
)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=localhost:19092")
public class AppTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private ObjectMapper mapper;

	@Test
	void integrationTest() throws ExecutionException, InterruptedException {
		var productDetailsProducer = createProducer(new StringSerializer(), new JsonSerializer<ProductDetails>(mapper));
		var skuDetailsProducer = createProducer(null, new JsonSerializer<SkuDetails>(mapper));
		var offerDetailsProducer = createProducer(null, new JsonSerializer<OfferDetails>(mapper));
		var priceProducer = createProducer(null, new JsonSerializer<Price>(mapper));
		var productConsumer = createConsumer(PRODUCTS_TOPIC, new StringDeserializer(), new JsonDeserializer<>(Product.class, mapper, false));

		// Send a price
		var price = new Price(
				"O1S1P1",
				"P1",
				"S1P1",
				19_999.99f);
		priceProducer.send(new ProducerRecord<>(PRICES_TOPIC, price)).get();
		assertIncompleteProductNotInKafka(productConsumer);

		// Send offer details
		var offerDetails = new OfferDetails(
				"O1S1P1",
				"P1",
				"S1P1",
				"Refurbished blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue ! It should work too.");
		offerDetailsProducer.send(new ProducerRecord<>(OFFER_DETAILS_TOPIC, offerDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send sku details
		var skuDetails = new SkuDetails(
				"S1P1",
				"P1",
				"Blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue !");
		skuDetailsProducer.send(new ProducerRecord<>(SKU_DETAILS_TOPIC, skuDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send product details
		var productDetails = new ProductDetails(
				null,
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys");
		productDetailsProducer.send(new ProducerRecord<>(PRODUCT_DETAILS_TOPIC, "P1", productDetails));
		var product = KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC);

		var expectedProduct = new Product(
				"P1",
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys",
				List.of(new Sku(
						"S1P1",
						"Blue wonderful thing",
						"That's a wonderful thing, trust me..., and this one is blue !",
						List.of(new Offer(
								"O1S1P1",
								"Refurbished blue wonderful thing",
								"That's a wonderful thing, trust me..., and this one is blue ! It should work too.",
								19_999.99f
						)))));

		assertThat(product, hasKey("P1"));
		assertThat(product, hasValue(expectedProduct));
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

	private static void assertIncompleteProductNotInKafka(Consumer<String, Product> productConsumer) {
		assertThrows(
				IllegalStateException.class,
				() -> KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC, Duration.ofSeconds(5).toMillis()),
				"Product should be incomplete and should not have been sent to Kafka");
	}
}
