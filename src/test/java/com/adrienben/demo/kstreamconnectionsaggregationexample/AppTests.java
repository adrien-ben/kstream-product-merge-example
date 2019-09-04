package com.adrienben.demo.kstreamconnectionsaggregationexample;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.PriceAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.OfferAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.ProductAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.SkuAvro;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.OFFER_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRICES_BY_PRODUCT_ID_REKEY_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRICES_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCTS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCT_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SKU_DETAILS_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@SpringBootTest
@EmbeddedKafka(
		topics = { PRODUCT_DETAILS_TOPIC, SKU_DETAILS_TOPIC, SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, OFFER_DETAILS_TOPIC,
				OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, PRICES_TOPIC, PRICES_BY_PRODUCT_ID_REKEY_TOPIC, PRODUCTS_TOPIC },
		ports = { 19092 }
)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=localhost:19092")
public class AppTests {

	@TestConfiguration
	static class SchemaRegistryConfiguration {
		@Bean
		public SchemaRegistryClient schemaRegistryClient() throws IOException, RestClientException {
			var client = new MockSchemaRegistryClient();
			client.register(PRODUCT_DETAILS_TOPIC + "-value", ProductDetailsAvro.SCHEMA$);
			client.register(SKU_DETAILS_TOPIC + "-value", SkuDetailsAvro.SCHEMA$);
			client.register(OFFER_DETAILS_TOPIC + "-value", OfferDetailsAvro.SCHEMA$);
			client.register(PRICES_TOPIC + "-value", PriceAvro.SCHEMA$);
			client.register(PRODUCTS_TOPIC + "-value", ProductAvro.SCHEMA$);
			return client;
		}
	}

	private final EmbeddedKafkaBroker embeddedKafka;
	private final SpecificAvroSerde<ProductDetailsAvro> productDetailsSerde;
	private final SpecificAvroSerde<SkuDetailsAvro> skuDetailsSerde;
	private final SpecificAvroSerde<OfferDetailsAvro> offerDetailsSerde;
	private final SpecificAvroSerde<PriceAvro> priceSerde;
	private final SpecificAvroSerde<ProductAvro> productSerde;

	@Autowired
	public AppTests(
			EmbeddedKafkaBroker embeddedKafka,
			SpecificAvroSerde<ProductDetailsAvro> productDetailsSerde,
			SpecificAvroSerde<SkuDetailsAvro> skuDetailsSerde,
			SpecificAvroSerde<OfferDetailsAvro> offerDetailsSerde,
			SpecificAvroSerde<PriceAvro> priceSerde,
			SpecificAvroSerde<ProductAvro> productSerde
	) {
		this.embeddedKafka = embeddedKafka;
		this.productDetailsSerde = productDetailsSerde;
		this.skuDetailsSerde = skuDetailsSerde;
		this.offerDetailsSerde = offerDetailsSerde;
		this.priceSerde = priceSerde;
		this.productSerde = productSerde;
	}

	@Test
	void integrationTest() throws ExecutionException, InterruptedException {
		var productDetailsProducer = createProducer(new StringSerializer(), productDetailsSerde.serializer());
		var skuDetailsProducer = createProducer(null, skuDetailsSerde.serializer());
		var offerDetailsProducer = createProducer(null, offerDetailsSerde.serializer());
		var priceProducer = createProducer(null, priceSerde.serializer());
		var productConsumer = createConsumer(PRODUCTS_TOPIC, new StringDeserializer(), productSerde.deserializer());

		// Send a price
		var price = new PriceAvro(
				"O1S1P1",
				"P1",
				"S1P1",
				19_999.99f);
		priceProducer.send(new ProducerRecord<>(PRICES_TOPIC, price)).get();
		assertIncompleteProductNotInKafka(productConsumer);

		// Send offer details
		var offerDetails = new OfferDetailsAvro(
				"O1S1P1",
				"P1",
				"S1P1",
				"Refurbished blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue ! It should work too.");
		offerDetailsProducer.send(new ProducerRecord<>(OFFER_DETAILS_TOPIC, offerDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send sku details
		var skuDetails = new SkuDetailsAvro(
				"S1P1",
				"P1",
				"Blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue !");
		skuDetailsProducer.send(new ProducerRecord<>(SKU_DETAILS_TOPIC, skuDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send product details
		var productDetails = new ProductDetailsAvro(
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys");
		productDetailsProducer.send(new ProducerRecord<>(PRODUCT_DETAILS_TOPIC, "P1", productDetails));
		var product = KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC);

		var expectedProduct = new ProductAvro(
				"P1",
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys",
				Collections.singletonList(new SkuAvro(
						"S1P1",
						"Blue wonderful thing",
						"That's a wonderful thing, trust me..., and this one is blue !",
						Collections.singletonList(new OfferAvro(
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

	private static void assertIncompleteProductNotInKafka(Consumer<String, ProductAvro> productConsumer) {
		assertThrows(
				AssertionError.class,
				() -> KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC, Duration.ofSeconds(5).toMillis()),
				"Product should be incomplete and should not have been sent to Kafka");
	}
}
