package com.adrienben.demo.kstreamconnectionsaggregationexample;

import com.adrienben.demo.domain.in.OfferDetailsProto;
import com.adrienben.demo.domain.in.PriceProto;
import com.adrienben.demo.domain.in.ProductDetailsProto;
import com.adrienben.demo.domain.in.SkuDetailsProto;
import com.adrienben.demo.domain.out.OfferProto;
import com.adrienben.demo.domain.out.ProductProto;
import com.adrienben.demo.domain.out.SkuProto;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
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
import org.springframework.test.context.TestConstructor;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.time.Duration;
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
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
@RequiredArgsConstructor
public class AppTests {

	@TestConfiguration
	static class SchemaRegistryConfiguration {
		@Bean
		public SchemaRegistryClient schemaRegistryClient() throws IOException, RestClientException {
			var client = new MockSchemaRegistryClient();
			client.register(PRODUCT_DETAILS_TOPIC + "-value", new ProtobufSchema(ProductDetailsProto.getDescriptor()));
			client.register(SKU_DETAILS_TOPIC + "-value", new ProtobufSchema(SkuDetailsProto.getDescriptor()));
			client.register(OFFER_DETAILS_TOPIC + "-value", new ProtobufSchema(OfferDetailsProto.getDescriptor()));
			client.register(PRICES_TOPIC + "-value", new ProtobufSchema(PriceProto.getDescriptor()));
			client.register(PRODUCTS_TOPIC + "-value", new ProtobufSchema(ProductProto.getDescriptor()));
			return client;
		}
	}

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	private final KafkaProtobufSerde<ProductDetailsProto> productDetailsSerde;
	private final KafkaProtobufSerde<SkuDetailsProto> skuDetailsSerde;
	private final KafkaProtobufSerde<OfferDetailsProto> offerDetailsSerde;
	private final KafkaProtobufSerde<PriceProto> priceSerde;
	private final KafkaProtobufSerde<ProductProto> productSerde;

	@Test
	void integrationTest() throws ExecutionException, InterruptedException {
		var productDetailsProducer = createProducer(new StringSerializer(), productDetailsSerde.serializer());
		var skuDetailsProducer = createProducer(null, skuDetailsSerde.serializer());
		var offerDetailsProducer = createProducer(null, offerDetailsSerde.serializer());
		var priceProducer = createProducer(null, priceSerde.serializer());
		var productConsumer = createConsumer(PRODUCTS_TOPIC, new StringDeserializer(), productSerde.deserializer());

		// Send a price
		var price = PriceProto.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setAmount(19_999.99f)
				.build();
		priceProducer.send(new ProducerRecord<>(PRICES_TOPIC, price)).get();
		assertIncompleteProductNotInKafka(productConsumer);

		// Send offer details
		var offerDetails = OfferDetailsProto.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setName("Refurbished blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue ! It should work too.")
				.build();
		offerDetailsProducer.send(new ProducerRecord<>(OFFER_DETAILS_TOPIC, offerDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send sku details
		var skuDetails = SkuDetailsProto.newBuilder()
				.setSkuId("S1P1")
				.setProductId("P1")
				.setName("Blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue !")
				.build();
		skuDetailsProducer.send(new ProducerRecord<>(SKU_DETAILS_TOPIC, skuDetails));
		assertIncompleteProductNotInKafka(productConsumer);

		// Send product details
		var productDetails = ProductDetailsProto.newBuilder()
				.setName("Wonderful thing")
				.setDescription("That's a wonderful thing, trust me...")
				.setBrand("ShadyGuys")
				.build();
		productDetailsProducer.send(new ProducerRecord<>(PRODUCT_DETAILS_TOPIC, "P1", productDetails));

		// Read resulting product
		var product = KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC);

		var expectedProduct = ProductProto.newBuilder()
				.setId("P1")
				.setName("Wonderful thing")
				.setDescription("That's a wonderful thing, trust me...")
				.setBrand("ShadyGuys")
				.addSkus(SkuProto.newBuilder()
						.setId("S1P1")
						.setName("Blue wonderful thing")
						.setDescription("That's a wonderful thing, trust me..., and this one is blue !")
						.addOffers(OfferProto.newBuilder()
								.setId("O1S1P1")
								.setName("Refurbished blue wonderful thing")
								.setDescription("That's a wonderful thing, trust me..., and this one is blue ! It should work too.")
								.setPrice(19_999.99f)
								.build())
						.build())
				.build();

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

	private static void assertIncompleteProductNotInKafka(Consumer<String, ProductProto> productConsumer) {
		assertThrows(
				IllegalStateException.class,
				() -> KafkaTestUtils.getSingleRecord(productConsumer, PRODUCTS_TOPIC, Duration.ofSeconds(5).toMillis()),
				"Product should be incomplete and should not have been sent to Kafka");
	}
}
