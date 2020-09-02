package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.PriceAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.OfferAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.ProductAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.SkuAvro;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestConstructor;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.OFFER_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRICES_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCTS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCT_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SKU_DETAILS_TOPIC;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Slf4j
@RequiredArgsConstructor
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
@SpringBootTest(classes = { JacksonAutoConfiguration.class, AvroConfig.class, StreamConfigTest.SchemaRegistryConfiguration.class })
class StreamConfigTest {

	@TestConfiguration
	static class SchemaRegistryConfiguration {
		@Bean
		public SchemaRegistryClient schemaRegistryClient() throws IOException, RestClientException {
			var client = new MockSchemaRegistryClient();
			client.register(PRODUCT_DETAILS_TOPIC + "-value", new AvroSchema(ProductDetailsAvro.SCHEMA$));
			client.register(SKU_DETAILS_TOPIC + "-value", new AvroSchema(SkuDetailsAvro.SCHEMA$));
			client.register(OFFER_DETAILS_TOPIC + "-value", new AvroSchema(OfferDetailsAvro.SCHEMA$));
			client.register(PRICES_TOPIC + "-value", new AvroSchema(PriceAvro.SCHEMA$));
			client.register(PRODUCTS_TOPIC + "-value", new AvroSchema(ProductAvro.SCHEMA$));
			return client;
		}
	}

	private final SpecificAvroSerde<ProductDetailsAvro> productDetailsAvroSerde;
	private final SpecificAvroSerde<SkuDetailsAvro> skuDetailsAvroSerde;
	private final SpecificAvroSerde<OfferDetailsAvro> offerDetailsAvroSerde;
	private final SpecificAvroSerde<PriceAvro> priceAvroSerde;
	private final SpecificAvroSerde<ProductAvro> productAvroSerde;
	private final ObjectMapper mapper;

	private TestInputTopic<String, ProductDetailsAvro> productDetailsInputTopic;
	private TestInputTopic<byte[], SkuDetailsAvro> skuDetailsInputTopic;
	private TestInputTopic<byte[], OfferDetailsAvro> offerDetailsInputTopic;
	private TestInputTopic<byte[], PriceAvro> priceInputTopic;
	private TestOutputTopic<String, ProductAvro> productOutputTopic;
	private TopologyTestDriver topologyTestDriver;

	@BeforeEach
	void setup() {
		topologyTestDriver = buildTopologyTestDriver();

		productDetailsInputTopic = topologyTestDriver
				.createInputTopic(PRODUCT_DETAILS_TOPIC, new StringSerializer(), productDetailsAvroSerde.serializer());
		skuDetailsInputTopic = topologyTestDriver
				.createInputTopic(SKU_DETAILS_TOPIC, new ByteArraySerializer(), skuDetailsAvroSerde.serializer());
		offerDetailsInputTopic = topologyTestDriver
				.createInputTopic(OFFER_DETAILS_TOPIC, new ByteArraySerializer(), offerDetailsAvroSerde.serializer());
		priceInputTopic = topologyTestDriver
				.createInputTopic(PRICES_TOPIC, new ByteArraySerializer(), priceAvroSerde.serializer());
		productOutputTopic = topologyTestDriver
				.createOutputTopic(PRODUCTS_TOPIC, new StringDeserializer(), productAvroSerde.deserializer());
	}

	private TopologyTestDriver buildTopologyTestDriver() {
		var streamConfig = new StreamConfig(
				productDetailsAvroSerde,
				skuDetailsAvroSerde,
				offerDetailsAvroSerde,
				priceAvroSerde,
				productAvroSerde,
				mapper);

		var streamsBuilder = new StreamsBuilder();
		streamConfig.kStream(streamsBuilder);
		var topology = streamsBuilder.build();

		var properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1324");
		properties.put(StreamsConfig.STATE_DIR_CONFIG, "./target/driver-state");
		return new TopologyTestDriver(topology, properties);
	}

	@AfterEach
	void cleanup() {
		try {
			topologyTestDriver.close();
		} catch (Exception exception) {
			log.error("Failed to cleanup state");
		}
	}

	@Test
	@DisplayName("It should only output product when complete")
	void productCompleteness() {

		// Send a price
		var price = new PriceAvro(
				"O1S1P1",
				"P1",
				"S1P1",
				19_999.99f);
		priceInputTopic.pipeInput(price);
		assertIncompleteProductNotInKafka();

		// Send offer details
		var offerDetails = new OfferDetailsAvro(
				"O1S1P1",
				"P1",
				"S1P1",
				"Refurbished blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue ! It should work too.");
		offerDetailsInputTopic.pipeInput(offerDetails);
		assertIncompleteProductNotInKafka();

		// Send sku details
		var skuDetails = new SkuDetailsAvro(
				"S1P1",
				"P1",
				"Blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue !");
		skuDetailsInputTopic.pipeInput(skuDetails);
		assertIncompleteProductNotInKafka();

		// Send product details
		var productDetails = new ProductDetailsAvro(
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys");
		productDetailsInputTopic.pipeInput("P1", productDetails);

		// Read resulting product
		var productKeyValue = productOutputTopic.readKeyValue();

		var expectedProduct = new ProductAvro(
				"P1",
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys",
				List.of(new SkuAvro(
						"S1P1",
						"Blue wonderful thing",
						"That's a wonderful thing, trust me..., and this one is blue !",
						List.of(new OfferAvro(
								"O1S1P1",
								"Refurbished blue wonderful thing",
								"That's a wonderful thing, trust me..., and this one is blue ! It should work too.",
								19_999.99f
						)))));

		assertThat(productKeyValue.key, is("P1"));
		assertThat(productKeyValue.value, is(expectedProduct));
	}

	private void assertIncompleteProductNotInKafka() {
		assertThat(productOutputTopic.isEmpty(), is(true));
	}
}
