package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.domain.in.OfferDetailsProto;
import com.adrienben.demo.domain.in.PriceProto;
import com.adrienben.demo.domain.in.ProductDetailsProto;
import com.adrienben.demo.domain.in.SkuDetailsProto;
import com.adrienben.demo.domain.out.OfferProto;
import com.adrienben.demo.domain.out.ProductProto;
import com.adrienben.demo.domain.out.SkuProto;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
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
@SpringBootTest(classes = { JacksonAutoConfiguration.class, ProtobufSerdeConfig.class, StreamConfigTest.SchemaRegistryConfiguration.class })
class StreamConfigTest {

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

	private final KafkaProtobufSerde<ProductDetailsProto> productDetailsProtoSerde;
	private final KafkaProtobufSerde<SkuDetailsProto> skuDetailsProtoSerde;
	private final KafkaProtobufSerde<OfferDetailsProto> offerDetailsProtoSerde;
	private final KafkaProtobufSerde<PriceProto> priceProtoSerde;
	private final KafkaProtobufSerde<ProductProto> productProtoSerde;
	private final ObjectMapper mapper;

	private TestInputTopic<String, ProductDetailsProto> productDetailsInputTopic;
	private TestInputTopic<byte[], SkuDetailsProto> skuDetailsInputTopic;
	private TestInputTopic<byte[], OfferDetailsProto> offerDetailsInputTopic;
	private TestInputTopic<byte[], PriceProto> priceInputTopic;
	private TestOutputTopic<String, ProductProto> productOutputTopic;
	private TopologyTestDriver topologyTestDriver;

	@BeforeEach
	void setup() {
		topologyTestDriver = buildTopologyTestDriver();

		productDetailsInputTopic = topologyTestDriver
				.createInputTopic(PRODUCT_DETAILS_TOPIC, new StringSerializer(), productDetailsProtoSerde.serializer());
		skuDetailsInputTopic = topologyTestDriver
				.createInputTopic(SKU_DETAILS_TOPIC, new ByteArraySerializer(), skuDetailsProtoSerde.serializer());
		offerDetailsInputTopic = topologyTestDriver
				.createInputTopic(OFFER_DETAILS_TOPIC, new ByteArraySerializer(), offerDetailsProtoSerde.serializer());
		priceInputTopic = topologyTestDriver
				.createInputTopic(PRICES_TOPIC, new ByteArraySerializer(), priceProtoSerde.serializer());
		productOutputTopic = topologyTestDriver
				.createOutputTopic(PRODUCTS_TOPIC, new StringDeserializer(), productProtoSerde.deserializer());
	}

	private TopologyTestDriver buildTopologyTestDriver() {
		var streamConfig = new StreamConfig(
				productDetailsProtoSerde,
				skuDetailsProtoSerde,
				offerDetailsProtoSerde,
				priceProtoSerde,
				productProtoSerde,
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
		var price = PriceProto.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setAmount(19_999.99f)
				.build();
		priceInputTopic.pipeInput(price);
		assertIncompleteProductNotInKafka();

		// Send offer details
		var offerDetails = OfferDetailsProto.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setName("Refurbished blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue ! It should work too.")
				.build();
		offerDetailsInputTopic.pipeInput(offerDetails);
		assertIncompleteProductNotInKafka();

		// Send sku details
		var skuDetails = SkuDetailsProto.newBuilder()
				.setSkuId("S1P1")
				.setProductId("P1")
				.setName("Blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue !")
				.build();
		skuDetailsInputTopic.pipeInput(skuDetails);
		assertIncompleteProductNotInKafka();

		// Send product details
		var productDetails = ProductDetailsProto.newBuilder()
				.setName("Wonderful thing")
				.setDescription("That's a wonderful thing, trust me...")
				.setBrand("ShadyGuys")
				.build();
		productDetailsInputTopic.pipeInput("P1", productDetails);

		// Read resulting product
		var productKeyValue = productOutputTopic.readKeyValue();

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

		assertThat(productKeyValue.key, is("P1"));
		assertThat(productKeyValue.value, is(expectedProduct));
	}

	private void assertIncompleteProductNotInKafka() {
		assertThat(productOutputTopic.isEmpty(), is(true));
	}
}
