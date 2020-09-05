package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Offer;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Sku;
import com.adrienben.demo.kstreamconnectionsaggregationexample.service.ProductService;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestConstructor;

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
@SpringBootTest(classes = { AvroConfig.class, SchemaRegistryConfiguration.class })
class StreamConfigTest {

	private final SpecificAvroSerde<ProductDetails> productDetailsSerde;
	private final SpecificAvroSerde<SkuDetails> skuDetailsSerde;
	private final SpecificAvroSerde<OfferDetails> offerDetailsSerde;
	private final SpecificAvroSerde<Price> priceSerde;
	private final SpecificAvroSerde<Product> productSerde;

	private TestInputTopic<String, ProductDetails> productDetailsInputTopic;
	private TestInputTopic<byte[], SkuDetails> skuDetailsInputTopic;
	private TestInputTopic<byte[], OfferDetails> offerDetailsInputTopic;
	private TestInputTopic<byte[], Price> priceInputTopic;
	private TestOutputTopic<String, Product> productOutputTopic;
	private TopologyTestDriver topologyTestDriver;

	@BeforeEach
	void setup() {
		topologyTestDriver = buildTopologyTestDriver();

		productDetailsInputTopic = topologyTestDriver
				.createInputTopic(PRODUCT_DETAILS_TOPIC, new StringSerializer(), productDetailsSerde.serializer());
		skuDetailsInputTopic = topologyTestDriver
				.createInputTopic(SKU_DETAILS_TOPIC, new ByteArraySerializer(), skuDetailsSerde.serializer());
		offerDetailsInputTopic = topologyTestDriver
				.createInputTopic(OFFER_DETAILS_TOPIC, new ByteArraySerializer(), offerDetailsSerde.serializer());
		priceInputTopic = topologyTestDriver
				.createInputTopic(PRICES_TOPIC, new ByteArraySerializer(), priceSerde.serializer());
		productOutputTopic = topologyTestDriver
				.createOutputTopic(PRODUCTS_TOPIC, new StringDeserializer(), productSerde.deserializer());
	}

	private TopologyTestDriver buildTopologyTestDriver() {
		var streamConfig = new StreamConfig();
		var streamsBuilder = new StreamsBuilder();

		streamConfig.kStream(streamsBuilder,
				productDetailsSerde,
				skuDetailsSerde,
				offerDetailsSerde,
				priceSerde,
				productSerde,
				new ProductService());
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
		var price = new Price(
				"O1S1P1",
				"P1",
				"S1P1",
				19_999.99f);
		priceInputTopic.pipeInput(price);
		assertIncompleteProductNotInKafka();

		// Send offer details
		var offerDetails = new OfferDetails(
				"O1S1P1",
				"P1",
				"S1P1",
				"Refurbished blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue ! It should work too.");
		offerDetailsInputTopic.pipeInput(offerDetails);
		assertIncompleteProductNotInKafka();

		// Send sku details
		var skuDetails = new SkuDetails(
				"S1P1",
				"P1",
				"Blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue !");
		skuDetailsInputTopic.pipeInput(skuDetails);
		assertIncompleteProductNotInKafka();

		// Send product details
		var productDetails = new ProductDetails(
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys");
		productDetailsInputTopic.pipeInput("P1", productDetails);

		// Read resulting product
		var productKeyValue = productOutputTopic.readKeyValue();

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

		assertThat(productKeyValue.key, is("P1"));
		assertThat(productKeyValue.value, is(expectedProduct));
	}

	private void assertIncompleteProductNotInKafka() {
		assertThat(productOutputTopic.isEmpty(), is(true));
	}
}
