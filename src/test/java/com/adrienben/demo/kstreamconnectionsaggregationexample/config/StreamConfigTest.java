package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Offer;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Sku;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Collections;
import java.util.Properties;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.OFFER_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRICES_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCTS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.PRODUCT_DETAILS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SKU_DETAILS_TOPIC;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Slf4j
class StreamConfigTest {

	private static ObjectMapper mapper = new JacksonConfig().objectMapper();

	private TopologyTestDriver topologyTestDriver;

	@BeforeEach
	void setup() {
		topologyTestDriver = buildTopologyTestDriver();
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
		var productDetailsFactory = new ConsumerRecordFactory<>(PRODUCT_DETAILS_TOPIC, new StringSerializer(), new JsonSerializer<ProductDetails>());
		var skuDetailsFactory = new ConsumerRecordFactory<>(SKU_DETAILS_TOPIC, new ByteArraySerializer(), new JsonSerializer<SkuDetails>());
		var offerDetailsFactory = new ConsumerRecordFactory<>(OFFER_DETAILS_TOPIC, new ByteArraySerializer(), new JsonSerializer<OfferDetails>());
		var priceFactory = new ConsumerRecordFactory<>(PRICES_TOPIC, new ByteArraySerializer(), new JsonSerializer<Price>());

		// Send a price
		var price = new Price(
				"O1S1P1",
				"P1",
				"S1P1",
				19_999.99f);
		topologyTestDriver.pipeInput(priceFactory.create(price));
		assertIncompleteProductNotInKafka(topologyTestDriver);

		// Send offer details
		var offerDetails = new OfferDetails(
				"O1S1P1",
				"P1",
				"S1P1",
				"Refurbished blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue ! It should work too.");
		topologyTestDriver.pipeInput(offerDetailsFactory.create(offerDetails));
		assertIncompleteProductNotInKafka(topologyTestDriver);

		// Send sku details
		var skuDetails = new SkuDetails(
				"S1P1",
				"P1",
				"Blue wonderful thing",
				"That's a wonderful thing, trust me..., and this one is blue !");
		topologyTestDriver.pipeInput(skuDetailsFactory.create(skuDetails));
		assertIncompleteProductNotInKafka(topologyTestDriver);

		// Send product details
		var productDetails = new ProductDetails(
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys");
		topologyTestDriver.pipeInput(productDetailsFactory.create(PRODUCT_DETAILS_TOPIC, "P1", productDetails));
		var product = topologyTestDriver.readOutput(PRODUCTS_TOPIC, new StringDeserializer(), createJsonDeserializer(Product.class));

		var expectedProduct = new Product(
				"P1",
				"Wonderful thing",
				"That's a wonderful thing, trust me...",
				"ShadyGuys",
				Collections.singletonList(new Sku(
						"S1P1",
						"Blue wonderful thing",
						"That's a wonderful thing, trust me..., and this one is blue !",
						Collections.singletonList(new Offer(
								"O1S1P1",
								"Refurbished blue wonderful thing",
								"That's a wonderful thing, trust me..., and this one is blue ! It should work too.",
								19_999.99f
						)))));

		OutputVerifier.compareKeyValue(product, "P1", expectedProduct);
	}

	private static void assertIncompleteProductNotInKafka(TopologyTestDriver topologyTestDriver) {
		var product = topologyTestDriver.readOutput(PRODUCTS_TOPIC);
		assertThat(product, is(nullValue()));
	}

	private static <T> Deserializer<T> createJsonDeserializer(Class<T> tClass) {
		return new JsonDeserializer<>(tClass, mapper, false);
	}

	private static TopologyTestDriver buildTopologyTestDriver() {
		var streamConfig = new StreamConfig(mapper);

		var streamsBuilder = new StreamsBuilder();
		streamConfig.kStream(streamsBuilder);
		var topology = streamsBuilder.build();

		var properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1324");
		properties.put(StreamsConfig.STATE_DIR_CONFIG, "./target/driver-state");
		return new TopologyTestDriver(topology, properties);
	}
}
