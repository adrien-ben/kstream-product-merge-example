package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.domain.in.OfferDetails;
import com.adrienben.demo.domain.in.Price;
import com.adrienben.demo.domain.in.ProductDetails;
import com.adrienben.demo.domain.in.SkuDetails;
import com.adrienben.demo.domain.out.Offer;
import com.adrienben.demo.domain.out.Product;
import com.adrienben.demo.domain.out.Sku;
import com.adrienben.demo.kstreamconnectionsaggregationexample.service.ProductService;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestConstructor;

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
@SpringBootTest(classes = { ProtobufSerdeConfig.class, SchemaRegistryConfiguration.class })
class StreamConfigTest {

	private final KafkaProtobufSerde<ProductDetails> productDetailsSerde;
	private final KafkaProtobufSerde<SkuDetails> skuDetailsSerde;
	private final KafkaProtobufSerde<OfferDetails> offerDetailsSerde;
	private final KafkaProtobufSerde<Price> priceSerde;
	private final KafkaProtobufSerde<Product> productSerde;

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
		var price = Price.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setAmount(19_999.99f)
				.build();
		priceInputTopic.pipeInput(price);
		assertIncompleteProductNotInKafka();

		// Send offer details
		var offerDetails = OfferDetails.newBuilder()
				.setOfferId("O1S1P1")
				.setProductId("P1")
				.setSkuId("S1P1")
				.setName("Refurbished blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue ! It should work too.")
				.build();
		offerDetailsInputTopic.pipeInput(offerDetails);
		assertIncompleteProductNotInKafka();

		// Send sku details
		var skuDetails = SkuDetails.newBuilder()
				.setSkuId("S1P1")
				.setProductId("P1")
				.setName("Blue wonderful thing")
				.setDescription("That's a wonderful thing, trust me..., and this one is blue !")
				.build();
		skuDetailsInputTopic.pipeInput(skuDetails);
		assertIncompleteProductNotInKafka();

		// Send product details
		var productDetails = ProductDetails.newBuilder()
				.setName("Wonderful thing")
				.setDescription("That's a wonderful thing, trust me...")
				.setBrand("ShadyGuys")
				.build();
		productDetailsInputTopic.pipeInput("P1", productDetails);

		// Read resulting product
		var productKeyValue = productOutputTopic.readKeyValue();

		var expectedProduct = Product.newBuilder()
				.setId("P1")
				.setName("Wonderful thing")
				.setDescription("That's a wonderful thing, trust me...")
				.setBrand("ShadyGuys")
				.addSkus(Sku.newBuilder()
						.setId("S1P1")
						.setName("Blue wonderful thing")
						.setDescription("That's a wonderful thing, trust me..., and this one is blue !")
						.addOffers(Offer.newBuilder()
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
